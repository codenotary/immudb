/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// pgTextBool mirrors Postgres' text-format boolean accepted values:
// t/true/y/yes/on/1 for true; f/false/n/no/off/0 for false.
// Rails' pg gem sends "t"/"f" by default, not "true"/"false".
func pgTextBool(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true", "y", "yes", "on", "1":
		return true, true
	case "f", "false", "n", "no", "off", "0":
		return false, true
	}
	return false, false
}

// pgTextTimestamp parses the common text formats Postgres/Rails send
// for TIMESTAMP binds: "YYYY-MM-DD HH:MM:SS[.fff][Z|±HH:MM]" or RFC3339.
var pgTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05.999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05-07",
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02",
}

func pgTextTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range pgTimestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func buildNamedParams(paramsType []sql.ColDescriptor, paramsVal []interface{}) ([]*schema.NamedParam, error) {
	pMap := make(map[string]interface{})
	for index, param := range paramsType {
		name := param.Column

		val := paramsVal[index]
		// NULL parameter (Bind protocol pLen == -1).
		if val == nil {
			pMap[name] = nil
			continue
		}
		// text param
		if p, ok := val.(string); ok {
			switch param.Type {

			case sql.IntegerType:
				int, err := strconv.Atoi(p)
				if err != nil {
					return nil, err
				}
				pMap[name] = int64(int)
			case sql.VarcharType:
				pMap[name] = p
			case sql.BooleanType:
				// Postgres text-format booleans: "t"/"f" (Rails),
				// plus the long forms. Anything else is an error,
				// not a silent false, so we don't round-trip a
				// corrupted bool (the old `p == "true"` check stored
				// Rails' "t" as false).
				if b, ok := pgTextBool(p); ok {
					pMap[name] = b
				} else {
					return nil, fmt.Errorf("invalid boolean text bind value %q for parameter %s", p, name)
				}
			case sql.Float64Type:
				f, err := strconv.ParseFloat(p, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float bind value %q for parameter %s: %w", p, name, err)
				}
				pMap[name] = f
			case sql.TimestampType:
				// Rails sends timestamps as "YYYY-MM-DD HH:MM:SS.ffffff".
				// Must become time.Time so EncodeParams emits a Ts
				// SQLValue — otherwise the engine coerces the leading
				// digits as an integer and stores the year only.
				if t, ok := pgTextTimestamp(p); ok {
					pMap[name] = t
				} else {
					return nil, fmt.Errorf("invalid timestamp bind value %q for parameter %s", p, name)
				}
			case sql.BLOBType:
				d, err := hex.DecodeString(p)
				if err != nil {
					return nil, err
				}
				pMap[name] = d
			default:
				// AnyType / unrecognised: keep as string. ORMs that
				// don't introspect parameter types ahead of binding
				// (Rails sends text-format params for everything
				// when the inferred type is unknown) rely on the
				// engine's downstream coercion, not on us forcing
				// a specific type here.
				pMap[name] = p
			}
		}
		// binary param
		if p, ok := val.([]byte); ok {
			switch param.Type {
			case sql.IntegerType:
				i, err := getInt64(p)
				if err != nil {
					return nil, err
				}
				pMap[name] = i
			case sql.VarcharType:
				pMap[name] = string(p)
			case sql.BooleanType:
				v := false
				if p[0] == byte(1) {
					v = true
				}
				pMap[name] = v
			case sql.BLOBType:
				pMap[name] = p
			default:
				// AnyType: pass raw bytes through; downstream
				// coercion picks the right value.
				pMap[name] = p
			}
		}
	}
	return schema.EncodeParams(pMap)
}

func getInt64(p []byte) (int64, error) {
	switch len(p) {
	case 8:
		return int64(binary.BigEndian.Uint64(p)), nil
	case 4:
		return int64(binary.BigEndian.Uint32(p)), nil
	case 2:
		return int64(binary.BigEndian.Uint16(p)), nil
	default:
		return 0, fmt.Errorf("cannot convert a slice of %d byte in an INTEGER parameter", len(p))
	}
}
