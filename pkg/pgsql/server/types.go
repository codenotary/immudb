/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func buildNamedParams(paramsType []sql.ColDescriptor, paramsVal []interface{}) ([]*schema.NamedParam, error) {
	pMap := make(map[string]interface{})
	for index, param := range paramsType {
		name := param.Column

		val := paramsVal[index]
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
				pMap[name] = p == "true"
			case sql.BLOBType:
				d, err := hex.DecodeString(p)
				if err != nil {
					return nil, err
				}
				pMap[name] = d
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
