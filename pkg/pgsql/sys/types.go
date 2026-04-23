/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package sys

import (
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
)

// pgTypeOIDForSQLType maps immudb's internal SQLValueType to the
// canonical PostgreSQL type OID the wire protocol returns for that
// column. Matches the map in pkg/pgsql/server/immudb_functions.go
// (pgTypeOIDByName) so the two layers stay consistent.
func pgTypeOIDForSQLType(t sql.SQLValueType) int64 {
	switch t {
	case sql.IntegerType:
		return 20 // int8 — safe for all immudb integers, which are int64
	case sql.BooleanType:
		return 16 // bool
	case sql.VarcharType:
		return 1043 // varchar
	case sql.UUIDType:
		return 2950 // uuid
	case sql.BLOBType:
		return 17 // bytea
	case sql.Float64Type:
		return 701 // float8
	case sql.TimestampType:
		return 1114 // timestamp (no tz)
	case sql.JSONType:
		return 3802 // jsonb
	case sql.AnyType:
		return 2276 // "any" — matches PG's polymorphic pseudo-type
	default:
		return 0 // unknown — clients will use text fallback
	}
}

// pgTypeNameForSQLType returns the PostgreSQL type name for a given
// immudb SQLValueType, as emitted by format_type(oid, typmod). Used
// both by the pg_attribute scan (for PG_catalog niceties) and by the
// format_type built-in function once it's wired up.
//
// maxLen is respected only for VARCHAR: format_type emits
// `character varying(N)` when a typmod is supplied, matching PG's
// conventions for what \d shows.
func pgTypeNameForSQLType(t sql.SQLValueType, maxLen int) string {
	switch t {
	case sql.IntegerType:
		return "bigint"
	case sql.BooleanType:
		return "boolean"
	case sql.VarcharType:
		if maxLen > 0 {
			return fmt.Sprintf("character varying(%d)", maxLen)
		}
		return "character varying"
	case sql.UUIDType:
		return "uuid"
	case sql.BLOBType:
		return "bytea"
	case sql.Float64Type:
		return "double precision"
	case sql.TimestampType:
		return "timestamp without time zone"
	case sql.JSONType:
		return "jsonb"
	case sql.AnyType:
		return "any"
	default:
		return "text" // conservative fallback — psql still renders something
	}
}

// pgTypeLenForSQLType returns the PostgreSQL attlen value for
// pg_attribute. Fixed-width types report their byte size; variable-
// width types report -1 (PG's convention).
func pgTypeLenForSQLType(t sql.SQLValueType) int64 {
	switch t {
	case sql.BooleanType:
		return 1
	case sql.IntegerType:
		return 8
	case sql.Float64Type:
		return 8
	case sql.UUIDType:
		return 16
	case sql.TimestampType:
		return 8
	// VARCHAR, BLOB, JSON, ANY — variable-width
	default:
		return -1
	}
}
