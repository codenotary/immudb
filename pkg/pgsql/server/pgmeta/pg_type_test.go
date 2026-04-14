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

package pgmeta

import (
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
)

// TestPgTypeMapOIDsMatchPostgres pins the wire OID for each immudb
// SQLValueType to the value that real Postgres returns for the
// corresponding pg_type row. ORM type registries (Rails, SQLAlchemy,
// pgx) key on these OIDs to choose a deserialiser; getting one wrong
// silently corrupts data on the read path (e.g. TimestampType used to
// be OID 20 / int8, so the pg gem decoded the text timestamp via to_i
// and kept only the leading year).
func TestPgTypeMapOIDsMatchPostgres(t *testing.T) {
	want := map[string]int{
		sql.BooleanType:   16,   // bool
		sql.BLOBType:      17,   // bytea
		sql.TimestampType: 1114, // timestamp without time zone
		sql.IntegerType:   20,   // int8
		sql.VarcharType:   25,   // text
		sql.UUIDType:      2950, // uuid
		sql.Float64Type:   701,  // float8
		sql.JSONType:      3802, // jsonb
	}
	for typeName, oid := range want {
		got := PgTypeMap[typeName][PgTypeMapOid]
		if got != oid {
			t.Errorf("OID for %s = %d, want %d", typeName, got, oid)
		}
	}
}
