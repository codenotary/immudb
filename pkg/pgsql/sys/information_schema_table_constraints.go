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
	"context"

	"github.com/codenotary/immudb/embedded/sql"
)

// information_schema.table_constraints — one row per PK or UNIQUE
// constraint per table. Mirrors the pg_constraint A3 system table
// but with the info_schema column shape clients expect. constraint_type
// values are the SQL-standard strings ('PRIMARY KEY', 'UNIQUE',
// 'FOREIGN KEY', 'CHECK'); we only emit the first two because immudb
// doesn't materialise the others in its catalog yet.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "information_schema_table_constraints",
		Columns: []sql.SystemTableColumn{
			{Name: "constraint_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "constraint_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "constraint_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "table_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "constraint_type", Type: sql.VarcharType, MaxLen: 16},
			{Name: "is_deferrable", Type: sql.VarcharType, MaxLen: 3},
			{Name: "initially_deferred", Type: sql.VarcharType, MaxLen: 3},
			{Name: "enforced", Type: sql.VarcharType, MaxLen: 3},
			{Name: "constraint_oid", Type: sql.IntegerType},
		},
		PKColumn: "constraint_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0)
			for _, t := range tables {
				for _, idx := range t.GetIndexes() {
					var conname, ctype string
					switch {
					case idx.IsPrimary():
						conname = t.Name() + "_pkey"
						ctype = "PRIMARY KEY"
					case idx.IsUnique():
						conname = idx.Name()
						ctype = "UNIQUE"
					default:
						continue
					}
					rows = append(rows, &sql.Row{ValuesByPosition: []sql.TypedValue{
						sql.NewVarchar("immudb"),
						sql.NewVarchar("public"),
						sql.NewVarchar(conname),
						sql.NewVarchar("immudb"),
						sql.NewVarchar("public"),
						sql.NewVarchar(t.Name()),
						sql.NewVarchar(ctype),
						sql.NewVarchar("NO"),
						sql.NewVarchar("NO"),
						sql.NewVarchar("YES"),
						sql.NewInteger(relOID("information_schema_constraint", conname)),
					}})
				}
			}
			return rows, nil
		},
	})
}
