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

// information_schema.key_column_usage — one row per column that
// participates in a PK or UNIQUE constraint. The canonical query
// JOINs this with information_schema.table_constraints to discover
// the set of indexed columns; Alembic / Flyway / Hibernate all emit
// this shape when probing constraints.
//
// Matches pgschema.infoSchemaKeyColumnUsageResolver's column shape so
// callers that relied on the legacy resolver see identical output.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "information_schema_key_column_usage",
		Columns: []sql.SystemTableColumn{
			{Name: "constraint_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "constraint_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "constraint_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "table_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "column_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "ordinal_position", Type: sql.IntegerType},
			{Name: "position_in_unique_constraint", Type: sql.IntegerType},
			// Synthesised PK — (constraint_name, column_name) would be
			// unique but the registry only accepts single-column keys.
			{Name: "usage_oid", Type: sql.IntegerType},
		},
		PKColumn: "usage_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0)
			for _, t := range tables {
				for _, idx := range t.GetIndexes() {
					if !idx.IsPrimary() && !idx.IsUnique() {
						continue
					}
					conname := idx.Name()
					if idx.IsPrimary() {
						conname = t.Name() + "_pkey"
					}
					for i, c := range idx.Cols() {
						rows = append(rows, &sql.Row{ValuesByPosition: []sql.TypedValue{
							sql.NewVarchar("immudb"),
							sql.NewVarchar("public"),
							sql.NewVarchar(conname),
							sql.NewVarchar("immudb"),
							sql.NewVarchar("public"),
							sql.NewVarchar(t.Name()),
							sql.NewVarchar(c.Name()),
							sql.NewInteger(int64(i + 1)),
							sql.NewNull(sql.IntegerType),
							sql.NewInteger(colOID("public", t.Name(), conname+":"+c.Name())),
						}})
					}
				}
			}
			return rows, nil
		},
	})
}
