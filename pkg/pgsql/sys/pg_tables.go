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

// pg_tables: a PG compatibility view over pg_class that XORM / GORM /
// SQLAlchemy / Hibernate / psql `\dt` read to enumerate base tables.
//
// Column shape and values match the legacy handlePgTablesQuery canned
// handler in pgadmin_compat.go so no ORM that used to work regresses.
// The key columns clients filter on are schemaname (always 'public'
// in immudb's single-schema model) and tablename (for existence
// probes).
//
// hasindexes reflects whether the table has any non-PK index —
// matching what PG reports. Other booleans are always false: immudb
// has no rules / triggers / row-level security.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_tables",
		Columns: []sql.SystemTableColumn{
			{Name: "schemaname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "tablename", Type: sql.VarcharType, MaxLen: 128},
			{Name: "tableowner", Type: sql.VarcharType, MaxLen: 64},
			{Name: "tablespace", Type: sql.VarcharType, MaxLen: 64},
			{Name: "hasindexes", Type: sql.BooleanType},
			{Name: "hasrules", Type: sql.BooleanType},
			{Name: "hastriggers", Type: sql.BooleanType},
			{Name: "rowsecurity", Type: sql.BooleanType},
			// Synthesised PK — natural key is (schemaname, tablename)
			// but the registry wants a single-column PK.
			{Name: "table_oid", Type: sql.IntegerType},
		},
		PKColumn: "table_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0, len(tables))
			for _, t := range tables {
				// Real PG pg_tables.hasindexes counts any index; immudb
				// creates the PK index implicitly on every table so
				// len(idxs) > 0 is always true for base tables. Leave
				// the literal check in place in case some future table
				// shape skips the implicit PK.
				rows = append(rows, &sql.Row{ValuesByPosition: []sql.TypedValue{
					sql.NewVarchar("public"),
					sql.NewVarchar(t.Name()),
					sql.NewVarchar("immudb"),
					sql.NewNull(sql.VarcharType),
					sql.NewBool(len(t.GetIndexes()) > 0),
					sql.NewBool(false),
					sql.NewBool(false),
					sql.NewBool(false),
					sql.NewInteger(relOID("public", t.Name())),
				}})
			}
			return rows, nil
		},
	})
}
