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

// information_schema.tables — one row per user table.
//
// The table lives under the name `information_schema_tables` because
// the wire layer's removePGCatalogReferences rewrite converts the
// dotted form `information_schema.tables` to the underscore form at
// rewrite time (see query_machine.go:1015). Matching PG's table_type
// values ('BASE TABLE' / 'VIEW') matters for Alembic / Flyway / JDBC
// schema browsers, which filter on them.
//
// Column shape mirrors pgschema.infoSchemaTablesResolver so queries
// that relied on the legacy resolver still work byte-for-byte.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "information_schema_tables",
		Columns: []sql.SystemTableColumn{
			{Name: "table_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "table_type", Type: sql.VarcharType, MaxLen: 16},
			// Synthesised PK: registry requires a single-column PK and
			// the natural key (catalog, schema, name) isn't expressible.
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
				rows = append(rows, &sql.Row{ValuesByPosition: []sql.TypedValue{
					sql.NewVarchar("immudb"),
					sql.NewVarchar("public"),
					sql.NewVarchar(t.Name()),
					sql.NewVarchar("BASE TABLE"),
					sql.NewInteger(relOID("public", t.Name())),
				}})
			}
			return rows, nil
		},
	})
}
