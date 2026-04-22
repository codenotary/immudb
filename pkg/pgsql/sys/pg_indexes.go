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
	"fmt"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
)

// pg_indexes: one row per index on a user table. ORMs (XORM, GORM,
// Hibernate) read this to enumerate indexes; pgAdmin's object browser
// renders from it. The indexdef column holds a synthesised
// CREATE INDEX statement good enough for "does this index cover X"
// checks — real PG returns the original CREATE source, but immudb
// doesn't persist DDL text.
//
// Column shape mirrors the legacy handlePgIndexesQuery canned handler
// (pgadmin_compat.go:857). The naming convention <table>_idx_<id>
// matches the old handler so tests pinning that name keep working.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_indexes",
		Columns: []sql.SystemTableColumn{
			{Name: "schemaname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "tablename", Type: sql.VarcharType, MaxLen: 128},
			{Name: "indexname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "tablespace", Type: sql.VarcharType, MaxLen: 64},
			{Name: "indexdef", Type: sql.VarcharType, MaxLen: 1024},
			// Synthesised PK: indexname is per-schema but registry
			// wants a single column, so use a synthetic oid.
			{Name: "index_oid", Type: sql.IntegerType},
		},
		PKColumn: "index_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0, len(tables))
			for _, t := range tables {
				for _, idx := range t.GetIndexes() {
					indexName := fmt.Sprintf("%s_idx_%d", t.Name(), idx.ID())
					rows = append(rows, &sql.Row{ValuesByPosition: []sql.TypedValue{
						sql.NewVarchar("public"),
						sql.NewVarchar(t.Name()),
						sql.NewVarchar(indexName),
						sql.NewVarchar("pg_default"),
						sql.NewVarchar(pgIndexesIndexDef(t.Name(), idx)),
						sql.NewInteger(relOID("pg_indexes", indexName)),
					}})
				}
			}
			return rows, nil
		},
	})
}

// pgIndexesIndexDef synthesises a CREATE [UNIQUE] INDEX statement for
// an index. Mirrors buildIndexDef in pgadmin_compat.go so clients that
// compared the indexdef string against a known form don't regress.
func pgIndexesIndexDef(tableName string, idx *sql.Index) string {
	colNames := make([]string, 0, len(idx.Cols()))
	for _, c := range idx.Cols() {
		colNames = append(colNames, c.Name())
	}
	verb := "CREATE INDEX"
	if idx.IsUnique() {
		verb = "CREATE UNIQUE INDEX"
	}
	return fmt.Sprintf("%s ON %s (%s)", verb, tableName, strings.Join(colNames, ", "))
}
