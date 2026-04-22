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
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
)

// pg_constraint: one row per primary-key or unique-index constraint on
// each user table. immudb doesn't yet materialise CHECK or FOREIGN KEY
// constraints in the catalog layer, so those contypes are omitted.
//
// contype ('p'=primary, 'u'=unique) drives psql's constraint-display
// block in `\d`. conkey is a PG int2[] of attnums; we emit it as a
// braces-wrapped comma-separated list (`{1,2}`) which is the textual
// form PG itself uses when array_to_string isn't involved.
//
// Constraint names: primary-key constraints are named `<table>_pkey`
// (psql convention), unique indexes reuse their index name. Matches
// what `\d` prints for real PostgreSQL and makes the constraint oid
// FNV-stable across restarts.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_constraint",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "conname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "connamespace", Type: sql.IntegerType},
			{Name: "contype", Type: sql.VarcharType, MaxLen: 1},
			{Name: "condeferrable", Type: sql.BooleanType},
			{Name: "condeferred", Type: sql.BooleanType},
			{Name: "convalidated", Type: sql.BooleanType},
			{Name: "conrelid", Type: sql.IntegerType},
			{Name: "contypid", Type: sql.IntegerType},
			{Name: "conindid", Type: sql.IntegerType},
			{Name: "conparentid", Type: sql.IntegerType},
			{Name: "confrelid", Type: sql.IntegerType},
			{Name: "confupdtype", Type: sql.VarcharType, MaxLen: 1},
			{Name: "confdeltype", Type: sql.VarcharType, MaxLen: 1},
			{Name: "confmatchtype", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conislocal", Type: sql.BooleanType},
			{Name: "coninhcount", Type: sql.IntegerType},
			{Name: "connoinherit", Type: sql.BooleanType},
			{Name: "conkey", Type: sql.VarcharType, MaxLen: 256},
			{Name: "confkey", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conpfeqop", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conppeqop", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conffeqop", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conexclop", Type: sql.VarcharType, MaxLen: 1},
			{Name: "conbin", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0, len(tables)*2)
			for _, t := range tables {
				tableOID := relOID("public", t.Name())
				for _, idx := range t.GetIndexes() {
					switch {
					case idx.IsPrimary():
						rows = append(rows, rowConstraint(
							t.Name()+"_pkey", "p", tableOID, idx, relOID("public", idx.Name())))
					case idx.IsUnique():
						rows = append(rows, rowConstraint(
							idx.Name(), "u", tableOID, idx, relOID("public", idx.Name())))
					}
				}
			}
			return rows, nil
		},
	})
}

func rowConstraint(conname, contype string, tableOID int64, idx *sql.Index, conindid int64) *sql.Row {
	cols := idx.Cols()
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = strconv.Itoa(int(c.ID()))
	}
	conkey := "{" + strings.Join(parts, ",") + "}"

	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("pg_constraint", conname)),
		sql.NewVarchar(conname),
		sql.NewInteger(OIDNamespacePublic),
		sql.NewVarchar(contype),
		sql.NewBool(false), // condeferrable
		sql.NewBool(false), // condeferred
		sql.NewBool(true),  // convalidated
		sql.NewInteger(tableOID),
		sql.NewInteger(0), // contypid
		sql.NewInteger(conindid),
		sql.NewInteger(0), // conparentid
		sql.NewInteger(0), // confrelid
		sql.NewVarchar(" "),
		sql.NewVarchar(" "),
		sql.NewVarchar(" "),
		sql.NewBool(true), // conislocal
		sql.NewInteger(0),
		sql.NewBool(true), // connoinherit
		sql.NewVarchar(conkey),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
	}}
}
