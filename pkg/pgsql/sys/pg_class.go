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

// Column names in pg_class. Kept in one place so pgClassColumns and
// the row builder can't drift.
const (
	pgClassOid                 = "oid"
	pgClassRelname             = "relname"
	pgClassRelnamespace        = "relnamespace"
	pgClassReltype             = "reltype"
	pgClassReloftype           = "reloftype"
	pgClassRelowner            = "relowner"
	pgClassRelam               = "relam"
	pgClassRelfilenode         = "relfilenode"
	pgClassReltablespace       = "reltablespace"
	pgClassRelpages            = "relpages"
	pgClassReltuples           = "reltuples"
	pgClassRelallvisible       = "relallvisible"
	pgClassReltoastrelid       = "reltoastrelid"
	pgClassRelhasindex         = "relhasindex"
	pgClassRelisshared         = "relisshared"
	pgClassRelpersistence      = "relpersistence"
	pgClassRelkind             = "relkind"
	pgClassRelnatts            = "relnatts"
	pgClassRelchecks           = "relchecks"
	pgClassRelhasrules         = "relhasrules"
	pgClassRelhastriggers      = "relhastriggers"
	pgClassRelhassubclass      = "relhassubclass"
	pgClassRelrowsecurity      = "relrowsecurity"
	pgClassRelforcerowsecurity = "relforcerowsecurity"
	pgClassRelispopulated      = "relispopulated"
	pgClassRelreplident        = "relreplident"
	pgClassRelispartition      = "relispartition"
	pgClassRelhasoids          = "relhasoids"
)

var pgClassColumns = []sql.SystemTableColumn{
	{Name: pgClassOid, Type: sql.IntegerType},
	{Name: pgClassRelname, Type: sql.VarcharType, MaxLen: 128},
	{Name: pgClassRelnamespace, Type: sql.IntegerType},
	{Name: pgClassReltype, Type: sql.IntegerType},
	{Name: pgClassReloftype, Type: sql.IntegerType},
	{Name: pgClassRelowner, Type: sql.IntegerType},
	{Name: pgClassRelam, Type: sql.IntegerType},
	{Name: pgClassRelfilenode, Type: sql.IntegerType},
	{Name: pgClassReltablespace, Type: sql.IntegerType},
	{Name: pgClassRelpages, Type: sql.IntegerType},
	{Name: pgClassReltuples, Type: sql.Float64Type},
	{Name: pgClassRelallvisible, Type: sql.IntegerType},
	{Name: pgClassReltoastrelid, Type: sql.IntegerType},
	{Name: pgClassRelhasindex, Type: sql.BooleanType},
	{Name: pgClassRelisshared, Type: sql.BooleanType},
	{Name: pgClassRelpersistence, Type: sql.VarcharType, MaxLen: 1},
	{Name: pgClassRelkind, Type: sql.VarcharType, MaxLen: 1},
	{Name: pgClassRelnatts, Type: sql.IntegerType},
	{Name: pgClassRelchecks, Type: sql.IntegerType},
	{Name: pgClassRelhasrules, Type: sql.BooleanType},
	{Name: pgClassRelhastriggers, Type: sql.BooleanType},
	{Name: pgClassRelhassubclass, Type: sql.BooleanType},
	{Name: pgClassRelrowsecurity, Type: sql.BooleanType},
	{Name: pgClassRelforcerowsecurity, Type: sql.BooleanType},
	{Name: pgClassRelispopulated, Type: sql.BooleanType},
	{Name: pgClassRelreplident, Type: sql.VarcharType, MaxLen: 1},
	{Name: pgClassRelispartition, Type: sql.BooleanType},
	{Name: pgClassRelhasoids, Type: sql.BooleanType},
}

// pg_class: one row per user table and one per user index.
// Views live in the engine's tableResolvers rather than the catalog;
// we don't expose them in pg_class yet because the engine doesn't let
// us enumerate them from inside a Scan without a more invasive change
// (deferred to a later phase).
//
// Every user object is reported under the 'public' namespace because
// immudb has no schema concept; that's what psql's \d expects as a
// default and it matches the handlePgTablesQuery canned handler.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name:     "pg_class",
		Columns:  pgClassColumns,
		PKColumn: pgClassOid,
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}

			tables := cat.GetTables()
			// Each table contributes one row, plus one row per index on
			// that table. Pre-size for the common case.
			rows := make([]*sql.Row, 0, len(tables)*2)

			for _, t := range tables {
				rows = append(rows, rowPgClassForTable(t))
				for _, idx := range t.GetIndexes() {
					rows = append(rows, rowPgClassForIndex(t, idx))
				}
			}
			return rows, nil
		},
	})
}

func rowPgClassForTable(t *sql.Table) *sql.Row {
	cols := t.Cols()
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("public", t.Name())),
		sql.NewVarchar(t.Name()),
		sql.NewInteger(OIDNamespacePublic),
		sql.NewInteger(0), // reltype — pg_type composite row; we don't synthesise these
		sql.NewInteger(0), // reloftype
		sql.NewInteger(10),
		sql.NewInteger(0), // relam — tables don't have an access method in PG
		sql.NewInteger(0), // relfilenode
		sql.NewInteger(0), // reltablespace (pg_default)
		sql.NewInteger(0), // relpages
		sql.NewFloat64(0), // reltuples — could be approximate; 0 is safe
		sql.NewInteger(0), // relallvisible
		sql.NewInteger(0), // reltoastrelid
		sql.NewBool(len(t.GetIndexes()) > 0),
		sql.NewBool(false), // relisshared
		sql.NewVarchar("p"),
		sql.NewVarchar("r"),
		sql.NewInteger(int64(len(cols))),
		sql.NewInteger(0), // relchecks — we have the data but enumerating needs a Table accessor we don't yet expose
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(true),  // relispopulated — user tables always are
		sql.NewVarchar("d"),
		sql.NewBool(false),
		sql.NewBool(false),
	}}
}

func rowPgClassForIndex(t *sql.Table, idx *sql.Index) *sql.Row {
	// Index OID hashes on (table.name, index.name) so renaming either
	// side changes it — acceptable since clients don't cache index oids
	// across migrations.
	indexRelName := idx.Name()
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("public", indexRelName)),
		sql.NewVarchar(indexRelName),
		sql.NewInteger(OIDNamespacePublic),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewInteger(10),
		sql.NewInteger(OIDAccessMethodBtree),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewFloat64(0),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewBool(false), // relhasindex — indexes don't have sub-indexes
		sql.NewBool(false),
		sql.NewVarchar("p"),
		sql.NewVarchar("i"), // relkind 'i' = index
		sql.NewInteger(int64(len(idx.Cols()))),
		sql.NewInteger(0),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(true),
		sql.NewVarchar("n"),
		sql.NewBool(false),
		sql.NewBool(false),
	}}
}
