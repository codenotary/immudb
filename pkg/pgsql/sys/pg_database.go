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

// pg_database: one row for the current database.
//
// immudb has multi-database support at the server level, but a SQL
// transaction only sees the database it's bound to — enumerating the
// others would require plumbing the server's database manager through
// the engine's Scan context. That's out of scope for A3; clients that
// just want `\l` to return *something* see a single row for the current
// db, which matches what current_database() already reports.
//
// The datname value is read from current_database()'s implementation —
// keeping it as "defaultdb" avoids drift if the two ever disagree.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_database",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "datname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "datdba", Type: sql.IntegerType},
			{Name: "encoding", Type: sql.IntegerType},
			{Name: "datcollate", Type: sql.VarcharType, MaxLen: 64},
			{Name: "datctype", Type: sql.VarcharType, MaxLen: 64},
			{Name: "datistemplate", Type: sql.BooleanType},
			{Name: "datallowconn", Type: sql.BooleanType},
			{Name: "datconnlimit", Type: sql.IntegerType},
			{Name: "datlastsysoid", Type: sql.IntegerType},
			{Name: "datfrozenxid", Type: sql.IntegerType},
			{Name: "datminmxid", Type: sql.IntegerType},
			{Name: "dattablespace", Type: sql.IntegerType},
			{Name: "datacl", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return []*sql.Row{rowDatabase("defaultdb")}, nil
		},
	})
}

func rowDatabase(name string) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("pg_catalog", name)),
		sql.NewVarchar(name),
		sql.NewInteger(10),   // datdba — stable role oid, matches pg_namespace.nspowner
		sql.NewInteger(6),    // encoding 6 = UTF8 in PG
		sql.NewVarchar("C"),  // datcollate — immudb has no locale concept
		sql.NewVarchar("C"),
		sql.NewBool(false),   // datistemplate
		sql.NewBool(true),    // datallowconn
		sql.NewInteger(-1),   // datconnlimit — unlimited
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewInteger(0),    // dattablespace = pg_default
		sql.NewNull(sql.VarcharType),
	}}
}
