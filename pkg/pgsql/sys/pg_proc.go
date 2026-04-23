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
	"sort"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
)

// pg_proc: one row per registered built-in function.
//
// The Scan walks sql.RegisteredFunctions() (exported in A3 alongside
// this table) and emits a row per name with a stable FNV-hashed oid.
// We don't try to introspect parameter types — registered functions
// sometimes accept variadic args, sometimes overload by type; PG's
// pg_proc schema doesn't cleanly represent either. Clients that care
// about signatures read pg_type via the `proargtypes` vector and
// mostly just want the row to *exist* so `\df` can list the name.
//
// Row order is sorted by name for test stability.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_proc",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "proname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "pronamespace", Type: sql.IntegerType},
			{Name: "proowner", Type: sql.IntegerType},
			{Name: "prolang", Type: sql.IntegerType},
			{Name: "procost", Type: sql.Float64Type},
			{Name: "prorows", Type: sql.Float64Type},
			{Name: "provariadic", Type: sql.IntegerType},
			{Name: "prosupport", Type: sql.IntegerType},
			{Name: "prokind", Type: sql.VarcharType, MaxLen: 1},
			{Name: "prosecdef", Type: sql.BooleanType},
			{Name: "proleakproof", Type: sql.BooleanType},
			{Name: "proisstrict", Type: sql.BooleanType},
			{Name: "proretset", Type: sql.BooleanType},
			{Name: "provolatile", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proparallel", Type: sql.VarcharType, MaxLen: 1},
			{Name: "pronargs", Type: sql.IntegerType},
			{Name: "pronargdefaults", Type: sql.IntegerType},
			{Name: "prorettype", Type: sql.IntegerType},
			{Name: "proargtypes", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proallargtypes", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proargmodes", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proargnames", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proargdefaults", Type: sql.VarcharType, MaxLen: 1},
			{Name: "protrftypes", Type: sql.VarcharType, MaxLen: 1},
			{Name: "prosrc", Type: sql.VarcharType, MaxLen: 128},
			{Name: "probin", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proconfig", Type: sql.VarcharType, MaxLen: 1},
			{Name: "proacl", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			fns := sql.RegisteredFunctions()
			names := make([]string, 0, len(fns))
			for n := range fns {
				names = append(names, n)
			}
			sort.Strings(names)

			rows := make([]*sql.Row, 0, len(names))
			for _, n := range names {
				rows = append(rows, rowProc(n))
			}
			return rows, nil
		},
	})
}

func rowProc(name string) *sql.Row {
	lower := strings.ToLower(name)
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("pg_catalog_proc", lower)),
		sql.NewVarchar(lower),
		sql.NewInteger(OIDNamespacePgCatalog),
		sql.NewInteger(10),
		sql.NewInteger(12), // prolang 12 = internal in real PG
		sql.NewFloat64(1),
		sql.NewFloat64(0),
		sql.NewInteger(0),
		sql.NewInteger(0),
		sql.NewVarchar("f"), // prokind 'f' = normal function
		sql.NewBool(false),
		sql.NewBool(false),
		sql.NewBool(true),
		sql.NewBool(false),
		sql.NewVarchar("i"), // provolatile 'i' = immutable (safe default; clients re-call anyway)
		sql.NewVarchar("s"), // proparallel 's' = safe
		sql.NewInteger(0),   // pronargs — we can't introspect params; 0 means "variadic in practice"
		sql.NewInteger(0),
		sql.NewInteger(0),   // prorettype — 0 = unknown; clients fall back to text-decode
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewVarchar(lower),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
	}}
}
