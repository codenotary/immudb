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

// pg_roles: one row per immudb user that's visible to the current tx.
// immudb's tx exposes ListUsers which returns every user in the
// database; we mark admins as rolsuper=true. Rows emit stable oids
// hashed on the username so joins against pg_class.relowner etc. work.
//
// If ListUsers errors (e.g. the tx handle doesn't have permission) we
// fall back to a synthetic `immudb` superuser row so psql's `\du`
// doesn't fail outright.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_roles",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "rolname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "rolsuper", Type: sql.BooleanType},
			{Name: "rolinherit", Type: sql.BooleanType},
			{Name: "rolcreaterole", Type: sql.BooleanType},
			{Name: "rolcreatedb", Type: sql.BooleanType},
			{Name: "rolcanlogin", Type: sql.BooleanType},
			{Name: "rolreplication", Type: sql.BooleanType},
			{Name: "rolconnlimit", Type: sql.IntegerType},
			{Name: "rolpassword", Type: sql.VarcharType, MaxLen: 16},
			{Name: "rolvaliduntil", Type: sql.VarcharType, MaxLen: 1},
			{Name: "rolbypassrls", Type: sql.BooleanType},
			{Name: "rolconfig", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			users, err := tx.ListUsers(ctx)
			if err != nil || len(users) == 0 {
				return []*sql.Row{rowRole("immudb", true)}, nil
			}
			rows := make([]*sql.Row, 0, len(users))
			for _, u := range users {
				rows = append(rows, rowRole(u.Username(), u.Permission() == sql.PermissionSysAdmin))
			}
			return rows, nil
		},
	})
}

func rowRole(name string, isSuper bool) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("pg_catalog_role", name)),
		sql.NewVarchar(name),
		sql.NewBool(isSuper),
		sql.NewBool(true),      // rolinherit — default PG behaviour
		sql.NewBool(isSuper),   // rolcreaterole
		sql.NewBool(isSuper),   // rolcreatedb
		sql.NewBool(true),      // rolcanlogin — every immudb user can log in
		sql.NewBool(false),
		sql.NewInteger(-1),     // rolconnlimit
		sql.NewVarchar("********"),
		sql.NewNull(sql.VarcharType),
		sql.NewBool(isSuper),   // rolbypassrls
		sql.NewNull(sql.VarcharType),
	}}
}
