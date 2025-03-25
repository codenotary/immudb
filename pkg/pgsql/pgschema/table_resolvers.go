/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package pgschema

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
)

var pgClassCols = []sql.ColDescriptor{
	{
		Column: "oid",
		Type:   sql.IntegerType,
	},
	{
		Column: "relname",
		Type:   sql.VarcharType,
	},
	{
		Column: "relnamespace",
		Type:   sql.IntegerType,
	},
	{

		Column: "reltype",
		Type:   sql.VarcharType,
	},
	{

		Column: "reloftype",
		Type:   sql.IntegerType,
	},
	{

		Column: "relowner",
		Type:   sql.IntegerType,
	},
	{

		Column: "relam",
		Type:   sql.IntegerType,
	},
	{

		Column: "relfilenode",
		Type:   sql.IntegerType,
	},
	{

		Column: "reltablespace",
		Type:   sql.IntegerType,
	},
	{

		Column: "relpages",
		Type:   sql.IntegerType,
	},
	{

		Column: "reltuples",
		Type:   sql.Float64Type,
	},
	{

		Column: "relallvisible",
		Type:   sql.IntegerType,
	},
	{

		Column: "reltoastrelid",
		Type:   sql.IntegerType,
	},
	{

		Column: "relhasindex",
		Type:   sql.BooleanType,
	},
	{

		Column: "relisshared",
		Type:   sql.BooleanType,
	},
	{

		Column: "relpersistence",
		Type:   sql.VarcharType,
	},
	{

		Column: "relkind",
		Type:   sql.VarcharType,
	},
	{

		Column: "relnats",
		Type:   sql.IntegerType,
	},
	{

		Column: "relchecks",
		Type:   sql.IntegerType,
	},
	{

		Column: "relhasrules",
		Type:   sql.BooleanType,
	},
	{

		Column: "relhastriggers",
		Type:   sql.BooleanType,
	},
	{

		Column: "relhassubclass",
		Type:   sql.BooleanType,
	},
	{

		Column: "relrowsecurity",
		Type:   sql.BooleanType,
	},
	{

		Column: "relforcerowsecurity",
		Type:   sql.BooleanType,
	},
	{
		Column: "relispopulated",
		Type:   sql.BooleanType,
	},
	{
		Column: "relreplident",
		Type:   sql.VarcharType,
	},
	{
		Column: "relispartition",
		Type:   sql.BooleanType,
	},
	{
		Column: "relrewrite",
		Type:   sql.IntegerType,
	},
	{
		Column: "relfrozenxid",
		Type:   sql.IntegerType,
	},
	{
		Column: "relminmxid",
		Type:   sql.IntegerType,
	},
	{
		Column: "relacl",
		Type:   sql.AnyType,
	},
	{
		Column: "reloptions",
		Type:   sql.AnyType,
	},
	{
		Column: "relpartbound",
		Type:   sql.AnyType,
	},
}

type pgClassResolver struct{}

func (r *pgClassResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	rows := make([][]sql.ValueExp, len(tables))
	for i, t := range tables {
		rows[i] = []sql.ValueExp{
			sql.NewInteger(int64(t.ID())),        // oid
			sql.NewVarchar(t.Name()),             // relname
			sql.NewInteger(-1),                   // relnamespace
			sql.NewVarchar(""),                   // reltype
			sql.NewNull(sql.IntegerType),         // reloftype
			sql.NewInteger(0),                    // relowner
			sql.NewNull(sql.IntegerType),         // relam
			sql.NewNull(sql.IntegerType),         // relfilenode
			sql.NewNull(sql.IntegerType),         // reltablespace
			sql.NewNull(sql.IntegerType),         // relpages
			sql.NewNull(sql.Float64Type),         // reltuples
			sql.NewNull(sql.IntegerType),         // relallvisible
			sql.NewNull(sql.IntegerType),         // reltoastrelid
			sql.NewBool(len(t.GetIndexes()) > 1), // relhasindex
			sql.NewBool(false),                   // relisshared
			sql.NewNull(sql.VarcharType),         // relpersistence
			sql.NewVarchar("r"),                  // relkind
			sql.NewNull(sql.IntegerType),         // relnats
			sql.NewNull(sql.IntegerType),         // relchecks
			sql.NewBool(false),                   // relhasrules
			sql.NewBool(false),                   // relhastriggers
			sql.NewBool(false),                   // relhassubclass
			sql.NewBool(false),                   // relrowsecurity
			sql.NewBool(false),                   // relforcerowsecurity
			sql.NewBool(false),                   // relispopulated
			sql.NewVarchar(""),                   // relreplident
			sql.NewBool(false),                   // relispartition
			sql.NewInteger(0),                    // relrewrite
			sql.NewNull(sql.IntegerType),         // relfrozenxid
			sql.NewNull(sql.IntegerType),         // relminmxid
			sql.NewNull(sql.AnyType),             // relacl
			sql.NewNull(sql.AnyType),             // reloptions
			sql.NewNull(sql.AnyType),             // relpartbound
		}
	}

	return sql.NewValuesRowReader(
		tx,
		nil,
		pgClassCols,
		true,
		alias,
		rows,
	)
}

func (r *pgClassResolver) Table() string {
	return "pg_class"
}

var pgNamespaceCols = []sql.ColDescriptor{
	{
		Column: "oid",
		Type:   sql.IntegerType,
	},
	{
		Column: "nspname",
		Type:   sql.VarcharType,
	},
	{
		Column: "nspowner",
		Type:   sql.IntegerType,
	},
	{
		Column: "nspacl",
		Type:   sql.AnyType,
	},
}

type pgNamespaceResolver struct{}

func (r *pgNamespaceResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	return sql.NewValuesRowReader(
		tx,
		nil,
		pgNamespaceCols,
		true,
		alias,
		nil,
	)
}

func (r *pgNamespaceResolver) Table() string {
	return "pg_namespace"
}

var pgRolesCols = []sql.ColDescriptor{
	{
		Column: "rolname",
		Type:   sql.VarcharType,
	},
	{
		Column: "rolsuper",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolinherit",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolcreaterole",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolcreatedb",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolcanlogin",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolreplication",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolconnlimit",
		Type:   sql.IntegerType,
	},
	{
		Column: "rolpassword",
		Type:   sql.VarcharType,
	},
	{
		Column: "rolvaliduntil",
		Type:   sql.TimestampType,
	},
	{
		Column: "rolbypassrls",
		Type:   sql.BooleanType,
	},
	{
		Column: "rolconfig",
		Type:   sql.AnyType,
	},
	{
		Column: "oid",
		Type:   sql.IntegerType,
	},
}

type pgRolesResolver struct{}

func (r *pgRolesResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	users, err := tx.ListUsers(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([][]sql.ValueExp, len(users))
	for i, u := range users {
		isAdmin := u.Permission() == sql.PermissionSysAdmin || u.Permission() == sql.PermissionAdmin

		rows[i] = []sql.ValueExp{
			sql.NewVarchar(u.Username()),   // name
			sql.NewBool(isAdmin),           // rolsuper
			sql.NewBool(isAdmin),           // rolinherit
			sql.NewBool(isAdmin),           // rolcreaterole
			sql.NewBool(isAdmin),           // rolcreatedb
			sql.NewBool(true),              // rolcanlogin
			sql.NewBool(false),             // rolreplication
			sql.NewInteger(-1),             // rolconnlimit
			sql.NewVarchar("********"),     // rolpassword
			sql.NewNull(sql.TimestampType), // rolvaliduntil
			sql.NewBool(isAdmin),           // rolbypassrls
			sql.NewNull(sql.AnyType),       // rolconfig
			sql.NewNull(sql.IntegerType),   // oid
		}
	}

	return sql.NewValuesRowReader(
		tx,
		nil,
		pgRolesCols,
		true,
		alias,
		rows,
	)
}

func (r *pgRolesResolver) Table() string {
	return "pg_roles"
}

var tableResolvers = []sql.TableResolver{
	&pgClassResolver{},
	&pgNamespaceResolver{},
	&pgRolesResolver{},
}

func PgCatalogResolvers() []sql.TableResolver {
	return tableResolvers
}
