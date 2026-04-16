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

package pgschema

import (
	"context"
	"fmt"

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

// pg_attribute — column metadata per table
var pgAttributeCols = []sql.ColDescriptor{
	{Column: "attrelid", Type: sql.IntegerType},
	{Column: "attname", Type: sql.VarcharType},
	{Column: "atttypid", Type: sql.IntegerType},
	{Column: "attnum", Type: sql.IntegerType},
	{Column: "attlen", Type: sql.IntegerType},
	{Column: "atttypmod", Type: sql.IntegerType},
	{Column: "attnotnull", Type: sql.BooleanType},
	{Column: "atthasdef", Type: sql.BooleanType},
	{Column: "attisdropped", Type: sql.BooleanType},
	{Column: "attidentity", Type: sql.VarcharType},
	{Column: "attgenerated", Type: sql.VarcharType},
}

type pgAttributeResolver struct{}

func (r *pgAttributeResolver) Table() string { return "pg_attribute" }

func (r *pgAttributeResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	var rows [][]sql.ValueExp
	for _, t := range tables {
		for i, c := range t.Cols() {
			typeOID := immudbTypeToOID(c.Type())
			typeLen := immudbTypeToLen(c.Type())

			rows = append(rows, []sql.ValueExp{
				sql.NewInteger(int64(t.ID())),    // attrelid
				sql.NewVarchar(c.Name()),         // attname
				sql.NewInteger(int64(typeOID)),   // atttypid
				sql.NewInteger(int64(i + 1)),     // attnum (1-based)
				sql.NewInteger(int64(typeLen)),   // attlen
				sql.NewInteger(-1),               // atttypmod
				sql.NewBool(!c.IsNullable()),     // attnotnull
				sql.NewBool(false),               // atthasdef
				sql.NewBool(false),               // attisdropped
				sql.NewVarchar(""),               // attidentity
				sql.NewVarchar(""),               // attgenerated
			})
		}
	}

	return sql.NewValuesRowReader(tx, nil, pgAttributeCols, true, alias, rows)
}

// pg_index — index metadata
var pgIndexCols = []sql.ColDescriptor{
	{Column: "indexrelid", Type: sql.IntegerType},
	{Column: "indrelid", Type: sql.IntegerType},
	{Column: "indnatts", Type: sql.IntegerType},
	{Column: "indisunique", Type: sql.BooleanType},
	{Column: "indisprimary", Type: sql.BooleanType},
	{Column: "indkey", Type: sql.VarcharType},
}

type pgIndexResolver struct{}

func (r *pgIndexResolver) Table() string { return "pg_index" }

func (r *pgIndexResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	var rows [][]sql.ValueExp
	for _, t := range tables {
		for _, idx := range t.GetIndexes() {
			colNums := ""
			for i, c := range idx.Cols() {
				if i > 0 {
					colNums += " "
				}
				colNums += fmt.Sprintf("%d", c.ID())
			}

			rows = append(rows, []sql.ValueExp{
				sql.NewInteger(int64(idx.ID())),            // indexrelid
				sql.NewInteger(int64(t.ID())),              // indrelid
				sql.NewInteger(int64(len(idx.Cols()))),     // indnatts
				sql.NewBool(idx.IsUnique()),                // indisunique
				sql.NewBool(idx.IsPrimary()),               // indisprimary
				sql.NewVarchar(colNums),                    // indkey
			})
		}
	}

	return sql.NewValuesRowReader(tx, nil, pgIndexCols, true, alias, rows)
}

// pg_constraint — PK and CHECK constraints
var pgConstraintCols = []sql.ColDescriptor{
	{Column: "oid", Type: sql.IntegerType},
	{Column: "conname", Type: sql.VarcharType},
	{Column: "connamespace", Type: sql.IntegerType},
	{Column: "contype", Type: sql.VarcharType},
	{Column: "conrelid", Type: sql.IntegerType},
	{Column: "confrelid", Type: sql.IntegerType},
	{Column: "conkey", Type: sql.VarcharType},
}

type pgConstraintResolver struct{}

func (r *pgConstraintResolver) Table() string { return "pg_constraint" }

func (r *pgConstraintResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	var rows [][]sql.ValueExp
	oid := int64(10000)
	for _, t := range tables {
		pk := t.PrimaryIndex()
		if pk != nil {
			colNums := ""
			for i, c := range pk.Cols() {
				if i > 0 {
					colNums += ","
				}
				colNums += fmt.Sprintf("%d", c.ID())
			}

			rows = append(rows, []sql.ValueExp{
				sql.NewInteger(oid),              // oid
				sql.NewVarchar(t.Name() + "_pkey"), // conname
				sql.NewInteger(0),                // connamespace
				sql.NewVarchar("p"),              // contype (primary key)
				sql.NewInteger(int64(t.ID())),    // conrelid
				sql.NewInteger(0),                // confrelid
				sql.NewVarchar(colNums),          // conkey
			})
			oid++
		}
	}

	return sql.NewValuesRowReader(tx, nil, pgConstraintCols, true, alias, rows)
}

// pg_type — type catalog
var pgTypeCols = []sql.ColDescriptor{
	{Column: "oid", Type: sql.IntegerType},
	{Column: "typname", Type: sql.VarcharType},
	{Column: "typnamespace", Type: sql.IntegerType},
	{Column: "typlen", Type: sql.IntegerType},
	{Column: "typtype", Type: sql.VarcharType},
	{Column: "typbasetype", Type: sql.IntegerType},
	{Column: "typtypmod", Type: sql.IntegerType},
}

type pgTypeResolver struct{}

func (r *pgTypeResolver) Table() string { return "pg_type" }

func (r *pgTypeResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	typeRows := []struct {
		oid     int64
		name    string
		length  int64
		typtype string
	}{
		{16, "bool", 1, "b"},
		{17, "bytea", -1, "b"},
		{20, "int8", 8, "b"},
		{25, "text", -1, "b"},
		{114, "json", -1, "b"},
		{701, "float8", 8, "b"},
		{1114, "timestamp", 8, "b"},
		{2950, "uuid", 16, "b"},
	}

	rows := make([][]sql.ValueExp, len(typeRows))
	for i, tr := range typeRows {
		rows[i] = []sql.ValueExp{
			sql.NewInteger(tr.oid),       // oid
			sql.NewVarchar(tr.name),      // typname
			sql.NewInteger(11),           // typnamespace (pg_catalog)
			sql.NewInteger(tr.length),    // typlen
			sql.NewVarchar(tr.typtype),   // typtype
			sql.NewInteger(0),            // typbasetype
			sql.NewInteger(-1),           // typtypmod
		}
	}

	return sql.NewValuesRowReader(tx, nil, pgTypeCols, true, alias, rows)
}

// pg_settings — server configuration
var pgSettingsCols = []sql.ColDescriptor{
	{Column: "name", Type: sql.VarcharType},
	{Column: "setting", Type: sql.VarcharType},
	{Column: "unit", Type: sql.VarcharType},
	{Column: "category", Type: sql.VarcharType},
}

type pgSettingsResolver struct{}

func (r *pgSettingsResolver) Table() string { return "pg_settings" }

func (r *pgSettingsResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	settings := []struct{ name, setting, unit, category string }{
		{"server_version", "14.0", "", "Preset Options"},
		{"server_encoding", "UTF8", "", "Client Connection Defaults"},
		{"client_encoding", "UTF8", "", "Client Connection Defaults"},
		{"standard_conforming_strings", "on", "", "Client Connection Defaults"},
		{"DateStyle", "ISO, MDY", "", "Client Connection Defaults"},
		{"TimeZone", "UTC", "", "Client Connection Defaults"},
		{"integer_datetimes", "on", "", "Preset Options"},
		{"IntervalStyle", "postgres", "", "Client Connection Defaults"},
	}

	rows := make([][]sql.ValueExp, len(settings))
	for i, s := range settings {
		rows[i] = []sql.ValueExp{
			sql.NewVarchar(s.name),
			sql.NewVarchar(s.setting),
			sql.NewVarchar(s.unit),
			sql.NewVarchar(s.category),
		}
	}

	return sql.NewValuesRowReader(tx, nil, pgSettingsCols, true, alias, rows)
}

// pg_description — object descriptions (stub, returns empty)
var pgDescriptionCols = []sql.ColDescriptor{
	{Column: "objoid", Type: sql.IntegerType},
	{Column: "classoid", Type: sql.IntegerType},
	{Column: "objsubid", Type: sql.IntegerType},
	{Column: "description", Type: sql.VarcharType},
}

type pgDescriptionResolver struct{}

func (r *pgDescriptionResolver) Table() string { return "pg_description" }

func (r *pgDescriptionResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	return sql.NewValuesRowReader(tx, nil, pgDescriptionCols, true, alias, nil)
}

// information_schema_tables — table listing
var infoSchemaTablesCols = []sql.ColDescriptor{
	{Column: "table_catalog", Type: sql.VarcharType},
	{Column: "table_schema", Type: sql.VarcharType},
	{Column: "table_name", Type: sql.VarcharType},
	{Column: "table_type", Type: sql.VarcharType},
}

type infoSchemaTablesResolver struct{}

func (r *infoSchemaTablesResolver) Table() string { return "information_schema_tables" }

func (r *infoSchemaTablesResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	rows := make([][]sql.ValueExp, len(tables))
	for i, t := range tables {
		rows[i] = []sql.ValueExp{
			sql.NewVarchar("immudb"),      // table_catalog
			sql.NewVarchar("public"),      // table_schema
			sql.NewVarchar(t.Name()),      // table_name
			sql.NewVarchar("BASE TABLE"),  // table_type
		}
	}

	return sql.NewValuesRowReader(tx, nil, infoSchemaTablesCols, true, alias, rows)
}

// information_schema_columns — column listing
var infoSchemaColumnsCols = []sql.ColDescriptor{
	{Column: "table_catalog", Type: sql.VarcharType},
	{Column: "table_schema", Type: sql.VarcharType},
	{Column: "table_name", Type: sql.VarcharType},
	{Column: "column_name", Type: sql.VarcharType},
	{Column: "ordinal_position", Type: sql.IntegerType},
	{Column: "column_default", Type: sql.VarcharType},
	{Column: "is_nullable", Type: sql.VarcharType},
	{Column: "data_type", Type: sql.VarcharType},
	{Column: "character_maximum_length", Type: sql.IntegerType},
	{Column: "udt_name", Type: sql.VarcharType},
}

type infoSchemaColumnsResolver struct{}

func (r *infoSchemaColumnsResolver) Table() string { return "information_schema_columns" }

func (r *infoSchemaColumnsResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	var rows [][]sql.ValueExp
	for _, t := range tables {
		for i, c := range t.Cols() {
			nullable := "YES"
			if !c.IsNullable() {
				nullable = "NO"
			}

			maxLen := c.MaxLen()
			var maxLenVal sql.ValueExp
			if maxLen > 0 && (c.Type() == sql.VarcharType || c.Type() == sql.BLOBType) {
				maxLenVal = sql.NewInteger(int64(maxLen))
			} else {
				maxLenVal = sql.NewNull(sql.IntegerType)
			}

			rows = append(rows, []sql.ValueExp{
				sql.NewVarchar("immudb"),                    // table_catalog
				sql.NewVarchar("public"),                    // table_schema
				sql.NewVarchar(t.Name()),                    // table_name
				sql.NewVarchar(c.Name()),                    // column_name
				sql.NewInteger(int64(i + 1)),                // ordinal_position
				sql.NewNull(sql.VarcharType),                // column_default
				sql.NewVarchar(nullable),                    // is_nullable
				sql.NewVarchar(immudbTypeToPgName(c.Type())), // data_type
				maxLenVal,                                   // character_maximum_length
				sql.NewVarchar(immudbTypeToUdtName(c.Type())), // udt_name
			})
		}
	}

	return sql.NewValuesRowReader(tx, nil, infoSchemaColumnsCols, true, alias, rows)
}

// information_schema_schemata — schema listing
var infoSchemaSchemataCols = []sql.ColDescriptor{
	{Column: "catalog_name", Type: sql.VarcharType},
	{Column: "schema_name", Type: sql.VarcharType},
	{Column: "schema_owner", Type: sql.VarcharType},
}

type infoSchemaSchemataResolver struct{}

func (r *infoSchemaSchemataResolver) Table() string { return "information_schema_schemata" }

func (r *infoSchemaSchemataResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	rows := [][]sql.ValueExp{
		{
			sql.NewVarchar("immudb"),  // catalog_name
			sql.NewVarchar("public"),  // schema_name
			sql.NewVarchar("immudb"), // schema_owner
		},
	}

	return sql.NewValuesRowReader(tx, nil, infoSchemaSchemataCols, true, alias, rows)
}

// information_schema_key_column_usage — primary key column info
var infoSchemaKeyColumnUsageCols = []sql.ColDescriptor{
	{Column: "constraint_catalog", Type: sql.VarcharType},
	{Column: "constraint_schema", Type: sql.VarcharType},
	{Column: "constraint_name", Type: sql.VarcharType},
	{Column: "table_catalog", Type: sql.VarcharType},
	{Column: "table_schema", Type: sql.VarcharType},
	{Column: "table_name", Type: sql.VarcharType},
	{Column: "column_name", Type: sql.VarcharType},
	{Column: "ordinal_position", Type: sql.IntegerType},
}

type infoSchemaKeyColumnUsageResolver struct{}

func (r *infoSchemaKeyColumnUsageResolver) Table() string { return "information_schema_key_column_usage" }

func (r *infoSchemaKeyColumnUsageResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
	catalog := tx.Catalog()
	tables := catalog.GetTables()

	var rows [][]sql.ValueExp
	for _, t := range tables {
		pk := t.PrimaryIndex()
		if pk == nil {
			continue
		}

		constraintName := t.Name() + "_pkey"
		for i, c := range pk.Cols() {
			rows = append(rows, []sql.ValueExp{
				sql.NewVarchar("immudb"),         // constraint_catalog
				sql.NewVarchar("public"),         // constraint_schema
				sql.NewVarchar(constraintName),   // constraint_name
				sql.NewVarchar("immudb"),         // table_catalog
				sql.NewVarchar("public"),         // table_schema
				sql.NewVarchar(t.Name()),         // table_name
				sql.NewVarchar(c.Name()),         // column_name
				sql.NewInteger(int64(i + 1)),     // ordinal_position
			})
		}
	}

	return sql.NewValuesRowReader(tx, nil, infoSchemaKeyColumnUsageCols, true, alias, rows)
}

// Helper functions for type mapping

func immudbTypeToOID(t sql.SQLValueType) int {
	switch t {
	case sql.BooleanType:
		return 16
	case sql.BLOBType:
		return 17
	case sql.IntegerType:
		return 20
	case sql.VarcharType:
		return 25
	case sql.JSONType:
		return 114
	case sql.Float64Type:
		return 701
	case sql.TimestampType:
		return 1114
	case sql.UUIDType:
		return 2950
	default:
		return 25 // text fallback
	}
}

func immudbTypeToLen(t sql.SQLValueType) int {
	switch t {
	case sql.BooleanType:
		return 1
	case sql.IntegerType, sql.Float64Type, sql.TimestampType:
		return 8
	case sql.UUIDType:
		return 16
	default:
		return -1
	}
}

func immudbTypeToPgName(t sql.SQLValueType) string {
	switch t {
	case sql.BooleanType:
		return "boolean"
	case sql.IntegerType:
		return "bigint"
	case sql.VarcharType:
		return "text"
	case sql.BLOBType:
		return "bytea"
	case sql.Float64Type:
		return "double precision"
	case sql.TimestampType:
		return "timestamp without time zone"
	case sql.UUIDType:
		return "uuid"
	case sql.JSONType:
		return "json"
	default:
		return "text"
	}
}

func immudbTypeToUdtName(t sql.SQLValueType) string {
	switch t {
	case sql.BooleanType:
		return "bool"
	case sql.IntegerType:
		return "int8"
	case sql.VarcharType:
		return "text"
	case sql.BLOBType:
		return "bytea"
	case sql.Float64Type:
		return "float8"
	case sql.TimestampType:
		return "timestamp"
	case sql.UUIDType:
		return "uuid"
	case sql.JSONType:
		return "json"
	default:
		return "text"
	}
}

var tableResolvers = []sql.TableResolver{
	&pgClassResolver{},
	&pgNamespaceResolver{},
	&pgRolesResolver{},
	&pgAttributeResolver{},
	&pgIndexResolver{},
	&pgConstraintResolver{},
	&pgTypeResolver{},
	&pgSettingsResolver{},
	&pgDescriptionResolver{},
	&infoSchemaTablesResolver{},
	&infoSchemaColumnsResolver{},
	&infoSchemaSchemataResolver{},
	&infoSchemaKeyColumnUsageResolver{},
}

func PgCatalogResolvers() []sql.TableResolver {
	return tableResolvers
}
