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

// information_schema.columns — one row per column per user table.
//
// This is the single most-probed view in the PG compatibility surface:
// Alembic, Flyway, Hibernate, JDBC, psql `\d`, SQLAlchemy reflection —
// every one of them introspects columns via this view. A missing row
// here means the client silently assumes "column doesn't exist" and
// skips migrations / renders wrong DDL.
//
// Column shape is a superset of pgschema.infoSchemaColumnsResolver
// (A4 adds numeric_precision, numeric_scale, datetime_precision plus
// the standard audit columns). Anything the legacy resolver returned
// still resolves with the same value here.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "information_schema_columns",
		Columns: []sql.SystemTableColumn{
			{Name: "table_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "table_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "column_name", Type: sql.VarcharType, MaxLen: 128},
			{Name: "ordinal_position", Type: sql.IntegerType},
			{Name: "column_default", Type: sql.VarcharType, MaxLen: 256},
			{Name: "is_nullable", Type: sql.VarcharType, MaxLen: 3},
			{Name: "data_type", Type: sql.VarcharType, MaxLen: 64},
			{Name: "character_maximum_length", Type: sql.IntegerType},
			{Name: "character_octet_length", Type: sql.IntegerType},
			{Name: "numeric_precision", Type: sql.IntegerType},
			{Name: "numeric_precision_radix", Type: sql.IntegerType},
			{Name: "numeric_scale", Type: sql.IntegerType},
			{Name: "datetime_precision", Type: sql.IntegerType},
			{Name: "udt_catalog", Type: sql.VarcharType, MaxLen: 64},
			{Name: "udt_schema", Type: sql.VarcharType, MaxLen: 64},
			{Name: "udt_name", Type: sql.VarcharType, MaxLen: 32},
			{Name: "is_identity", Type: sql.VarcharType, MaxLen: 3},
			{Name: "is_generated", Type: sql.VarcharType, MaxLen: 16},
			{Name: "is_updatable", Type: sql.VarcharType, MaxLen: 3},
			// Synthesised PK: (catalog, schema, table, column) isn't a
			// single-column key.
			{Name: "column_oid", Type: sql.IntegerType},
		},
		PKColumn: "column_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0, len(tables)*8)
			for _, t := range tables {
				// Match pg_attribute's PK-implies-NOT-NULL treatment so
				// information_schema.columns reports is_nullable='NO'
				// for every PK column — matches real PG behaviour and
				// what the pre-A4 hardened test asserts.
				pkCols := map[string]struct{}{}
				if pk := t.PrimaryIndex(); pk != nil {
					for _, c := range pk.Cols() {
						pkCols[c.Name()] = struct{}{}
					}
				}
				for i, c := range t.Cols() {
					_, isPK := pkCols[c.Name()]
					rows = append(rows, rowInformationSchemaColumn(t.Name(), c, i+1, isPK))
				}
			}
			return rows, nil
		},
	})
}

func rowInformationSchemaColumn(tableName string, c *sql.Column, ordinal int, isPK bool) *sql.Row {
	nullable := "YES"
	if !c.IsNullable() || isPK {
		nullable = "NO"
	}

	var charMaxLen, charOctetLen sql.TypedValue = sql.NewNull(sql.IntegerType), sql.NewNull(sql.IntegerType)
	if c.Type() == sql.VarcharType && c.MaxLen() > 0 {
		charMaxLen = sql.NewInteger(int64(c.MaxLen()))
		// PG reports octet length as max_len*4 for UTF-8; keep it the
		// same as character_maximum_length for simplicity — every
		// client we've seen reads only one of the two.
		charOctetLen = sql.NewInteger(int64(c.MaxLen()))
	}

	var numericPrecision, numericPrecisionRadix, numericScale sql.TypedValue =
		sql.NewNull(sql.IntegerType), sql.NewNull(sql.IntegerType), sql.NewNull(sql.IntegerType)
	switch c.Type() {
	case sql.IntegerType:
		numericPrecision = sql.NewInteger(64)
		numericPrecisionRadix = sql.NewInteger(2)
		numericScale = sql.NewInteger(0)
	case sql.Float64Type:
		numericPrecision = sql.NewInteger(53)
		numericPrecisionRadix = sql.NewInteger(2)
	}

	var datetimePrecision sql.TypedValue = sql.NewNull(sql.IntegerType)
	if c.Type() == sql.TimestampType {
		datetimePrecision = sql.NewInteger(6)
	}

	var columnDefault sql.TypedValue = sql.NewNull(sql.VarcharType)
	if c.HasDefault() {
		// Default expressions aren't easily stringifiable without an
		// expression formatter; exposing the raw Go value is good
		// enough for clients that only care "is there a default".
		columnDefault = sql.NewVarchar("DEFAULT")
	}

	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewVarchar("immudb"),
		sql.NewVarchar("public"),
		sql.NewVarchar(tableName),
		sql.NewVarchar(c.Name()),
		sql.NewInteger(int64(ordinal)),
		columnDefault,
		sql.NewVarchar(nullable),
		sql.NewVarchar(infoSchemaDataType(c.Type())),
		charMaxLen,
		charOctetLen,
		numericPrecision,
		numericPrecisionRadix,
		numericScale,
		datetimePrecision,
		sql.NewVarchar("immudb"),
		sql.NewVarchar("pg_catalog"),
		sql.NewVarchar(infoSchemaUDTName(c.Type())),
		sql.NewVarchar("NO"), // is_identity — immudb has AUTO_INCREMENT, not identity
		sql.NewVarchar("NEVER"),
		sql.NewVarchar("YES"),
		sql.NewInteger(colOID("public", tableName, c.Name())),
	}}
}

// infoSchemaDataType returns the value information_schema.columns.data_type
// emits for each immudb SQLValueType. Matches the pre-A4 pgschema
// resolver shape (immudbTypeToPgName) so JDBC / Alembic / hardened
// test assertions are unchanged. Note: real PG reports VARCHAR as
// `character varying`; we report `text` because immudb's VARCHAR
// behaves like PG's TEXT at the storage layer and clients don't care
// about the distinction for type introspection.
func infoSchemaDataType(t sql.SQLValueType) string {
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
		return "jsonb"
	default:
		return "text"
	}
}

// infoSchemaUDTName is the short PG type name for udt_name / udt_schema
// output. Stays in sync with pg_type.typname.
func infoSchemaUDTName(t sql.SQLValueType) string {
	switch t {
	case sql.BooleanType:
		return "bool"
	case sql.IntegerType:
		return "int8"
	case sql.VarcharType:
		return "varchar"
	case sql.BLOBType:
		return "bytea"
	case sql.Float64Type:
		return "float8"
	case sql.TimestampType:
		return "timestamp"
	case sql.UUIDType:
		return "uuid"
	case sql.JSONType:
		return "jsonb"
	default:
		return "text"
	}
}
