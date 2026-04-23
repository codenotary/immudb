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

// information_schema.schemata — one row per schema visible to the
// session. immudb has no user-facing schema concept so the set is
// fixed: public, pg_catalog, information_schema. Matches pg_namespace
// (A2) but with the information_schema standard column shape.
//
// Expanded from pgschema.infoSchemaSchemataResolver (which returned
// only 'public') so psql's `\dn` via the standard view shows the
// system schemas too.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "information_schema_schemata",
		Columns: []sql.SystemTableColumn{
			{Name: "catalog_name", Type: sql.VarcharType, MaxLen: 64},
			{Name: "schema_name", Type: sql.VarcharType, MaxLen: 64},
			{Name: "schema_owner", Type: sql.VarcharType, MaxLen: 64},
			{Name: "default_character_set_catalog", Type: sql.VarcharType, MaxLen: 1},
			{Name: "default_character_set_schema", Type: sql.VarcharType, MaxLen: 1},
			{Name: "default_character_set_name", Type: sql.VarcharType, MaxLen: 1},
			{Name: "sql_path", Type: sql.VarcharType, MaxLen: 1},
			// Synthesised PK: the natural key is schema_name but we
			// keep it explicit so the registry contract is uniform
			// across every info_schema table.
			{Name: "schema_oid", Type: sql.IntegerType},
		},
		PKColumn: "schema_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return []*sql.Row{
				rowInformationSchemaSchema(OIDNamespacePgCatalog, "pg_catalog"),
				rowInformationSchemaSchema(OIDNamespacePublic, "public"),
				rowInformationSchemaSchema(OIDNamespaceInformationSchema, "information_schema"),
			}, nil
		},
	})
}

func rowInformationSchemaSchema(oid int64, name string) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewVarchar("immudb"),
		sql.NewVarchar(name),
		sql.NewVarchar("immudb"),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewInteger(oid),
	}}
}
