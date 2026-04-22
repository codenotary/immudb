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

// pg_namespace has three synthetic rows — immudb has no user-facing
// schema concept (all user tables are conceptually in "public") so the
// set is fixed. We emit the same three PG ships with, using PG's own
// OIDs so cached-oid clients don't see drift.
//
// The nspowner/nspacl columns are stubs; any query that filters on
// them would get no results, which is harmless because no client
// actually drives ACL decisions from pg_namespace on immudb.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_namespace",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "nspname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "nspowner", Type: sql.IntegerType},
			{Name: "nspacl", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return []*sql.Row{
				rowNamespace(OIDNamespacePgCatalog, "pg_catalog"),
				rowNamespace(OIDNamespacePublic, "public"),
				rowNamespace(OIDNamespaceInformationSchema, "information_schema"),
			}, nil
		},
	})
}

func rowNamespace(oid int64, name string) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(oid),
		sql.NewVarchar(name),
		sql.NewInteger(10), // nspowner — arbitrary stable value matching pg_roles row
		sql.NewNull(sql.VarcharType),
	}}
}
