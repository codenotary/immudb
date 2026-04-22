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

// pg_views: one row per user-defined view.
//
// immudb supports CREATE VIEW at the engine layer but views aren't
// enumerable via Catalog.GetTables() — they're stored as engine-level
// table resolvers keyed by name, not as rows in the regular catalog.
// Exposing them here requires a separate accessor on Catalog/engine
// that we don't yet export.
//
// Registering the table with a 0-row Scan matters because psql `\dv`
// and pgAdmin's view browser JOIN this — without a registration the
// dispatcher's allPgRefsRegistered would reject the query and route
// it to the canned handler instead of letting the engine serve the
// JOIN correctly.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_views",
		Columns: []sql.SystemTableColumn{
			{Name: "schemaname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "viewname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "viewowner", Type: sql.VarcharType, MaxLen: 64},
			{Name: "definition", Type: sql.VarcharType, MaxLen: 4096},
			{Name: "view_oid", Type: sql.IntegerType},
		},
		PKColumn: "view_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
