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

// Three empty-stub tables that psql's `\d <table>` tail-section
// probes:
//
//	pg_policy        — row-level-security policies (PG ≥ 9.5).
//	                   immudb has no RLS, table is empty.
//	pg_publication   — logical-replication publications (PG ≥ 10).
//	                   immudb has no logical replication here.
//	pg_statistic_ext — extended statistics (PG ≥ 10). immudb's
//	                   planner has no extended-stats object model.
//
// Each is registered non-empty-schema but Scan returns nil. The
// registration matters because without it psql's tail queries fall
// to the canned pgAdminProbe handler, which invents canned rows
// and renders junk like "Policies: POLICY ''" / "Publications: ''"
// / "Statistics objects: '.' ON  FROM" in the `\d` output. Empty
// tables → count(*) returns 0 → psql suppresses the whole section.
//
// Column shapes are a small superset of what psql joins on. If a
// future psql version joins on a column we don't advertise, the
// engine errors and we either extend the schema here or the query
// falls back to the canned handler (and the user sees a harmless
// noise section). Either failure mode is strictly better than the
// current "wrong-looking output" we're replacing.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_policy",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "polname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "polrelid", Type: sql.IntegerType},
			{Name: "polcmd", Type: sql.VarcharType, MaxLen: 1},
			{Name: "polpermissive", Type: sql.BooleanType},
			{Name: "polroles", Type: sql.VarcharType, MaxLen: 256},
			{Name: "polqual", Type: sql.VarcharType, MaxLen: 1024},
			{Name: "polwithcheck", Type: sql.VarcharType, MaxLen: 1024},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})

	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_publication",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "pubname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "pubowner", Type: sql.IntegerType},
			{Name: "puballtables", Type: sql.BooleanType},
			{Name: "pubinsert", Type: sql.BooleanType},
			{Name: "pubupdate", Type: sql.BooleanType},
			{Name: "pubdelete", Type: sql.BooleanType},
			{Name: "pubtruncate", Type: sql.BooleanType},
			{Name: "pubviaroot", Type: sql.BooleanType},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})

	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_statistic_ext",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "stxrelid", Type: sql.IntegerType},
			{Name: "stxname", Type: sql.VarcharType, MaxLen: 128},
			{Name: "stxnamespace", Type: sql.IntegerType},
			{Name: "stxowner", Type: sql.IntegerType},
			{Name: "stxstattarget", Type: sql.IntegerType},
			{Name: "stxkeys", Type: sql.VarcharType, MaxLen: 256},
			{Name: "stxkind", Type: sql.VarcharType, MaxLen: 16},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
