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

// pg_inherits: table-inheritance relationships. immudb has no
// CREATE TABLE … INHERITS (…) — tables have no parent/child
// structure — so this always returns 0 rows.
//
// Registered empty specifically to stop psql's `\d <table>`
// tail-section probe
//
//	SELECT count(*) FROM pg_catalog.pg_inherits WHERE inhparent = '<oid>'
//
// from falling to the canned pgAdminProbe handler (which would
// return 1 and make psql render "Number of child tables: 1"). The
// empty Scan makes count(*) return 0, and the whole tail section
// disappears from psql's output.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_inherits",
		Columns: []sql.SystemTableColumn{
			{Name: "inhrelid", Type: sql.IntegerType},
			{Name: "inhparent", Type: sql.IntegerType},
			{Name: "inhseqno", Type: sql.IntegerType},
			{Name: "inhdetachpending", Type: sql.BooleanType},
			// Synthetic PK — natural key is (inhrelid, inhseqno).
			{Name: "inh_oid", Type: sql.IntegerType},
		},
		PKColumn: "inh_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
