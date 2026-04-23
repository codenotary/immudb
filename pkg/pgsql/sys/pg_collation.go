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

// pg_collation: collation metadata. immudb has no collation support
// (the default byte-wise comparison is always in effect), so this
// table is always empty. Registered non-empty-schema purely so the
// dispatcher's allPgRefsRegistered passthrough accepts psql's `\d`
// collation subquery:
//
//	(SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
//	 WHERE c.oid = a.attcollation AND t.oid = a.atttypid
//	       AND a.attcollation <> t.typcollation) AS attcollation
//
// With the table registered empty the subquery correlates to zero
// rows and psql shows an empty "Collation" column — correct for
// immudb which has no per-column collation concept.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_collation",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "collname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "collnamespace", Type: sql.IntegerType},
			{Name: "collowner", Type: sql.IntegerType},
			{Name: "collprovider", Type: sql.VarcharType, MaxLen: 1},
			{Name: "collisdeterministic", Type: sql.BooleanType},
			{Name: "collencoding", Type: sql.IntegerType},
			{Name: "collcollate", Type: sql.VarcharType, MaxLen: 64},
			{Name: "collctype", Type: sql.VarcharType, MaxLen: 64},
			{Name: "collversion", Type: sql.VarcharType, MaxLen: 64},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
