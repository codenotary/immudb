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

// pg_attrdef: column-default metadata. immudb stores column
// defaults on the Column struct directly rather than as a separate
// catalog table, so there's no natural row source. Registering the
// table empty (0 rows) is still necessary because psql's `\d
// <table>` detail query JOINs pg_attrdef in a subquery to fetch
// the default expression text:
//
//	(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
//	 FROM pg_catalog.pg_attrdef d
//	 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
//
// Without this registration the dispatcher's allPgRefsRegistered
// marks the whole query unregistered and shunts it to the canned
// pgAdminProbe handler, which then matches the "pg_type" substring
// (from the sibling pg_collation/pg_type subquery) and returns a
// synthesized stdPgTypes row set — 23 all-NULL rows where the user
// expects their actual column metadata.
//
// The correct behaviour with an empty pg_attrdef: the subquery
// returns NULL for every row, which psql renders as a blank
// "Default" column. That's semantically correct for immudb tables
// (defaults live in the column, not in a side table).
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_attrdef",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "adrelid", Type: sql.IntegerType},
			{Name: "adnum", Type: sql.IntegerType},
			{Name: "adbin", Type: sql.VarcharType, MaxLen: 1024},
			{Name: "adsrc", Type: sql.VarcharType, MaxLen: 1024},
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
