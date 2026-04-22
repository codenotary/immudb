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

// pg_sequences: one row per user sequence.
//
// immudb has NEXTVAL / CURRVAL function shims that route to AUTO_INCREMENT
// columns (see embedded/sql/functions.go) but no CREATE SEQUENCE DDL,
// so there are no user-owned sequences to enumerate. The table is
// registered empty so Rails' `SELECT sequencename FROM pg_sequences …`
// and the similar XORM / SQLAlchemy probes get the right shape rather
// than a canned-handler error.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_sequences",
		Columns: []sql.SystemTableColumn{
			{Name: "schemaname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "sequencename", Type: sql.VarcharType, MaxLen: 128},
			{Name: "sequenceowner", Type: sql.VarcharType, MaxLen: 64},
			{Name: "data_type", Type: sql.VarcharType, MaxLen: 32},
			{Name: "start_value", Type: sql.IntegerType},
			{Name: "min_value", Type: sql.IntegerType},
			{Name: "max_value", Type: sql.IntegerType},
			{Name: "increment_by", Type: sql.IntegerType},
			{Name: "cycle", Type: sql.BooleanType},
			{Name: "cache_size", Type: sql.IntegerType},
			{Name: "last_value", Type: sql.IntegerType},
			{Name: "seq_oid", Type: sql.IntegerType},
		},
		PKColumn: "seq_oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
