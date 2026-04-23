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

// pg_description: object comments. immudb has no COMMENT ON statement,
// so this always returns 0 rows. Registering it anyway keeps psql's
// LEFT JOIN pg_description queries happy — without a registration the
// dispatcher's allPgRefsRegistered would reject the query and fall
// through to the canned handler.
//
// The synthetic `objoid_key` PK column exists only because the
// SystemTableDef registry requires a single-column PK; the natural
// PG key is (objoid, classoid, objsubid) which isn't expressible here.
// Empty row set means the PK is never exercised.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_description",
		Columns: []sql.SystemTableColumn{
			{Name: "objoid", Type: sql.IntegerType},
			{Name: "classoid", Type: sql.IntegerType},
			{Name: "objsubid", Type: sql.IntegerType},
			{Name: "description", Type: sql.VarcharType, MaxLen: 1024},
			{Name: "objoid_key", Type: sql.IntegerType}, // synthesized PK
		},
		PKColumn: "objoid_key",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return nil, nil
		},
	})
}
