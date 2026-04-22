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

// pg_am lists access methods. immudb uses a single index implementation
// (B-tree variant), but psql's `\d` query joins against pg_am and
// expects at minimum 'btree' to be present; we also include 'hash'
// because the PG catalog has had both since forever and clients
// occasionally filter on amname.
//
// `amhandler` points at a handler function oid in real PG; we set it
// to 0 (NULL equivalent for the oid type) because no one queries it.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_am",
		Columns: []sql.SystemTableColumn{
			{Name: "oid", Type: sql.IntegerType},
			{Name: "amname", Type: sql.VarcharType, MaxLen: 64},
			{Name: "amhandler", Type: sql.IntegerType},
			{Name: "amtype", Type: sql.VarcharType, MaxLen: 1}, // 'i' = index, 't' = table
		},
		PKColumn: "oid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			return []*sql.Row{
				rowAm(OIDAccessMethodBtree, "btree"),
				rowAm(OIDAccessMethodHash, "hash"),
			}, nil
		},
	})
}

func rowAm(oid int64, name string) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(oid),
		sql.NewVarchar(name),
		sql.NewInteger(0),
		sql.NewVarchar("i"),
	}}
}
