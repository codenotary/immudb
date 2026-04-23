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
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
)

// pg_index: one row per index on a user table. indexrelid points at
// the pg_class row for the index (relkind='i'); indrelid points at
// the pg_class row for the table the index is on.
//
// indkey in real PG is an int2vector — a space-separated list of
// attnums the index covers. We emit the same textual form as varchar
// because immudb SQL doesn't have a native int2vector, and every
// client we've seen treats indkey as text anyway (psql parses it as a
// space-separated list of ints).
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_index",
		Columns: []sql.SystemTableColumn{
			{Name: "indexrelid", Type: sql.IntegerType},
			{Name: "indrelid", Type: sql.IntegerType},
			{Name: "indnatts", Type: sql.IntegerType},
			{Name: "indnkeyatts", Type: sql.IntegerType},
			{Name: "indisunique", Type: sql.BooleanType},
			{Name: "indisprimary", Type: sql.BooleanType},
			{Name: "indisexclusion", Type: sql.BooleanType},
			{Name: "indimmediate", Type: sql.BooleanType},
			{Name: "indisclustered", Type: sql.BooleanType},
			{Name: "indisvalid", Type: sql.BooleanType},
			{Name: "indcheckxmin", Type: sql.BooleanType},
			{Name: "indisready", Type: sql.BooleanType},
			{Name: "indislive", Type: sql.BooleanType},
			{Name: "indisreplident", Type: sql.BooleanType},
			{Name: "indkey", Type: sql.VarcharType, MaxLen: 256},
			{Name: "indcollation", Type: sql.VarcharType, MaxLen: 256},
			{Name: "indclass", Type: sql.VarcharType, MaxLen: 256},
			{Name: "indoption", Type: sql.VarcharType, MaxLen: 256},
			{Name: "indexprs", Type: sql.VarcharType, MaxLen: 1},
			{Name: "indpred", Type: sql.VarcharType, MaxLen: 1},
		},
		PKColumn: "indexrelid",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			cat := tx.Catalog()
			if cat == nil {
				return nil, nil
			}
			tables := cat.GetTables()
			rows := make([]*sql.Row, 0, len(tables)*2)

			for _, t := range tables {
				tableOID := relOID("public", t.Name())
				for _, idx := range t.GetIndexes() {
					rows = append(rows, rowPgIndex(tableOID, t, idx))
				}
			}
			return rows, nil
		},
	})
}

func rowPgIndex(tableOID int64, t *sql.Table, idx *sql.Index) *sql.Row {
	cols := idx.Cols()

	// Build indkey: space-separated attnum list.
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = strconv.Itoa(int(c.ID()))
	}
	indkey := strings.Join(parts, " ")

	// indcollation, indclass, indoption: vectors of the same length as
	// indkey, each a zero-value. psql's \d reads them but tolerates all
	// zeros (= default collation / default opclass / default options).
	zeros := make([]string, len(cols))
	for i := range zeros {
		zeros[i] = "0"
	}
	zerosJoined := strings.Join(zeros, " ")

	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewInteger(relOID("public", idx.Name())),
		sql.NewInteger(tableOID),
		sql.NewInteger(int64(len(cols))),
		sql.NewInteger(int64(len(cols))), // indnkeyatts — we have no INCLUDE cols
		sql.NewBool(idx.IsUnique()),
		sql.NewBool(idx.IsPrimary()),
		sql.NewBool(false), // indisexclusion
		sql.NewBool(true),  // indimmediate — always; we don't support DEFERRABLE
		sql.NewBool(false),
		sql.NewBool(true), // indisvalid
		sql.NewBool(false),
		sql.NewBool(true),
		sql.NewBool(true),
		sql.NewBool(idx.IsPrimary()), // indisreplident mirrors PK for simplicity
		sql.NewVarchar(indkey),
		sql.NewVarchar(zerosJoined),
		sql.NewVarchar(zerosJoined),
		sql.NewVarchar(zerosJoined),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
	}}
}
