/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestVerifyRowForColumnTypes(t *testing.T) {
	_, cli, ctx := setupTestServerAndClient(t)

	for i, d := range []struct {
		t  string
		v  interface{}
		vs string
		cv schema.SqlValue
	}{
		{"INTEGER", nil, "NULL", &schema.SQLValue_Null{}},
		{"INTEGER", 79, "79", &schema.SQLValue_N{N: 79}},
		{"VARCHAR", nil, "NULL", &schema.SQLValue_Null{}},
		{"VARCHAR", "abcd", "'abcd'", &schema.SQLValue_S{S: "abcd"}},
		{"BOOLEAN", nil, "NULL", &schema.SQLValue_Null{}},
		{"BOOLEAN", true, "TRUE", &schema.SQLValue_B{B: true}},
		{"BLOB", nil, "NULL", &schema.SQLValue_Null{}},
		{"BLOB", []byte{0xab, 0xcd, 0xef}, "x'ABCDEF'", &schema.SQLValue_Bs{Bs: []byte{0xab, 0xcd, 0xef}}},
		{"TIMESTAMP", nil, "NULL", &schema.SQLValue_Null{}},
		{"TIMESTAMP", time.Unix(1234, 0), "CAST(1234 AS TIMESTAMP)", &schema.SQLValue_Ts{Ts: 1234000000}},
		{"FLOAT", nil, "NULL", &schema.SQLValue_Null{}},
		{"FLOAT", 12.3456, "12.3456", &schema.SQLValue_F{F: 12.3456}},
	} {
		t.Run(fmt.Sprintf("%d %+v", i, d), func(t *testing.T) {

			tab := fmt.Sprintf("table_%d", i)

			t.Run("create table", func(t *testing.T) {
				_, err := cli.SQLExec(ctx, fmt.Sprintf(`
					CREATE TABLE %s(
						id INTEGER AUTO_INCREMENT,
						val %s,
						PRIMARY KEY (id)
					)
				`, tab, d.t),
					nil)
				require.NoError(t, err)
			})

			t.Run("insert with value in query string", func(t *testing.T) {
				_, err := cli.SQLExec(ctx, fmt.Sprintf(`
					INSERT INTO %s(val)
					VALUES (%s)
				`, tab, d.vs), nil)
				require.NoError(t, err)
			})

			t.Run("insert with value in query parameter", func(t *testing.T) {
				_, err := cli.SQLExec(ctx, fmt.Sprintf(`
					INSERT INTO %s(val)
					VALUES (@val)
				`, tab), map[string]interface{}{
					"val": d.v,
				})
				require.NoError(t, err)
			})

			t.Run("query and verify", func(t *testing.T) {
				res, err := cli.SQLQuery(ctx, fmt.Sprintf(`
				SELECT id, val FROM %s
				`, tab),
					nil, false)
				require.NoError(t, err)
				require.Len(t, res.Rows, 2)

				for _, row := range res.Rows {
					err = cli.VerifyRow(ctx, row, tab, []*schema.SQLValue{row.Values[0]})
					require.NoError(t, err)
					require.Equal(t, d.cv, row.Values[1].Value)
				}
			})
		})
	}
}
