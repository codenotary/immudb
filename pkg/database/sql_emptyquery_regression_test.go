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

package database

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

// TestSQLQueryEmptyStatementDoesNotPanic reproduces issue #2100 (panic #2):
// db.SQLQuery indexed stmts[0] without a length guard. Since 1.11.0 the SQL
// grammar accepts empty / comment-only input and ParseSQL returns an empty
// statement slice with no error, so an empty query (routine probe traffic from
// drivers) panicked with "index out of range [0] with length 0", crashing the
// server. It must return ErrExpectingDQLStmt instead.
func TestSQLQueryEmptyStatementDoesNotPanic(t *testing.T) {
	db := makeDb(t)

	for _, q := range []string{"", "   ", "\n\t ", "-- just a comment", "/* nothing here */"} {
		_, err := db.SQLQuery(context.Background(), nil, &schema.SQLQueryRequest{Sql: q})
		require.ErrorIs(t, err, sql.ErrExpectingDQLStmt, "empty query %q must not panic", q)

		_, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: q})
		require.ErrorIs(t, err, sql.ErrExpectingDQLStmt, "empty query %q must not panic", q)
	}
}
