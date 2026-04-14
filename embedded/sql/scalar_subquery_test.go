/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package sql

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

// TestScalarSubQuery_InUpdateSet covers the canonical XORM/Hibernate
// "recompute the count" pattern that previously failed at parse:
//
//	UPDATE repository SET num_pulls = (SELECT count(*) FROM …) WHERE …
//
// The grammar's `val` rule had no `( select_stmt )` production so the
// inner SELECT was rejected with `unexpected SELECT at position N`.
// Adding ScalarSubQueryExp + the primary-rule alternative makes
// scalar subqueries work in any expression position.
func TestScalarSubQuery_InUpdateSet(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))
	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	eng, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	ctx := context.Background()
	_, _, err = eng.Exec(ctx, nil,
		`CREATE TABLE parent (id INTEGER PRIMARY KEY, num_children INTEGER);
		 CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER);`, nil)
	require.NoError(t, err)

	_, _, err = eng.Exec(ctx, nil,
		`INSERT INTO parent (id, num_children) VALUES (1, 0);
		 INSERT INTO child (id, parent_id) VALUES (10, 1), (11, 1), (12, 1);`, nil)
	require.NoError(t, err)

	// Scalar subquery in UPDATE SET.
	_, _, err = eng.Exec(ctx, nil,
		`UPDATE parent SET num_children = (SELECT count(*) FROM child WHERE parent_id = 1) WHERE id = 1`, nil)
	require.NoError(t, err)

	rows, err := queryAll(t, eng, `SELECT num_children FROM parent WHERE id = 1`, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, int64(3), rows[0].ValuesByPosition[0].RawValue())
}

// TestScalarSubQuery_InWhere covers a scalar subquery in a SELECT WHERE
// clause — also a common ORM pattern that hit the same grammar gap.
func TestScalarSubQuery_InWhere(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))
	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	eng, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	ctx := context.Background()
	_, _, err = eng.Exec(ctx, nil,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
		 INSERT INTO t (id, n) VALUES (1, 10), (2, 20), (3, 30);`, nil)
	require.NoError(t, err)

	rows, err := queryAll(t, eng,
		`SELECT id FROM t WHERE n = (SELECT MAX(n) FROM t)`, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, int64(3), rows[0].ValuesByPosition[0].RawValue())
}

// TestScalarSubQuery_NoRows confirms that a subquery with no matching
// rows yields SQL NULL (matches PG semantics).
func TestScalarSubQuery_NoRows(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))
	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	eng, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	ctx := context.Background()
	_, _, err = eng.Exec(ctx, nil,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
		 INSERT INTO t (id, n) VALUES (1, 10);`, nil)
	require.NoError(t, err)

	// No row matches the inner WHERE → subquery returns NULL → outer
	// WHERE comparison is unknown → no rows.
	rows, err := queryAll(t, eng,
		`SELECT id FROM t WHERE n = (SELECT n FROM t WHERE id = 999)`, nil)
	require.NoError(t, err)
	require.Len(t, rows, 0)
}

// helper — ReadAllRows-style accumulator.
func queryAll(t *testing.T, e *Engine, sql string, params map[string]interface{}) ([]*Row, error) {
	t.Helper()
	r, err := e.Query(context.Background(), nil, sql, params)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ReadAllRows(context.Background(), r)
}
