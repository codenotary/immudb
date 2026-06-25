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

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSortSpillWithProjectionPushdownDoesNotPanic reproduces issue #2100
// (panic #1): a SELECT that does NOT reference every table column, over a
// result set large enough to spill the sort buffer to a temp file, used to
// panic with a nil pointer dereference in encodeRow.
//
// Projection pushdown (rawRowReader.Read) leaves row.ValuesByPosition[i] == nil
// for columns excluded by neededColIDs. The disk-spill sort path
// (fileSorter.flushBuffer -> encodeRow) iterates every position and called
// v.Type() on those nil slots, crashing the whole server.
//
// The reproduction query shape from the issue is:
//
//	SELECT a, b FROM t WHERE ... ORDER BY id LIMIT n OFFSET m
//
// The OFFSET clause disables the top-N heap optimisation (it requires
// offset == nil), forcing the full file-sort path; a small sortBufferSize plus
// enough rows forces the spill where the panic lived.
func TestSortSpillWithProjectionPushdownDoesNotPanic(t *testing.T) {
	engine := setupCommonTest(t)
	engine.sortBufferSize = 8

	ctx := context.Background()
	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}

	// "unused" is NOT referenced by the query below, so projection pushdown
	// excludes it from neededColIDs and leaves its position nil on every row.
	mustExec(`CREATE TABLE events (
		id INTEGER NOT NULL AUTO_INCREMENT,
		grp INTEGER NOT NULL,
		payload VARCHAR[256] NOT NULL,
		unused VARCHAR[256] NOT NULL,
		PRIMARY KEY(id)
	);`)

	const nRows = 50
	for i := 1; i <= nRows; i++ {
		mustExec(fmt.Sprintf(
			`INSERT INTO events (grp, payload, unused) VALUES (%d, 'payload_%d', 'unused_%d');`,
			i, i, i,
		))
	}

	// SELECT references id, grp and payload but NOT "unused" => the unused
	// column is nil in ValuesByPosition. OFFSET forces the full file-sort and
	// the small buffer forces a disk spill.
	rows, err := engine.queryAll(ctx, nil,
		`SELECT id, payload FROM events ORDER BY grp ASC LIMIT 20 OFFSET 0;`,
		nil,
	)
	require.NoError(t, err, "#2100: sort spill with projection pushdown must not panic/error")
	require.Len(t, rows, 20)

	for i, r := range rows {
		idVal, ok := r.ValuesByPosition[0].RawValue().(int64)
		require.Truef(t, ok, "row %d: id should be int64, got %T", i, r.ValuesByPosition[0].RawValue())
		// grp == id in this dataset, and ORDER BY grp ASC LIMIT 20 returns the
		// first 20 inserted rows in order.
		require.Equalf(t, int64(i+1), idVal, "row %d: rows must be in ORDER BY grp order", i)

		field := r.ValuesByPosition[1]
		require.NotNilf(t, field, "row %d: payload must not be nil (id=%d)", i, idVal)
		require.Falsef(t, field.IsNull(), "row %d: payload must not be NULL (id=%d)", i, idVal)
		require.Equalf(t, fmt.Sprintf("payload_%d", idVal), field.RawValue().(string),
			"row %d: payload must round-trip through sort spill for id=%d", i, idVal)
	}
}
