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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrderByProjectedVarcharSurvivesSortSpill reproduces issues #2041 and
// #2044: a SELECT that projects a VARCHAR column which is NOT in the ORDER BY
// list, over a row count large enough to spill the sort buffer to a temp file,
// used to return a spurious NULL for the projected VARCHAR. Through the pgsql
// wire, that NULL surfaced on the Go client as
// `converting NULL to string is unsupported`.
//
// The table schema and query mirror the reproduction in #2041:
//
//	SELECT id, req_primary_fields FROM audit_log
//	 ORDER BY created_tstamp DESC, id ASC LIMIT 10 OFFSET 0
//
// and the no-LIMIT variant from #2044. The column req_primary_fields is
// populated with non-NULL values on every row; any NULL coming back out is a
// regression.
//
// The test forces the sort-spill path by dropping sortBufferSize to 8 and
// inserting 50 rows — small and fast, but above the buffer size so
// mergeAllChunks runs. To sanity-check that the assertions are not tautological
// a reader can temporarily set engine.sortBufferSize = 100000 (disable spill)
// and confirm the test still passes; only the spill path is what used to break.
func TestOrderByProjectedVarcharSurvivesSortSpill(t *testing.T) {
	engine := setupCommonTest(t)
	engine.sortBufferSize = 8

	ctx := context.Background()
	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}

	mustExec(`CREATE TABLE audit_log (
		id INTEGER NOT NULL AUTO_INCREMENT,
		created_tstamp INTEGER NOT NULL,
		req_primary_fields VARCHAR[256] NOT NULL,
		PRIMARY KEY(id)
	);`)

	const nRows = 50
	for i := 1; i <= nRows; i++ {
		// created_tstamp strictly decreases with id, so ORDER BY created_tstamp
		// DESC returns the freshest-inserted (= highest-id) rows first.
		ts := nRows - i + 1
		mustExec(fmt.Sprintf(
			`INSERT INTO audit_log (created_tstamp, req_primary_fields) VALUES (%d, 'field_%d');`,
			ts, i,
		))
	}

	t.Run("limit10", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil,
			`SELECT id, req_primary_fields FROM audit_log
			 ORDER BY created_tstamp DESC, id ASC LIMIT 10 OFFSET 0;`,
			nil,
		)
		require.NoError(t, err, "#2041 query must not error on projected VARCHAR after sort spill")
		require.Len(t, rows, 10)

		for i, r := range rows {
			idVal, ok := r.ValuesByPosition[0].RawValue().(int64)
			require.Truef(t, ok, "row %d: id should be int64, got %T", i, r.ValuesByPosition[0].RawValue())

			field := r.ValuesByPosition[1]
			require.NotNilf(t, field, "row %d: req_primary_fields must not be nil (id=%d)", i, idVal)
			require.Falsef(t, field.IsNull(),
				"row %d: req_primary_fields must not be NULL (id=%d) — this is #2041/#2044", i, idVal)

			strVal, ok := field.RawValue().(string)
			require.Truef(t, ok, "row %d: req_primary_fields should be string, got %T", i, field.RawValue())
			require.Equalf(t, fmt.Sprintf("field_%d", idVal), strVal,
				"row %d: req_primary_fields value must match the one inserted for id=%d", i, idVal)
		}
	})

	t.Run("noLimit", func(t *testing.T) {
		// Same query without LIMIT — bypasses the top-N heap path and forces
		// the full file-sort. Covers #2044 which did not include a LIMIT.
		rows, err := engine.queryAll(ctx, nil,
			`SELECT id, req_primary_fields FROM audit_log
			 ORDER BY created_tstamp DESC, id ASC;`,
			nil,
		)
		require.NoError(t, err, "#2044 no-LIMIT query must not error on projected VARCHAR after sort spill")
		require.Len(t, rows, nRows)

		for i, r := range rows {
			idVal, ok := r.ValuesByPosition[0].RawValue().(int64)
			require.Truef(t, ok, "row %d: id should be int64", i)

			field := r.ValuesByPosition[1]
			require.NotNilf(t, field, "row %d: req_primary_fields must not be nil (id=%d)", i, idVal)
			require.Falsef(t, field.IsNull(),
				"row %d: req_primary_fields must not be NULL (id=%d) — this is #2041/#2044", i, idVal)
			require.Equalf(t, fmt.Sprintf("field_%d", idVal), field.RawValue().(string),
				"row %d: req_primary_fields value must round-trip through sort spill for id=%d", i, idVal)
		}
	})
}
