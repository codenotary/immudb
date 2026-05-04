/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/
*/

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCountStarIndexedWhere is the regression guard for issue #2093:
// `SELECT COUNT(*) … WHERE …` over an index that covers every WHERE
// column must take the index-only fast path (no row-payload decode) and
// return the same result as the slow path.
func TestCountStarIndexedWhere(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string, params map[string]interface{}) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, params)
		require.NoError(t, err, stmt)
	}

	mustExec(`CREATE TABLE logs (
		id INTEGER NOT NULL AUTO_INCREMENT,
		entity_name VARCHAR[255],
		action VARCHAR[50],
		partner_id VARCHAR[64],
		created_at INTEGER,
		PRIMARY KEY (id)
	);`, nil)
	mustExec(`CREATE INDEX ON logs(partner_id, created_at, action);`, nil)

	// Insert enough rows to span multiple partner / action / time buckets.
	// Layout (3 partners × 2 actions × 50 timestamps = 300 rows):
	//   partner_id ∈ {p0, p1, p2}
	//   action     ∈ {scan, view}
	//   created_at ∈ [1000, 1050)
	for partner := 0; partner < 3; partner++ {
		for _, act := range []string{"scan", "view"} {
			for ts := 1000; ts < 1050; ts++ {
				mustExec(
					`INSERT INTO logs (entity_name, action, partner_id, created_at) VALUES (@name, @act, @pid, @ts);`,
					map[string]interface{}{
						"name": fmt.Sprintf("e-%d-%d", partner, ts),
						"act":  act,
						"pid":  fmt.Sprintf("p%d", partner),
						"ts":   ts,
					})
			}
		}
	}

	t.Run("indexed WHERE — fast path matches slow path", func(t *testing.T) {
		// Same WHERE shape the issue uses: equality on leading column,
		// range on middle column, equality on trailing column. All three
		// columns are part of the composite index → eligible for the
		// index-only count.
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE partner_id = 'p1' AND created_at >= 1010 AND created_at < 1030 AND action = 'scan';`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 20, rows[0].ValuesByPosition[0].RawValue())

		// Cross-check against the materialising slow path by issuing a
		// shaped-equivalent SELECT and counting the returned rows.
		ref, err := engine.queryAll(ctx, nil,
			`SELECT id FROM logs WHERE partner_id = 'p1' AND created_at >= 1010 AND created_at < 1030 AND action = 'scan';`,
			nil)
		require.NoError(t, err)
		require.Len(t, ref, 20)
	})

	t.Run("indexed WHERE — equality-only", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE partner_id = 'p2' AND action = 'view';`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 50, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("indexed WHERE — zero matches", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE partner_id = 'no-such-partner' AND created_at >= 0;`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 0, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("indexed WHERE — parameterised", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE partner_id = @pid AND created_at >= @lo AND created_at < @hi;`,
			map[string]interface{}{"pid": "p0", "lo": 1000, "hi": 1010})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		// 10 timestamps × 2 actions
		require.EqualValues(t, 20, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("non-indexed WHERE column — falls back to slow path", func(t *testing.T) {
		// entity_name is not part of any non-PK index here; the planner
		// must take the existing materialising path. The result must
		// still be correct — just not via the new fast path.
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE entity_name = 'e-0-1005';`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		// Inserted once per action: 2 rows.
		require.EqualValues(t, 2, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("bare COUNT(*) still works", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil, `SELECT COUNT(*) FROM logs;`, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 300, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("GROUP BY still uses standard aggregation", func(t *testing.T) {
		// A regression guard: the index-only path must not be selected
		// when GROUP BY is present, because per-group counting needs
		// the grouped-aggregator pipeline.
		rows, err := engine.queryAll(ctx, nil,
			`SELECT partner_id, COUNT(*) FROM logs WHERE partner_id = 'p0' GROUP BY partner_id;`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 100, rows[1-1].ValuesByPosition[1].RawValue())
	})

	t.Run("PK-covered WHERE on primary index", func(t *testing.T) {
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE id >= 10 AND id < 20;`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 10, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("IS NULL on indexed column", func(t *testing.T) {
		// Insert one row with NULL action to verify NULL handling in the
		// key-only path.
		mustExec(
			`INSERT INTO logs (entity_name, action, partner_id, created_at) VALUES ('null-action', NULL, 'p9', 9999);`,
			nil)
		rows, err := engine.queryAll(ctx, nil,
			`SELECT COUNT(*) FROM logs WHERE partner_id = 'p9' AND action IS NULL;`,
			nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 1, rows[0].ValuesByPosition[0].RawValue())
	})
}
