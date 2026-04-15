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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCountStarOverInnerJoin guards that `SELECT count(*) FROM a JOIN b …`
// returns cleanly without the "already closed" error. Gitea's
// GetIssueStats executes this exact pattern and it blocks the issues
// page from rendering.
func TestCountStarOverInnerJoin(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()
	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}
	mustExec(`CREATE TABLE ja (id INTEGER NOT NULL, name VARCHAR, PRIMARY KEY(id));`)
	mustExec(`CREATE TABLE jb (id INTEGER NOT NULL, a_id INTEGER, PRIMARY KEY(id));`)

	// Empty tables
	rows, err := engine.queryAll(ctx, nil,
		`SELECT count(*) FROM ja INNER JOIN jb ON ja.id = jb.a_id;`,
		nil)
	require.NoError(t, err, "empty COUNT(*) over INNER JOIN must not error — this is the Gitea GetIssueStats regression guard")
	require.Len(t, rows, 1)

	// With data
	mustExec(`INSERT INTO ja (id, name) VALUES (1, 'a'), (2, 'b');`)
	mustExec(`INSERT INTO jb (id, a_id) VALUES (10, 1), (11, 1), (12, 2);`)
	rows, err = engine.queryAll(ctx, nil,
		`SELECT count(*) FROM ja INNER JOIN jb ON ja.id = jb.a_id;`,
		nil)
	require.NoError(t, err, "COUNT(*) over INNER JOIN with data must not error")
	require.Len(t, rows, 1)
	require.EqualValues(t, 3, rows[0].ValuesByPosition[0].RawValue())
}
