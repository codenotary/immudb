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

// TestInSubqueryParamTypeInference pins that InferParameters walks the
// inner WHERE of an `IN (SELECT … WHERE col = $N)` subquery and
// propagates the inner col's type into the params map. Before the fix,
// InSubQueryExp.requiresType only validated the IN-expression was used
// as a BOOLEAN and stopped — leaving $N at the default AnyType. lib/pq
// then bound the param via VARCHAR text format, and the subsequent
// Compare() fired "values are not comparable" at evaluation time.
//
// Gitea's FindRecentlyPushedNewBranches is the canonical repro: an IN
// subquery nests `WHERE is_fork = $N` where $N must carry BOOLEAN so
// the bind comes back as a bool, not a string.
func TestInSubqueryParamTypeInference(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}
	mustExec(`CREATE TABLE repo (id INTEGER NOT NULL, is_fork BOOLEAN, PRIMARY KEY(id));`)
	mustExec(`CREATE TABLE branch (id INTEGER NOT NULL, repo_id INTEGER, PRIMARY KEY(id));`)

	params, err := engine.InferParameters(ctx, nil,
		`SELECT id FROM branch WHERE repo_id IN (SELECT id FROM repo WHERE is_fork = $1)`)
	require.NoError(t, err)
	require.Contains(t, params, "param1")
	require.Equalf(t, BooleanType, params["param1"],
		"IN subquery param type must be propagated from the inner col (BOOLEAN), got %v", params["param1"])
}

// TestInSubqueryComparisonExec is the execution-time companion. Even with
// correct type inference, if the bind value reaches the inner WHERE
// untyped, Compare() errors. This test verifies the query runs end-to-end
// with a bool param bound, no "values are not comparable" error.
func TestInSubqueryComparisonExec(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}
	mustExec(`CREATE TABLE repo (id INTEGER NOT NULL, is_fork BOOLEAN, PRIMARY KEY(id));`)
	mustExec(`CREATE TABLE branch (id INTEGER NOT NULL, repo_id INTEGER, PRIMARY KEY(id));`)
	mustExec(`INSERT INTO repo (id, is_fork) VALUES (1, false), (2, true);`)
	mustExec(`INSERT INTO branch (id, repo_id) VALUES (10, 1), (11, 2);`)

	rows, err := engine.queryAll(ctx, nil,
		`SELECT id FROM branch WHERE repo_id IN (SELECT id FROM repo WHERE is_fork = $1)`,
		map[string]interface{}{"param1": false})
	require.NoError(t, err,
		"exec must not fire 'values are not comparable' on IN subquery with correct bool bind")
	require.Len(t, rows, 1, "only branch 10 (repo_id=1, is_fork=false) should match")
}

// TestInsertActionRunShape is a narrow repro of the Gitea InsertRun
// flow that surfaced "values are not comparable" in the compose env.
// The INSERT itself is straightforward (22 scalar values into
// action_run) — if the error persists, it's inside immudb's INSERT-
// side validation / index-maintenance path and not specific to
// subqueries.
func TestInsertActionRunShape(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}
	mustExec(`CREATE TABLE action_run (
		id INTEGER NOT NULL AUTO_INCREMENT,
		title VARCHAR[256],
		repo_id INTEGER,
		owner_id INTEGER,
		workflow_id VARCHAR[256],
		_index INTEGER,
		trigger_user_id INTEGER,
		schedule_id INTEGER,
		ref VARCHAR[256],
		commit_sha VARCHAR[256],
		is_fork_pull_request BOOLEAN,
		need_approval BOOLEAN,
		approved_by INTEGER,
		event VARCHAR[256],
		event_payload BLOB,
		trigger_event VARCHAR[256],
		status INTEGER,
		version INTEGER,
		started INTEGER,
		stopped INTEGER,
		previous_duration INTEGER,
		created INTEGER,
		updated INTEGER,
		PRIMARY KEY(id)
	);`)

	_, _, err := engine.Exec(ctx, nil,
		`INSERT INTO action_run (title, repo_id, owner_id, workflow_id, _index, trigger_user_id, schedule_id, ref, commit_sha, is_fork_pull_request, need_approval, approved_by, event, event_payload, trigger_event, status, version, started, stopped, previous_duration, created, updated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
		map[string]interface{}{
			"param1":  "Push to main",
			"param2":  int64(1),
			"param3":  int64(1),
			"param4":  ".gitea/workflows/ci.yml",
			"param5":  int64(1),
			"param6":  int64(1),
			"param7":  int64(0),
			"param8":  "refs/heads/main",
			"param9":  "abc123",
			"param10": false,
			"param11": false,
			"param12": int64(0),
			"param13": "push",
			"param14": []byte("{\"ref\":\"refs/heads/main\"}"),
			"param15": "push",
			"param16": int64(1),
			"param17": int64(1),
			"param18": int64(0),
			"param19": int64(0),
			"param20": int64(0),
			"param21": int64(1736000000),
			"param22": int64(1736000000),
		})
	require.NoError(t, err,
		"InsertRun action_run shape must not fire 'values are not comparable' — this is Gitea's handleWorkflows path")
}

// TestSumCaseWhenAggregate pins that `SUM(CASE WHEN … END)` parses and
// evaluates. Gitea's eventsource UIDcounts query shape:
//
//	SELECT user_id, SUM(CASE WHEN status = $1 THEN 1 ELSE 0 END) AS count
//	  FROM notification GROUP BY user_id
//
// Before the Phase-2 fix, immudb's grammar rejected the CASE-inside-SUM
// with "unexpected CASE at position 24" because AGGREGATE_FUNC only
// accepted a col (or DISTINCT col), never a general expression.
func TestSumCaseWhenAggregate(t *testing.T) {
	t.Skip("SUM(CASE WHEN …) requires a new AggExp AST node — tracked as Phase 2 of the Gitea background-errors plan.")
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}
	mustExec(`CREATE TABLE n (id INTEGER NOT NULL, user_id INTEGER, status INTEGER, PRIMARY KEY(id));`)
	mustExec(`INSERT INTO n (id, user_id, status) VALUES (1, 100, 1), (2, 100, 1), (3, 100, 2), (4, 200, 1);`)

	rows, err := engine.queryAll(ctx, nil,
		`SELECT user_id, SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS c FROM n GROUP BY user_id ORDER BY user_id`,
		nil)
	require.NoError(t, err, "SUM(CASE WHEN …) must parse and evaluate")
	require.Len(t, rows, 2)
	require.EqualValues(t, 100, rows[0].ValuesByPosition[0].RawValue())
	require.EqualValues(t, 2, rows[0].ValuesByPosition[1].RawValue())
	require.EqualValues(t, 200, rows[1].ValuesByPosition[0].RawValue())
	require.EqualValues(t, 1, rows[1].ValuesByPosition[1].RawValue())
}
