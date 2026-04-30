/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInsertThenUpdateSameRowInOneExec pins the regression reported in
// codenotary/immudb#2092: an INSERT followed by an UPDATE on the same row,
// sent as a single Engine.Exec call (multi-statement SQL, autocommit), trips
// store.ErrCannotUpdateKeyTransiency.
//
// The transiency-collision class was supposed to be closed by 35bb7962
// ("disjoint keyRef spaces for transient vs non-transient tx entries"), but
// that fix only covered the indexer-walk in OngoingTx.set. The SQL engine
// also issues a transient write per non-PK index inside doUpsert, and on the
// second statement of the same tx the store-level transiency check still
// fires because the secondary-index transient keyRef from the INSERT remains
// in transientEntries while the engine reissues a Set on the primary row.
//
// Both variants below must pass: with and without a UNIQUE composite index.
func TestInsertThenUpdateSameRowInOneExec_PlainTable(t *testing.T) {
	engine := setupCommonTest(t)

	ctx := context.Background()

	_, _, err := engine.Exec(ctx, nil,
		"CREATE TABLE t2 (id INTEGER AUTO_INCREMENT, k1 VARCHAR[64], v VARCHAR[64], PRIMARY KEY id);",
		nil)
	require.NoError(t, err)

	// INSERT then UPDATE same row, in a single Exec call. UPDATE WHERE k1
	// matches the row just inserted; the column being updated is non-indexed.
	_, _, err = engine.Exec(ctx, nil,
		"INSERT INTO t2 (k1, v) VALUES ('a', 'orig'); UPDATE t2 SET v = 'upd' WHERE k1 = 'a';",
		nil)
	require.NoError(t, err,
		"INSERT + UPDATE on the same row in one Exec call must commit cleanly — see #2092")
}

func TestInsertThenUpdateSameRowInOneExec_UniqueCompositeIndex(t *testing.T) {
	engine := setupCommonTest(t)

	ctx := context.Background()

	_, _, err := engine.Exec(ctx, nil, `
		CREATE TABLE t (
			id INTEGER AUTO_INCREMENT,
			k1 VARCHAR[64],
			k2 VARCHAR[64],
			v  VARCHAR[64],
			PRIMARY KEY id
		);
		CREATE UNIQUE INDEX ON t(k1, k2);
	`, nil)
	require.NoError(t, err)

	// Same shape as above but with a UNIQUE composite index in play, which
	// is the original Gitea-style reproduction surface.
	_, _, err = engine.Exec(ctx, nil,
		"INSERT INTO t (k1, k2, v) VALUES ('a', 'b', 'orig'); UPDATE t SET v = 'upd' WHERE k1 = 'a' AND k2 = 'b';",
		nil)
	require.NoError(t, err,
		"INSERT + UPDATE on the same row in one Exec call (UNIQUE composite index variant) must commit cleanly — see #2092")
}

// Control: the same SQL split across two Exec calls (i.e. autocommit per
// statement) must keep working. This is the path that already passed before
// the fix — used to bracket the regression.
func TestInsertThenUpdateSameRow_SeparateExecCalls(t *testing.T) {
	engine := setupCommonTest(t)

	ctx := context.Background()

	_, _, err := engine.Exec(ctx, nil,
		"CREATE TABLE t2 (id INTEGER AUTO_INCREMENT, k1 VARCHAR[64], v VARCHAR[64], PRIMARY KEY id);",
		nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(ctx, nil,
		"INSERT INTO t2 (k1, v) VALUES ('a', 'orig');", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(ctx, nil,
		"UPDATE t2 SET v = 'upd' WHERE k1 = 'a';", nil)
	require.NoError(t, err)
}

// TestInsertThenUpdateSameRowInOneExec_ServerStylePrebuiltTx mimics the gRPC
// server path: NewTx is called explicitly first (the way pkg/server/sql.go
// SQLExec does it via db.NewSQLTx), and the resulting non-nil SQLTx is
// passed into Exec. The server path is what surfaces #2092 in production
// (`immuclient exec "INSERT...; UPDATE same row..."`); the simpler
// engine.Exec(ctx, nil, ...) path used by the other tests in this file
// internally calls e.NewTx(ctx, opts) the FIRST time it sees a nil tx,
// using the same DefaultTxOptions, so on paper the two paths should be
// equivalent — but in practice the server path errors and we need to pin
// the difference, then fix it.
func TestInsertThenUpdateSameRowInOneExec_ServerStylePrebuiltTx(t *testing.T) {
	engine := setupCommonTest(t)

	ctx := context.Background()

	_, _, err := engine.Exec(ctx, nil,
		"CREATE TABLE t2 (id INTEGER AUTO_INCREMENT, k1 VARCHAR[64], v VARCHAR[64], PRIMARY KEY id);",
		nil)
	require.NoError(t, err)

	// Server-style: explicit NewTx, then Exec with that tx.
	tx, err := engine.NewTx(ctx, DefaultTxOptions())
	require.NoError(t, err)

	_, _, err = engine.Exec(ctx, tx,
		"INSERT INTO t2 (k1, v) VALUES ('a', 'orig'); UPDATE t2 SET v = 'upd' WHERE k1 = 'a';",
		nil)
	require.NoError(t, err,
		"INSERT + UPDATE on the same row in one Exec call with a server-style prebuilt SQLTx must commit cleanly — see #2092")
}
