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

package transactions

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

func TestNewTx(t *testing.T) {
	path := t.TempDir()

	db, err := database.NewDB("db1", nil, database.DefaultOptions().WithDBRootPath(path), logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	_, err = NewTransaction(context.Background(), nil, db, "session1")
	require.ErrorIs(t, err, sql.ErrIllegalArguments)

	tx, err := NewTransaction(context.Background(), sql.DefaultTxOptions(), db, "session1")
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Rollback()
	require.NoError(t, err)

	_, err = tx.SQLQuery(context.Background(), nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.SQLExec(context.Background(), nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.Rollback()
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	_, err = tx.Commit(context.Background())
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)
}

// A statement that fails before reaching the engine (a parse error, for example)
// must leave the ongoing transaction untouched: the engine never got a chance to
// cancel it, so dropping the reference here would orphan it together with the
// snapshots it holds. See issue #2127.
func TestSQLExecPreExecutionErrorKeepsTxOpen(t *testing.T) {
	db, err := database.NewDB("db1", nil, database.DefaultOptions().WithDBRootPath(t.TempDir()), logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	tx, err := NewTransaction(context.Background(), sql.DefaultTxOptions(), db, "session1")
	require.NoError(t, err)

	err = tx.SQLExec(context.Background(), &schema.SQLExecRequest{Sql: "THIS IS NOT SQL"})
	require.Error(t, err)

	require.False(t, tx.IsClosed(), "a parse error must not close the transaction")

	// the client can still roll back, which is what releases the snapshots
	require.NoError(t, tx.Rollback())
	require.True(t, tx.IsClosed())
}

// An execution-stage error is cancelled by the engine itself, so the transaction
// is expected to end up closed. Pinned here so the fix for #2127 does not quietly
// change this path.
func TestSQLExecExecutionErrorClosesTx(t *testing.T) {
	db, err := database.NewDB("db1", nil, database.DefaultOptions().WithDBRootPath(t.TempDir()), logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	tx, err := NewTransaction(context.Background(), sql.DefaultTxOptions(), db, "session1")
	require.NoError(t, err)

	err = tx.SQLExec(context.Background(), &schema.SQLExecRequest{Sql: "INSERT INTO nonexistent(id) VALUES (1);"})
	require.Error(t, err)

	require.True(t, tx.IsClosed(), "the engine cancels the tx on execution errors")
	require.ErrorIs(t, tx.Rollback(), sql.ErrNoOngoingTx)
}

// Repeated pre-execution failures used to leak one tbtree snapshot each, so after
// maxActiveSnapshots of them no further transaction could be opened until the
// server was restarted. See issue #2127.
func TestSQLExecPreExecutionErrorDoesNotLeakSnapshots(t *testing.T) {
	const maxActiveSnapshots = 4

	storeOpts := store.DefaultOptions().
		WithIndexOptions(store.DefaultIndexOptions().WithMaxActiveSnapshots(maxActiveSnapshots))

	db, err := database.NewDB("db1", nil,
		database.DefaultOptions().WithDBRootPath(t.TempDir()).WithStoreOptions(storeOpts),
		logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	// mirrors the reported production cycle: the client submits an invalid
	// statement, attempts a rollback, and carries on regardless of its outcome
	for i := 0; i < maxActiveSnapshots*2; i++ {
		tx, err := NewTransaction(context.Background(), sql.DefaultTxOptions(), db, "session1")
		require.NoErrorf(t, err, "opening a transaction failed on iteration %d: snapshots are leaking", i)

		require.Error(t, tx.SQLExec(context.Background(), &schema.SQLExecRequest{Sql: "THIS IS NOT SQL"}))

		_ = tx.Rollback()
	}
}
