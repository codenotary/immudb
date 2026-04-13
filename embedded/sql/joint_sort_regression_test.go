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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJointGroupOrderSpill reproduces the regression where
// `SELECT ... FROM big JOIN small ON ... GROUP BY ... ORDER BY ... LIMIT N`
// failed with `write /tmp/immudb<rand>: file already closed` once the
// sort buffer overflowed and the merge phase needed disk-backed temp files.
//
// Root cause: jointRowReader.Read closed nested rawRowReaders mid-iteration
// (joint_row_reader.go:198), each of which fired the engine's onClose
// callback that called sqlTx.Cancel(); the deferred removeTempFiles then
// closed the *os.File the still-running fileSorter merge was writing to.
//
// Fix: temp-file ownership moved from SQLTx to fileSorter, with Close()
// propagated through the resultReader chain (file_sort.go, sort_reader.go).
// This test asserts the bug stays fixed and that no temp file leaks.
func TestJointGroupOrderSpill(t *testing.T) {
	engine := setupCommonTest(t)
	// Force the sorter to spill to disk on a tiny dataset.
	engine.sortBufferSize = 4

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(context.Background(), nil, stmt, nil)
		require.NoError(t, err, stmt)
	}

	mustExec(`CREATE TABLE genre (id INTEGER NOT NULL, name VARCHAR(32), PRIMARY KEY(id));`)
	mustExec(`CREATE TABLE track (id INTEGER NOT NULL, genre_id INTEGER, PRIMARY KEY(id));`)

	for i := 1; i <= 6; i++ {
		mustExec(fmt.Sprintf(`INSERT INTO genre (id, name) VALUES (%d, 'g%d');`, i, i))
	}
	// 60 tracks distributed across 6 genres — well above sortBufferSize=4
	// so mergeAllChunks must allocate temp files.
	for i := 1; i <= 60; i++ {
		mustExec(fmt.Sprintf(`INSERT INTO track (id, genre_id) VALUES (%d, %d);`, i, ((i-1)%6)+1))
	}

	// Snapshot temp dir so we can detect leaks.
	tempDirBefore := countImmudbTempFiles(t)

	rows, err := engine.queryAll(
		context.Background(),
		nil,
		`SELECT g.name, COUNT(*) AS n
		 FROM track t JOIN genre g ON t.genre_id = g.id
		 GROUP BY g.name
		 ORDER BY n DESC
		 LIMIT 5;`,
		nil,
	)
	require.NoError(t, err, "JOIN+GROUP+ORDER must not regress to file-already-closed")
	require.Len(t, rows, 5)
	for _, r := range rows {
		// 60 tracks / 6 genres = 10 tracks each; each row's count must be 10.
		v, ok := r.ValuesByPosition[1].RawValue().(int64)
		require.True(t, ok, "count column should be int64")
		require.EqualValues(t, 10, v)
	}

	// Defensive: queryAll closes the reader; temp files must be gone.
	tempDirAfter := countImmudbTempFiles(t)
	require.Equal(t, tempDirBefore, tempDirAfter,
		"fileSorter.Close should remove every temp file it opened")
}

// TestFileSorterCloseRemovesTempFiles exercises the new Close() chain
// directly via a sortRowReader, independent of the engine plumbing.
func TestFileSorterCloseRemovesTempFiles(t *testing.T) {
	engine := setupCommonTest(t)
	engine.sortBufferSize = 4

	mustExec := func(stmt string) {
		_, _, err := engine.Exec(context.Background(), nil, stmt, nil)
		require.NoError(t, err)
	}
	mustExec(`CREATE TABLE nums (id INTEGER NOT NULL, n INTEGER, PRIMARY KEY(id));`)
	for i := 1; i <= 20; i++ {
		mustExec(fmt.Sprintf(`INSERT INTO nums (id, n) VALUES (%d, %d);`, i, 21-i))
	}

	before := countImmudbTempFiles(t)

	reader, err := engine.Query(context.Background(), nil, `SELECT id, n FROM nums ORDER BY n;`, nil)
	require.NoError(t, err)
	rows, err := ReadAllRows(context.Background(), reader)
	require.NoError(t, err)
	require.Len(t, rows, 20)
	require.NoError(t, reader.Close())

	after := countImmudbTempFiles(t)
	require.Equal(t, before, after, "ORDER BY with file-sort spill must not leak temp files")
}

func countImmudbTempFiles(t *testing.T) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "immudb*"))
	require.NoError(t, err)
	return len(matches)
}

// TestSqlTxDeregisterTempFile pins the new helper used by fileSorter.Close
// to keep tx.tempFiles in sync.
func TestSqlTxDeregisterTempFile(t *testing.T) {
	engine := setupCommonTest(t)
	tx, err := engine.NewTx(context.Background(), DefaultTxOptions().WithReadOnly(true))
	require.NoError(t, err)
	defer tx.Cancel()

	f1, err := tx.createTempFile()
	require.NoError(t, err)
	f2, err := tx.createTempFile()
	require.NoError(t, err)
	require.Len(t, tx.tempFiles, 2)

	tx.deregisterTempFile(f1)
	require.Len(t, tx.tempFiles, 1)
	require.Equal(t, f2, tx.tempFiles[0])

	// Cleanup so Cancel doesn't error on a stale handle.
	require.NoError(t, f1.Close())
	require.NoError(t, os.Remove(f1.Name()))

	// Deregistering an unknown file must be a no-op.
	other, err := os.CreateTemp("", "immudb-extern")
	require.NoError(t, err)
	defer os.Remove(other.Name())
	defer other.Close()
	tx.deregisterTempFile(other)
	require.Len(t, tx.tempFiles, 1)

	// Sanity: name format unchanged.
	require.True(t, strings.Contains(filepath.Base(f2.Name()), "immudb"))
}
