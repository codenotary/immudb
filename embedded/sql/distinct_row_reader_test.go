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
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestDistinctRowReader(t *testing.T) {
	dummyr := &dummyRowReader{failReturningColumns: false}

	dummyr.failReturningColumns = true
	_, err := newDistinctRowReader(context.Background(), dummyr)
	require.ErrorIs(t, err, errDummy)

	dummyr.failReturningColumns = false

	rowReader, err := newDistinctRowReader(context.Background(), dummyr)
	require.NoError(t, err)
	require.Equal(t, dummyr.TableAlias(), rowReader.TableAlias())
	require.Equal(t, dummyr.OrderBy(), rowReader.OrderBy())
	require.Equal(t, dummyr.ScanSpecs(), rowReader.ScanSpecs())

	require.Nil(t, rowReader.Tx())

	_, err = rowReader.colsBySelector(context.Background())
	require.ErrorIs(t, err, errDummy)

	dummyr.failReturningColumns = true
	_, err = rowReader.Columns(context.Background())
	require.ErrorIs(t, err, errDummy)

	require.Nil(t, rowReader.Parameters())

	err = rowReader.InferParameters(context.Background(), nil)
	require.NoError(t, err)

	dummyr.failInferringParams = true

	err = rowReader.InferParameters(context.Background(), nil)
	require.ErrorIs(t, err, errDummy)
}

// BenchmarkDistinctSpill exercises the D5 spill-to-disk path on a 200k
// distinct-row population (smaller than the doc's 10M target so the
// bench finishes in seconds — but still 1000+ spill flushes at the
// 100-row threshold, which is enough to stress the merge path). The
// benchmark variant is the gate that perf-delta tracks; the
// correctness assertion is over in TestDistinctSpill.
//
// Each iteration runs SELECT DISTINCT k FROM t over a single-table scan
// with k = i % distinctK; the threshold of 100 forces ~2000 spill
// flushes per iteration, exercising both the in-mem fast path and the
// on-disk binary search. Any regression in spill performance shows up
// here as latency or alloc growth.
func BenchmarkDistinctSpill(b *testing.B) {
	st, err := store.Open(b.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(b, err)
	b.Cleanup(func() { _ = st.Close() })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithDistinctSpillThreshold(100))
	require.NoError(b, err)

	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE TABLE t(id INTEGER, k INTEGER, PRIMARY KEY id);", nil)
	require.NoError(b, err)

	const (
		distinctK = 200_000
		nRows     = 200_000
	)
	const perTx = 1000
	for off := 0; off < nRows; off += perTx {
		tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(b, err)
		end := off + perTx
		if end > nRows {
			end = nRows
		}
		for i := off; i < end; i++ {
			_, _, err := engine.Exec(context.Background(), tx,
				fmt.Sprintf("UPSERT INTO t(id, k) VALUES (%d, %d);", i, i%distinctK), nil)
			require.NoError(b, err)
		}
		_, _, err = engine.Exec(context.Background(), tx, "COMMIT;", nil)
		require.NoError(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := engine.Query(context.Background(), nil, "SELECT DISTINCT k FROM t", nil)
		if err != nil {
			b.Fatal(err)
		}
		seen := 0
		for {
			_, err := reader.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			if err != nil {
				reader.Close()
				b.Fatal(err)
			}
			seen++
		}
		reader.Close()
		if seen != distinctK {
			b.Fatalf("expected %d distinct rows, got %d", distinctK, seen)
		}
	}
}

// TestDistinctSpillTempFileLifecycle (D5 follow-up) verifies that the
// merge-replaces-previous-spill path correctly deregisters the old
// spill from the SQLTx tempFiles list (so each flush keeps only ONE
// spill file registered), and that tx Commit/Cancel removes the final
// spill from disk.
func TestDistinctSpillTempFileLifecycle(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	// Threshold of 8 forces ~12 spill flushes over a 100-key population.
	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithDistinctSpillThreshold(8))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE TABLE t(id INTEGER, k INTEGER, PRIMARY KEY id);", nil)
	require.NoError(t, err)

	// Populate.
	tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, _, err := engine.Exec(context.Background(), tx,
			fmt.Sprintf("UPSERT INTO t(id, k) VALUES (%d, %d);", i, i), nil)
		require.NoError(t, err)
	}
	_, _, err = engine.Exec(context.Background(), tx, "COMMIT;", nil)
	require.NoError(t, err)

	// Run DISTINCT inside an explicit-close tx so we can inspect
	// tempFiles before final cleanup.
	queryTx, err := engine.NewTx(context.Background(),
		DefaultTxOptions().WithReadOnly(true).WithExplicitClose(true))
	require.NoError(t, err)

	stmts, err := ParseSQL(strings.NewReader("SELECT DISTINCT k FROM t"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	dsStmt := stmts[0].(DataSource)

	reader, err := engine.QueryPreparedStmt(context.Background(), queryTx, dsStmt, nil)
	require.NoError(t, err)

	seen := 0
	for {
		_, err := reader.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		seen++
	}
	require.Equal(t, 100, seen)
	require.NoError(t, reader.Close())

	// Flush-replace-merge invariant: after many flushes the queryTx
	// should hold AT MOST one tempFile (the current spill).
	require.LessOrEqual(t, len(queryTx.tempFiles), 1,
		"flushToSpill should deregister the previous spill on each merge")

	// Capture path so we can assert the file is gone after Cancel.
	var spillPath string
	if len(queryTx.tempFiles) == 1 {
		spillPath = queryTx.tempFiles[0].Name()
	}

	require.NoError(t, queryTx.Cancel())

	if spillPath != "" {
		_, err := os.Stat(spillPath)
		require.True(t, os.IsNotExist(err),
			"tx Cancel should have removed the spill file at %s", spillPath)
	}
}

// TestDistinctSpill verifies D5: with DistinctSpillThreshold > 0 and an
// input that exceeds the threshold many times over, DISTINCT yields the
// correct unique row count without OOM and without ErrTooManyRows.
func TestDistinctSpill(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	// Threshold of 32 forces multiple spill flushes for a 200-row population
	// repeated 5x — exercises the merge path several times.
	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithDistinctSpillThreshold(32))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE TABLE t(id INTEGER, k INTEGER, PRIMARY KEY id);", nil)
	require.NoError(t, err)

	const distinctK = 200
	const dupes = 5
	for d := 0; d < dupes; d++ {
		// Each insert tx stays under DefaultMaxTxEntries.
		tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)
		for i := 0; i < distinctK; i++ {
			_, _, err := engine.Exec(context.Background(), tx,
				fmt.Sprintf("UPSERT INTO t(id, k) VALUES (%d, %d);", d*distinctK+i, i), nil)
			require.NoError(t, err)
		}
		_, _, err = engine.Exec(context.Background(), tx, "COMMIT;", nil)
		require.NoError(t, err)
	}

	reader, err := engine.Query(context.Background(), nil, "SELECT DISTINCT k FROM t", nil)
	require.NoError(t, err)
	defer reader.Close()

	seen := 0
	for {
		_, err := reader.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		seen++
	}

	require.Equal(t, distinctK, seen)
}
