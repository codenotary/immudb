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
