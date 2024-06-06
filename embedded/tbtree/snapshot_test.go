/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package tbtree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotSerialization(t *testing.T) {
	insertionCountThld := 10_000

	tbtree, err := Open(t.TempDir(), DefaultOptions().WithFlushThld(insertionCountThld))
	require.NoError(t, err)

	keyCount := insertionCountThld
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)

	_, _, _, _, err = snapshot.WriteTo(nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	dumpNBuf := new(bytes.Buffer)
	dumpHBuf := new(bytes.Buffer)
	wopts := &WriteOpts{
		OnlyMutated:    true,
		BaseNLogOffset: 0,
		BaseHLogOffset: 0,
		reportProgress: func(innerWritten, leafNodesWritten, keysWritten int) {},
	}
	_, _, _, _, err = snapshot.WriteTo(dumpNBuf, dumpHBuf, wopts)
	require.NoError(t, err)
	require.True(t, dumpNBuf.Len() == 0)

	_, _, _, err = snapshot.Get(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, _, err = snapshot.GetBetween(nil, 1, 2)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = snapshot.History(nil, 0, false, 1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = snapshot.History([]byte{}, 0, false, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = snapshot.Close()
	require.NoError(t, err)

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	snapshot, err = tbtree.Snapshot()
	require.NoError(t, err)

	fulldumpNBuf := new(bytes.Buffer)
	fulldumpHBuf := new(bytes.Buffer)
	wopts = &WriteOpts{
		OnlyMutated:    false,
		BaseNLogOffset: 0,
		BaseHLogOffset: 0,
		reportProgress: func(innerWritten, leafNodesWritten, keysWritten int) {},
	}
	_, _, _, _, err = snapshot.WriteTo(fulldumpNBuf, fulldumpHBuf, wopts)
	require.NoError(t, err)
	require.True(t, fulldumpNBuf.Len() > 0)

	err = snapshot.Close()
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotClosing(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NoError(t, err)

	err = snapshot.Close()
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, _, err = snapshot.Get([]byte{})
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, _, err = snapshot.GetBetween([]byte{}, 1, 1)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, err = snapshot.History([]byte{}, 0, false, 1)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, _, _, err = snapshot.GetWithPrefix([]byte{}, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = snapshot.NewReader(ReaderSpec{})
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = snapshot.NewHistoryReader(nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotLoadFromFullDump(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions().WithCompactionThld(1).WithDelayDuringCompaction(1))
	require.NoError(t, err)

	keyCount := 1_000
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	done := make(chan struct{})

	go func(done chan<- struct{}) {
		tbtree.Compact()
		done <- struct{}{}
	}(done)

	<-done

	checkAfterMonotonicInsertions(t, tbtree, 1, keyCount, true)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotIsolation(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions().WithCompactionThld(1).WithDelayDuringCompaction(1))
	require.NoError(t, err)

	err = tbtree.Insert([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	// snapshot creation
	snap1, err := tbtree.Snapshot()
	require.NoError(t, err)

	snap2, err := tbtree.Snapshot()
	require.NoError(t, err)

	t.Run("keys inserted before snapshot creation should be reachable", func(t *testing.T) {
		_, _, _, err = snap1.Get([]byte("key1"))
		require.NoError(t, err)

		_, _, _, err = snap2.Get([]byte("key1"))
		require.NoError(t, err)

		_, _, ts, _, err := snap1.GetWithPrefix([]byte("key"), nil)
		require.NoError(t, err)
		require.NotZero(t, ts)

		_, _, _, err = snap1.GetBetween([]byte("key1"), 1, snap1.Ts())
		require.NoError(t, err)

		_, _, _, err = snap2.GetBetween([]byte("key1"), 1, snap2.Ts())
		require.NoError(t, err)

		_, _, _, _, err = snap1.GetWithPrefix([]byte("key3"), nil)
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, _, _, _, err = snap1.GetWithPrefix([]byte("key1"), []byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	err = tbtree.Insert([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	t.Run("keys inserted after snapshot creation should NOT be reachable", func(t *testing.T) {
		_, _, _, err = snap1.Get([]byte("key2"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, _, _, err = snap2.Get([]byte("key2"))
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	err = snap1.Set([]byte("key1"), []byte("value1_snap1"))
	require.NoError(t, err)

	err = snap1.Set([]byte("key1_snap1"), []byte("value1_snap1"))
	require.NoError(t, err)

	err = snap2.Set([]byte("key1"), []byte("value1_snap2"))
	require.NoError(t, err)

	err = snap2.Set([]byte("key1_snap2"), []byte("value1_snap2"))
	require.NoError(t, err)

	t.Run("keys inserted after snapshot creation should NOT be reachable", func(t *testing.T) {

	})

	_, _, _, err = snap1.Get([]byte("key1_snap1"))
	require.NoError(t, err)

	_, _, _, err = snap2.Get([]byte("key1_snap2"))
	require.NoError(t, err)

	_, _, _, err = snap1.Get([]byte("key1_snap2"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = snap2.Get([]byte("key1_snap1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = snap1.GetBetween([]byte("key1_snap2"), 1, snap1.Ts())
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = snap2.GetBetween([]byte("key1_snap1"), 1, snap2.Ts())
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = tbtree.Get([]byte("key1_snap1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, _, err = tbtree.Get([]byte("key1_snap2"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = snap1.Close()
	require.NoError(t, err)

	err = snap2.Close()
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}
