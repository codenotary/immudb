/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tbtree

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotSerialization(t *testing.T) {
	insertionCountThld := 10_000

	tbtree, err := Open("test_tree_w", DefaultOptions().
		WithFlushThld(insertionCountThld))

	require.NoError(t, err)
	defer os.RemoveAll("test_tree_w")

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
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = snapshot.History(nil, 0, false, 1)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = snapshot.History([]byte{}, 0, false, 0)
	require.Equal(t, ErrIllegalArguments, err)

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
	tbtree, err := Open("test_tree_closing", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_closing")

	snapshot, err := tbtree.Snapshot()
	require.NoError(t, err)

	_, err = snapshot.NewReader(nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = snapshot.Close()
	require.NoError(t, err)

	err = snapshot.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	_, _, _, err = snapshot.Get([]byte{})
	require.Equal(t, ErrAlreadyClosed, err)

	_, _, err = snapshot.History([]byte{}, 0, false, 1)
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = snapshot.ExistKeyWith([]byte{}, nil)
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = snapshot.NewReader(nil)
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = snapshot.NewHistoryReader(nil)
	require.Equal(t, ErrAlreadyClosed, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotLoadFromFullDump(t *testing.T) {
	tbtree, err := Open("test_tree_r", DefaultOptions().WithCompactionThld(1).WithDelayDuringCompaction(1))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_r")

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
	tbtree, err := Open("test_tree_snap_isolation", DefaultOptions().WithCompactionThld(1).WithDelayDuringCompaction(1))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_snap_isolation")

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

		exists, err := snap1.ExistKeyWith([]byte("key"), nil)
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = snap1.ExistKeyWith([]byte("key3"), []byte("key3"))
		require.NoError(t, err)
		require.False(t, exists)
	})

	err = tbtree.Insert([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	t.Run("keys inserted after snapshot creation should NOT be reachable", func(t *testing.T) {
		_, _, _, err = snap1.Get([]byte("key2"))
		require.Equal(t, ErrKeyNotFound, err)

		_, _, _, err = snap2.Get([]byte("key2"))
		require.Equal(t, ErrKeyNotFound, err)
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
	require.Equal(t, ErrKeyNotFound, err)

	_, _, _, err = snap2.Get([]byte("key1_snap1"))
	require.Equal(t, ErrKeyNotFound, err)

	_, _, _, err = tbtree.Get([]byte("key1_snap1"))
	require.Equal(t, ErrKeyNotFound, err)

	_, _, _, err = tbtree.Get([]byte("key1_snap2"))
	require.Equal(t, ErrKeyNotFound, err)

	err = snap1.Close()
	require.NoError(t, err)

	err = snap2.Close()
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}
