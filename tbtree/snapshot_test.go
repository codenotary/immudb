/*
Copyright 2019-2020 vChain, Inc.

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
	insertionCountThld := 100_000

	tbtree, err := Open("test_tree_w", DefaultOptions().
		WithMaxNodeSize(MinNodeSize).
		WithFlushThld(insertionCountThld))

	require.NoError(t, err)
	defer os.RemoveAll("test_tree_w")

	keyCount := insertionCountThld
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)

	dumpBuf := new(bytes.Buffer)
	wopts := &WriteOpts{
		OnlyMutated: true,
		BaseOffset:  0,
	}
	_, _, err = snapshot.WriteTo(dumpBuf, wopts)
	require.NoError(t, err)
	require.True(t, dumpBuf.Len() == 0)

	_, _, err = snapshot.Get(nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = snapshot.GetTs(nil, 1)
	require.Error(t, ErrIllegalArguments, err)

	_, err = snapshot.GetTs([]byte{}, 0)
	require.Error(t, ErrIllegalArguments, err)

	err = snapshot.Close()
	require.NoError(t, err)

	_, err = tbtree.Flush()
	require.NoError(t, err)

	snapshot, err = tbtree.Snapshot()
	require.NoError(t, err)

	fulldumpBuf := new(bytes.Buffer)
	wopts = &WriteOpts{
		OnlyMutated: false,
		BaseOffset:  0,
	}
	_, _, err = snapshot.WriteTo(fulldumpBuf, wopts)
	require.NoError(t, err)
	require.True(t, fulldumpBuf.Len() > 0)

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

	_, err = snapshot.Reader(nil)
	require.Error(t, ErrIllegalArguments, err)

	err = snapshot.Close()
	require.NoError(t, err)

	err = snapshot.Close()
	require.Error(t, ErrAlreadyClosed, err)

	_, _, err = snapshot.Get([]byte{})
	require.Error(t, ErrAlreadyClosed, err)

	_, err = snapshot.GetTs([]byte{}, 1)
	require.Error(t, ErrAlreadyClosed, err)

	_, err = snapshot.Reader(nil)
	require.Error(t, ErrAlreadyClosed, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotLoadFromFullDump(t *testing.T) {
	tbtree, err := Open("test_tree_r", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_r")

	keyCount := 10_000
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	err = tbtree.DumpTo("test_tree_dump", false, DefaultFileSize, DefaultFileMode)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_dump")

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_dump", DefaultOptions())
	require.NoError(t, err)

	checkAfterMonotonicInsertions(t, tbtree, 1, keyCount, true)

	err = tbtree.Close()
	require.NoError(t, err)
}
