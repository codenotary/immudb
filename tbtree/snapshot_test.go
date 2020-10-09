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

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSerialization(t *testing.T) {
	insertionCountThld := 100_000

	tbtree, err := Open("testree", DefaultOptions().
		SetMaxNodeSize(MinNodeSize).
		SetFlushThld(insertionCountThld))

	require.NoError(t, err)
	defer os.RemoveAll("testree")

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
	_, err = snapshot.WriteTo(dumpBuf, wopts)
	require.NoError(t, err)
	require.True(t, dumpBuf.Len() == 0)

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
	_, err = snapshot.WriteTo(fulldumpBuf, wopts)
	require.NoError(t, err)
	require.True(t, fulldumpBuf.Len() > 0)

	err = snapshot.Close()
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestSnapshotLoadFromFullDump(t *testing.T) {
	tbtree, err := Open("testree", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("testree")

	keyCount := 100_000
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	err = fullDumpTo(tbtree, "dump")
	require.NoError(t, err)
	defer os.RemoveAll("dump")

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("dump", DefaultOptions())
	require.NoError(t, err)

	checkAfterMonotonicInsertions(t, tbtree, 1, keyCount, true)

	err = tbtree.Close()
	require.NoError(t, err)
}

func fullDumpTo(tbtree *TBtree, path string) error {
	opts := DefaultOptions()

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, opts.maxNodeSize)
	metadata.PutInt(MetaFileSize, opts.fileSize)

	appendableOpts := multiapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileSize(opts.fileSize).
		SetFileMode(opts.fileMode).
		SetFileExt("idb").
		SetMetadata(metadata.Bytes())

	multiapp, err := multiapp.Open(path, appendableOpts)
	if err != nil {
		return err
	}

	wopts := &WriteOpts{
		OnlyMutated: false,
		BaseOffset:  0,
	}

	snapshot, err := tbtree.Snapshot()
	if err != nil {
		return err
	}

	_, err = snapshot.WriteTo(&appendableWriter{multiapp}, wopts)
	if err != nil {
		return err
	}

	err = snapshot.Close()
	if err != nil {
		return err
	}

	return multiapp.Close()
}
