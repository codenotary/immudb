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

	"github.com/stretchr/testify/assert"
)

func TestSnapshotSerialization(t *testing.T) {
	insertionCountThreshold := 100_000

	tbtree, err := Open("tbtree.idb", DefaultOptions().
		SetMaxNodeSize(MinNodeSize).
		SetInsertionCountThreshold(insertionCountThreshold))

	assert.NoError(t, err)
	defer os.Remove("tbtree.idb")

	keyCount := insertionCountThreshold
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)

	dumpBuf := new(bytes.Buffer)
	wopts := &WriteOpts{
		OnlyMutated: true,
		BaseOffset:  0,
	}
	_, err = snapshot.WriteTo(dumpBuf, wopts)
	assert.NoError(t, err)
	assert.True(t, dumpBuf.Len() == 0)

	err = snapshot.Close()
	assert.NoError(t, err)

	_, err = tbtree.Flush()
	assert.NoError(t, err)

	snapshot, err = tbtree.Snapshot()
	assert.NoError(t, err)

	fulldumpBuf := new(bytes.Buffer)
	wopts = &WriteOpts{
		OnlyMutated: false,
		BaseOffset:  0,
	}
	_, err = snapshot.WriteTo(fulldumpBuf, wopts)
	assert.NoError(t, err)
	assert.True(t, fulldumpBuf.Len() > 0)

	err = snapshot.Close()
	assert.NoError(t, err)

	err = tbtree.Close()
	assert.NoError(t, err)
}

func TestSnapshotLoadFromFullDump(t *testing.T) {
	tbtree, err := Open("tbtree.idb", DefaultOptions().SetMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)
	defer os.Remove("tbtree.idb")

	keyCount := 100_000
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	err = fullDumpTo(tbtree, "dumped_tbtree.idb")
	assert.NoError(t, err)
	defer os.Remove("dumped_tbtree.idb")

	err = tbtree.Close()
	assert.NoError(t, err)

	tbtree, err = Open("dumped_tbtree.idb", DefaultOptions().SetMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	err = fullDumpTo(tbtree, "dumped_tbtree1.idb")
	assert.NoError(t, err)
	defer os.Remove("dumped_tbtree1.idb")

	dumpInfo1, err := os.Stat("dumped_tbtree.idb")
	assert.NoError(t, err)
	dumpInfo2, err := os.Stat("dumped_tbtree1.idb")
	assert.NoError(t, err)

	assert.Equal(t, dumpInfo1.Size(), dumpInfo2.Size())

	err = tbtree.Close()
	assert.NoError(t, err)
}

func fullDumpTo(tbtree *TBtree, filename string) error {
	dumpFile, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
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

	_, err = snapshot.WriteTo(dumpFile, wopts)
	if err != nil {
		return err
	}

	err = snapshot.Close()
	if err != nil {
		return err
	}

	return dumpFile.Close()
}
