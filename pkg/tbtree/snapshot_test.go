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
	tbtree, err := Open("tbtree.idb", DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)
	defer os.Remove("tbtree.idb")

	keyCount := 100_000
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)

	dumpBuf := new(bytes.Buffer)
	wopts := &WriteOpts{
		OnlyMutated: true,
		BaseOffset:  0,
		CommitLog:   false,
	}
	_, err = snapshot.WriteTo(dumpBuf, wopts)
	assert.NoError(t, err)
	assert.True(t, dumpBuf.Len() > 0)

	err = snapshot.Close()
	assert.NoError(t, err)

	err = tbtree.Close()
	assert.NoError(t, err)

	tbtree, err = Open("tbtree.idb", DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	assert.Equal(t, tbtree.root.ts(), uint64(keyCount))

	snapshot, err = tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)

	rspec := &ReaderSpec{
		initialKey: nil,
		isPrefix:   false,
		ascOrder:   true,
	}
	reader, err := snapshot.Reader(rspec)
	assert.NoError(t, err)

	i := 0
	prevk := reader.initialKey
	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(prevk, k) < 1)
		prevk = k
		i++
	}
	assert.Equal(t, keyCount, i)

	reader.Close()

	snapshot.Close()
}
