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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReaderForEmptyTreeShouldReturnError(t *testing.T) {
	tbtree, _ := New()

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.Reader(&ReaderSpec{initialKey: []byte{0, 0, 0, 0}, ascOrder: true})
	assert.Equal(t, ErrNoMoreEntries, err)
}

func TestReaderAscendingScan(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		assert.NoError(t, err)
	}()

	rspec := &ReaderSpec{
		initialKey: []byte{0, 0, 0, 250},
		isPrefix:   true,
		ascOrder:   true,
	}
	reader, err := snapshot.Reader(rspec)
	assert.NoError(t, err)

	err = snapshot.Close()
	assert.Equal(t, ErrReadersNotClosed, err)

	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(reader.initialKey, k) < 1)
	}

	err = reader.Close()
	assert.NoError(t, err)

	err = reader.Close()
	assert.Equal(t, ErrAlreadyClosed, err)
}

func TestReaderDescendingScan(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 257, true)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)
	defer snapshot.Close()

	rspec := &ReaderSpec{
		initialKey: []byte{0, 0, 0, 100},
		isPrefix:   false,
		ascOrder:   false,
	}
	reader, err := snapshot.Reader(rspec)
	assert.NoError(t, err)
	defer reader.Close()

	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(k, reader.initialKey) < 1)
	}
}

func TestFullScanAscendingOrder(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions())
	assert.NoError(t, err)

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)
	defer snapshot.Close()

	rspec := &ReaderSpec{
		initialKey: nil,
		isPrefix:   false,
		ascOrder:   true,
	}
	reader, err := snapshot.Reader(rspec)
	assert.NoError(t, err)
	defer reader.Close()

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
}

func TestFullScanDescendingOrder(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions())
	assert.NoError(t, err)

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)
	defer snapshot.Close()

	rspec := &ReaderSpec{
		initialKey: []byte{255, 255, 255, 255},
		isPrefix:   false,
		ascOrder:   false,
	}
	reader, err := snapshot.Reader(rspec)
	assert.NoError(t, err)
	defer reader.Close()

	i := 0
	prevk := reader.initialKey
	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(k, prevk) < 1)
		prevk = k
		i++
	}
	assert.Equal(t, keyCount, i)
}
