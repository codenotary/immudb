/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderForEmptyTreeShouldReturnError(t *testing.T) {
	tbtree, err := Open("test_tree_empty", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_empty")

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.NewReader(&ReaderSpec{SeekKey: []byte{0, 0, 0, 0}, DescOrder: false})
	require.Equal(t, ErrNoMoreEntries, err)
}

func TestReaderWithInvalidSpec(t *testing.T) {
	tbtree, err := Open("test_tree_rinv", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rinv")

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.NewReader(nil)
	require.Equal(t, ErrIllegalArguments, err)
}

func TestReaderAscendingScan(t *testing.T) {
	tbtree, err := Open("test_tree_rasc", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rasc")

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := &ReaderSpec{
		SeekKey:   []byte{0, 0, 0, 250},
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.Read()
	require.Equal(t, ErrAlreadyClosed, err)

	err = reader.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestReaderAscendingScanAsBefore(t *testing.T) {
	tbtree, err := Open("test_tree_rasc_as_before", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rasc_as_before")

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := &ReaderSpec{
		SeekKey:   []byte{0, 0, 0, 250},
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	for {
		k, _, err := reader.ReadAsBefore(uint64(1001))
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, err = reader.ReadAsBefore(0)
	require.Equal(t, ErrAlreadyClosed, err)

	err = reader.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestReaderAscendingScanWithoutSeekKey(t *testing.T) {
	tbtree, err := Open("test_tree_rsasc", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rsasc")

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := &ReaderSpec{
		SeekKey:   nil,
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.Read()
	require.Equal(t, ErrAlreadyClosed, err)

	err = reader.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestReaderDescendingScan(t *testing.T) {
	tbtree, err := Open("test_tree_rdesc", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rdesc")

	keyCount := 1024
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	seekKey := make([]byte, 4)
	binary.BigEndian.PutUint32(seekKey, uint32(512))

	prefixKey := make([]byte, 3)
	prefixKey[2] = 1

	rspec := &ReaderSpec{
		SeekKey:   seekKey,
		Prefix:    prefixKey,
		DescOrder: true,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)
	defer reader.Close()

	i := 0
	prevk := reader.seekKey
	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestReaderDescendingScanAsBefore(t *testing.T) {
	tbtree, err := Open("test_tree_rdesc_as_before", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rdesc_as_before")

	keyCount := 1024
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	seekKey := make([]byte, 4)
	binary.BigEndian.PutUint32(seekKey, uint32(512))

	prefixKey := make([]byte, 3)
	prefixKey[2] = 1

	rspec := &ReaderSpec{
		SeekKey:   seekKey,
		Prefix:    prefixKey,
		DescOrder: true,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)
	defer reader.Close()

	err = reader.Reset()
	require.NoError(t, err)

	i := 0
	prevk := reader.seekKey
	for {
		k, _, err := reader.ReadAsBefore(uint64(keyCount))
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestReaderDescendingWithoutSeekKeyScan(t *testing.T) {
	tbtree, err := Open("test_tree_rsdesc", DefaultOptions().WithMaxNodeSize(MinNodeSize))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_rsdesc")

	keyCount := 1024
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	prefixKey := make([]byte, 3)
	prefixKey[2] = 1

	rspec := &ReaderSpec{
		SeekKey:   nil,
		Prefix:    prefixKey,
		DescOrder: true,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)
	defer reader.Close()

	i := 0
	prevk := reader.seekKey
	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestFullScanAscendingOrder(t *testing.T) {
	tbtree, err := Open("test_tree_asc", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_asc")

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_asc", DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(keyCount), snapshot.Ts())
	defer snapshot.Close()

	rspec := &ReaderSpec{
		SeekKey:   nil,
		Prefix:    nil,
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)
	defer reader.Close()

	i := 0
	prevk := reader.seekKey
	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(prevk, k) < 1)
		prevk = k
		i++
	}
	require.Equal(t, keyCount, i)
}

func TestFullScanDescendingOrder(t *testing.T) {
	tbtree, err := Open("test_tree_desc", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_desc")

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	rspec := &ReaderSpec{
		SeekKey:   []byte{255, 255, 255, 255},
		Prefix:    nil,
		DescOrder: true,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)
	defer reader.Close()

	i := 0
	prevk := reader.seekKey
	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.True(t, bytes.Compare(k, prevk) < 1)
		prevk = k
		i++
	}
	require.Equal(t, keyCount, i)
}
