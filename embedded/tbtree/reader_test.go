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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderForEmptyTreeShouldReturnError(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.NewReader(ReaderSpec{SeekKey: make([]byte, tbtree.maxKeySize+1)})
	require.ErrorIs(t, err, ErrIllegalArguments)

	r, err := snapshot.NewReader(ReaderSpec{SeekKey: []byte{0, 0, 0, 0}, DescOrder: false})
	require.NoError(t, err)

	_, _, _, _, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreEntries)

	_, _, _, _, err = r.ReadBetween(1, 1)
	require.ErrorIs(t, err, ErrNoMoreEntries)
}

func TestReaderWithInvalidSpec(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()
}

func TestReaderAscendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := ReaderSpec{
		SeekKey:   []byte{0, 0, 0, 250},
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrReadersNotClosed)

	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.Read()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = reader.Reset()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = reader.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestReaderAscendingScanWithEndingKey(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := ReaderSpec{
		EndKey:       []byte{0, 0, 0, 100},
		InclusiveEnd: true,
		Prefix:       []byte{0, 0, 0},
		DescOrder:    false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrReadersNotClosed)

	var lastKey []byte

	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)

		lastKey = k
	}

	require.Equal(t, rspec.EndKey, lastKey)

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.Read()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = reader.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestReaderAscendingScanAsBefore(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := ReaderSpec{
		SeekKey:   []byte{0, 0, 0, 250},
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrReadersNotClosed)

	for {
		k, _, _, hc, err := reader.ReadBetween(0, 1001)
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
		require.Equal(t, uint64(1), hc)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.ReadBetween(0, 0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = reader.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestReaderAsBefore(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	key := []byte{0, 0, 0, 250}
	value := []byte{0, 0, 0, 251}

	for i := 0; i < 10; i++ {
		err = tbtree.Insert(key, value)
		require.NoError(t, err)
	}

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := ReaderSpec{
		Prefix: key,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	k, v, ts, hc, err := reader.ReadBetween(1, 9)
	require.NoError(t, err)
	require.Equal(t, key, k)
	require.Equal(t, value, v)
	require.Equal(t, uint64(9), ts)
	require.Equal(t, uint64(9), hc)

	err = reader.Close()
	require.NoError(t, err)
}

func TestReaderAscendingScanWithoutSeekKey(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := ReaderSpec{
		SeekKey:   nil,
		Prefix:    []byte{0, 0, 0, 250},
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrReadersNotClosed)

	for {
		k, _, _, _, err := reader.Read()
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(reader.seekKey, k) < 1)
	}

	err = reader.Close()
	require.NoError(t, err)

	_, _, _, _, err = reader.Read()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = reader.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestReaderDescendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

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

	rspec := ReaderSpec{
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
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestReaderDescendingScanAsBefore(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

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

	rspec := ReaderSpec{
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
		k, _, _, hc, err := reader.ReadBetween(0, uint64(keyCount))
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		require.Equal(t, uint64(1), hc)

		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestReaderDescendingWithoutSeekKeyScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	keyCount := 1024
	monotonicInsertions(t, tbtree, 1, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	prefixKey := make([]byte, 3)
	prefixKey[2] = 1

	rspec := ReaderSpec{
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
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(prevk, k) > 0)
		prevk = k
		i++
	}
	require.Equal(t, 256, i)
}

func TestFullScanAscendingOrder(t *testing.T) {
	dir := t.TempDir()
	tbtree, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open(dir, DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(keyCount), snapshot.Ts())
	defer snapshot.Close()

	rspec := ReaderSpec{
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
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(prevk, k) < 1)
		prevk = k
		i++
	}
	require.Equal(t, keyCount, i)
}

func TestFullScanDescendingOrder(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	keyCount := 10000
	randomInsertions(t, tbtree, keyCount, false)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	rspec := ReaderSpec{
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
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.True(t, bytes.Compare(k, prevk) < 1)
		prevk = k
		i++
	}
	require.Equal(t, keyCount, i)
}
