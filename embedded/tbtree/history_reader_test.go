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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryReaderEdgeCases(t *testing.T) {
	tbtree, err := Open("test_tree_history_edge", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_history_edge")

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.NewHistoryReader(nil)
	require.Equal(t, ErrIllegalArguments, err)

	rspec := &HistoryReaderSpec{
		Key:       []byte{0, 0, 0, 250},
		Offset:    0,
		DescOrder: false,
	}

	reader, err := snapshot.NewHistoryReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	err = reader.Close()
	require.NoError(t, err)

	err = reader.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = reader.Read()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestHistoryReaderAscendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open("test_tree_history_asc", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_history_asc")

	itCount := 10
	keyCount := 1000

	monotonicInsertions(t, tbtree, itCount, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := &HistoryReaderSpec{
		Key:       []byte{0, 0, 0, 250},
		Offset:    0,
		DescOrder: false,
		ReadLimit: itCount,
	}

	reader, err := snapshot.NewHistoryReader(rspec)
	require.NoError(t, err)

	defer reader.Close()

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	for {
		tss, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.Len(t, tss, itCount)

		for i := 0; i < itCount; i++ {
			require.Equal(t, uint64(250+1+i*keyCount), tss[i])
		}
	}
}

func TestHistoryReaderDescendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(4).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open("test_tree_history_desc", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_history_desc")

	itCount := 10
	keyCount := 1000

	monotonicInsertions(t, tbtree, itCount, keyCount, true)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer func() {
		err := snapshot.Close()
		require.NoError(t, err)
	}()

	rspec := &HistoryReaderSpec{
		Key:       []byte{0, 0, 0, 250},
		Offset:    0,
		DescOrder: true,
		ReadLimit: itCount,
	}

	reader, err := snapshot.NewHistoryReader(rspec)
	require.NoError(t, err)

	defer reader.Close()

	err = snapshot.Close()
	require.Equal(t, ErrReadersNotClosed, err)

	for {
		tss, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		require.Len(t, tss, itCount)

		for i := 0; i < itCount; i++ {
			require.Equal(t, uint64(250+1+i*keyCount), tss[len(tss)-1-i])
		}
	}
}
