/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryReaderEdgeCases(t *testing.T) {
	tbtree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)
	defer snapshot.Close()

	_, err = snapshot.NewHistoryReader(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	rspec := &HistoryReaderSpec{
		Key:       []byte{0, 0, 0, 250},
		Offset:    0,
		DescOrder: false,
	}

	reader, err := snapshot.NewHistoryReader(rspec)
	require.NoError(t, err)

	err = snapshot.Close()
	require.ErrorIs(t, err, ErrReadersNotClosed)

	err = reader.Close()
	require.NoError(t, err)

	err = reader.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = reader.Read()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestHistoryReaderAscendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(8).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

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
	require.ErrorIs(t, err, ErrReadersNotClosed)

	for {
		tvs, err := reader.Read()
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.Len(t, tvs, itCount)

		for i := 0; i < itCount; i++ {
			require.Equal(t, uint64(250+1+i*keyCount), tvs[i].Ts)
		}
	}
}

func TestHistoryReaderDescendingScan(t *testing.T) {
	opts := DefaultOptions().
		WithMaxKeySize(4).
		WithMaxValueSize(8)

	opts.WithMaxNodeSize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize))

	tbtree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

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
	require.ErrorIs(t, err, ErrReadersNotClosed)

	for {
		tvs, err := reader.Read()
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreEntries)
			break
		}

		require.Len(t, tvs, itCount)

		for i := 0; i < itCount; i++ {
			require.Equal(t, uint64(250+1+i*keyCount), tvs[len(tvs)-1-i].Ts)
		}
	}
}
