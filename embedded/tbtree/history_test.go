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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoryIterator(t *testing.T) {
	tree, err := newBTree(
		128*1024*1024,
		(1+rand.Intn(100))*PageSize,
	)
	require.NoError(t, err)

	key := make([]byte, 20)
	rand.Read(key)

	nEntries := 1000
	for n := 0; n < nEntries; n++ {
		e := Entry{
			Ts:    uint64(n + 1),
			HOff:  OffsetNone,
			HC:    0,
			Key:   key,
			Value: []byte(fmt.Sprintf("value%d", n)),
		}

		err := tree.Insert(e)
		require.NoError(t, err)
	}

	err = tree.Flush(context.Background(), true)
	require.NoError(t, err)

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	_, err = newHistoryIterator(tree.historyApp, snap, []byte("key"), uint64(nEntries))
	require.ErrorIs(t, err, ErrKeyNotFound)

	it, err := newHistoryIterator(tree.historyApp, snap, key, uint64(nEntries))
	require.NoError(t, err)
	require.Equal(t, nEntries, it.Entries())

	nDiscarded := rand.Intn(it.Entries())

	_, err = it.Discard(uint64(nDiscarded))
	require.NoError(t, err)

	for n := nDiscarded; n < nEntries; n++ {
		e, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, e.Ts, uint64(nEntries)-uint64(n))
		require.Equal(t, e.Value, []byte(fmt.Sprintf("value%d", uint64(nEntries)-uint64(n)-1)))
	}

	_, err = it.Next()
	require.ErrorIs(t, err, ErrNoMoreEntries)
}

func TestHistory(t *testing.T) {
	tree, err := newBTree(
		128*1024*1024,
		(1+rand.Intn(100))*PageSize,
	)
	require.NoError(t, err)

	key := make([]byte, 20)
	rand.Read(key)

	nEntries := 1000
	for n := 0; n < nEntries; n++ {
		e := Entry{
			Ts:    uint64(n + 1),
			HOff:  OffsetNone,
			HC:    0,
			Key:   key,
			Value: []byte(fmt.Sprintf("value%d", n)),
		}

		err := tree.Insert(e)
		require.NoError(t, err)
	}

	err = tree.Flush(context.Background(), true)
	require.NoError(t, err)

	t.Run("descending order", func(t *testing.T) {
		offset := rand.Intn(nEntries)
		limit := 1 + rand.Intn(nEntries-int(offset))

		values, _, err := tree.History(key, uint64(offset), true, limit)
		require.NoError(t, err)
		require.Len(t, values, limit)

		for i, tv := range values {
			require.Equal(t, tv.Ts, uint64(nEntries-offset-i))
			require.Equal(t, []byte(fmt.Sprintf("value%d", nEntries-offset-i-1)), tv.Value)
		}
	})

	t.Run("ascending order", func(t *testing.T) {
		offset := rand.Intn(nEntries)
		limit := 1 + rand.Intn(nEntries-int(offset))

		values, _, err := tree.History(key, uint64(offset), false, limit)
		require.NoError(t, err)
		require.Len(t, values, limit)

		for i, tv := range values {
			require.Equal(t, tv.Ts, uint64(offset+i+1))
			require.Equal(t, []byte(fmt.Sprintf("value%d", offset+i)), tv.Value)
		}
	})

	t.Run("history iteration", func(t *testing.T) {
		// TODO: try with multiple keys as well + backward

		snap, err := tree.ReadSnapshot()
		require.NoError(t, err)
		defer snap.Close()

		opts := DefaultIteratorOptions()
		opts.IncludeHistory = true

		it, err := snap.NewIterator(opts)
		require.NoError(t, err)
		defer it.Close()

		n := nEntries - 1
		for {
			e, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
			require.NoError(t, err)

			require.Equal(t, key, e.Key)
			require.Equal(t, fmt.Sprintf("value%d", n), string(e.Value))
			n--
		}
		require.Equal(t, n, -1)
	})
}
