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
	"bytes"
	"context"
	"os"
	"sort"
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"

	memapp "github.com/codenotary/immudb/embedded/appendable/memory"

	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	tree, err := newTBTree(
		128*1024*1024,
		(1+rand.Intn(100))*PageSize,
	)
	require.NoError(t, err)

	keyRange := Range{Min: 100, Max: 1000}
	valueRange := Range{Min: 1, Max: 100}

	keyBuf := make([]byte, keyRange.Max)
	valueBuf := make([]byte, valueRange.Max)

	gen := EntryGenerator{
		NewEntry: func(rnd *rand.Rand, i int) Entry {
			rnd.Read(keyBuf)
			rnd.Read(valueBuf)

			ks := keyRange.Int(rnd)
			vs := valueRange.Int(rnd)

			return Entry{
				Ts:    uint64(i + 1),
				HOff:  OffsetNone,
				HC:    0,
				Key:   keyBuf[:ks],
				Value: valueBuf[:vs],
			}
		},
	}
	gen.WithSeed(time.Now().UnixNano())

	n := 10000
	gen.Times(n, func(_ int, e Entry) {
		err := tree.Insert(e)
		require.NoError(t, err)
	})

	require.Equal(t, tree.wb.UsedPages(), tree.nSplits+1)
	require.True(t, tree.rootPageID().isMemPage())
	require.Equal(t, tree.Ts(), uint64(n))

	writeSnap, err := tree.WriteSnapshot()
	require.NoError(t, err)
	requireEntriesAreSorted(t, writeSnap, n)
	writeSnap.Close()

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)
	require.Zero(t, tree.wb.UsedPages())

	treeApp := tree.treeApp
	size, _ := treeApp.Size()
	require.Greater(t, size, int64(0))
	require.False(t, tree.rootPageID().isMemPage())

	requireNoPageIsPinned(t, tree.pgBuf)

	gen.Times(n, func(_ int, e Entry) {
		err := tree.UseEntry(e.Key, func(e1 *Entry) error {
			require.Equal(t, e, *e1)
			return nil
		})
		require.NoError(t, err)
	})

	requireNoPageIsPinned(t, tree.pgBuf)

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)
	defer snap.Close()

	requireEntriesAreSorted(t, snap, n)
	require.Equal(t, tree.ActiveSnapshots(), 1)
}

// TODO: following tests must be merged.
// TODO: only a key version per tx can be set.
func TestInsertDuplicateKeys(t *testing.T) {
	tree, err := newTBTree(
		1024*1024,
		100*PageSize,
	)
	require.NoError(t, err)

	type entry struct {
		Key   []byte
		Count int
	}

	m := 100
	keySpace := make([]entry, m)

	gen := EntryGenerator{
		NewEntry: func(_ *rand.Rand, i int) Entry {
			e := &keySpace[rand.Intn(m)]
			e.Count++

			return Entry{
				Ts:    uint64(i + 1),
				HOff:  OffsetNone,
				Key:   e.Key,
				Value: []byte(fmt.Sprintf("v%d", e.Count)),
			}
		},
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < m; i++ {
		key := make([]byte, 10+rand.Intn(100))
		rand.Read(key)

		keySpace[i] = entry{Key: key}
	}

	keysMap := make(map[string]int, m)
	for i, e := range keySpace {
		keysMap[string(e.Key)] = i
	}
	require.Len(t, keysMap, m)

	n := 10000
	gen.Times(n, func(i int, e Entry) {
		err := tree.Insert(e)
		require.NoError(t, err)
	})

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	it, err := tree.NewIterator(DefaultIteratorOptions())
	require.NoError(t, err)

	expectedEntries := 0
	for i := range keySpace {
		if keySpace[i].Count > 0 {
			expectedEntries++
		}
	}

	checkHistory := func(key []byte, n int) {
		for v := 1; v <= n; v++ {
			value, _, err := tree.GetRevision(key, v)
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("v%d", v)), value)
		}
	}

	nFoundEntries := 0
	for {
		e, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		require.NoError(t, err)

		idx, has := keysMap[string(e.Key)]
		require.True(t, has)

		count := keySpace[idx].Count
		require.Equal(t, e.HC+1, uint64(count))

		checkHistory(e.Key, count)

		nFoundEntries++
	}
	require.Equal(t, expectedEntries, nFoundEntries)
}

func TestInsertDuplicateKey(t *testing.T) {
	tree, err := newTBTree(128*1024*1024, 100*PageSize)
	require.NoError(t, err)

	key := make([]byte, 50)
	rand.Read(key)

	gen := EntryGenerator{
		NewEntry: func(_ *rand.Rand, i int) Entry {
			return Entry{
				Ts:    uint64(i + 1),
				HOff:  OffsetNone,
				HC:    0,
				Key:   key,
				Value: []byte(fmt.Sprintf("value%d", i)),
			}
		},
	}

	n := 1009 + rand.Intn(10000)
	gen.Times(n, func(i int, e Entry) {
		err := tree.Insert(e)
		require.NoError(t, err)
	})

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	err = tree.UseEntry(key, func(e *Entry) error {
		require.Equal(t, []byte(fmt.Sprintf("value%d", n-1)), e.Value)
		return nil
	})
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		value, hc, err := tree.GetRevision(key, i+1)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), hc)
		require.Equal(t, []byte(fmt.Sprintf("value%d", i)), value)
	}
}

func TestRecoverSnapshotDuringOpen(t *testing.T) {
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageBuffer((1 + rand.Intn(100)) * PageSize)

	var treeAppBuf bytes.Buffer
	historyApp := memapp.New()

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactoryFunc(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			switch subPath {
			case "tree":
				return memapp.NewWithBuffer(&treeAppBuf), nil
			case "history":
				return historyApp, nil
			}
			return nil, fmt.Errorf("unknown path: %s", subPath)
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	tree, err := Open("", opts)
	require.NoError(t, err)

	n := 100
	_, err = randomInserts(tree, n)
	require.NoError(t, err)

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	firstRootID := tree.rootPageID()
	require.Zero(t, tree.StalePages(), 0)

	_, err = randomInserts(tree, n)
	require.NoError(t, err)

	var expectedStalePages uint32
	for slot := 0; slot < wb.UsedPages(); slot++ {
		pg, err := wb.Get(memPageID(slot))
		require.NoError(t, err)

		if pg.IsCopied() {
			expectedStalePages++
		}
	}

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	latestRootID := tree.rootPageID()
	require.Equal(t, tree.StalePages(), expectedStalePages)

	err = tree.Close()
	require.NoError(t, err)

	/*
		opts = opts.WithAppFactoryFunc(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			switch subPath {
			case "tlog":
				return memapp.NewWithBuffer(&treeAppCopy), nil
			case "hlog":
				return historyApp, nil
			}
			return nil, fmt.Errorf("unknown path: %s", subPath)
		})
	*/
	t.Run("recover latest snapshot", func(t *testing.T) {
		tree, err := Open("", opts)
		require.NoError(t, err)

		require.Equal(t, latestRootID, tree.rootPageID())
		require.Equal(t, uint64(2*n), tree.Ts())
		require.Equal(t, tree.StalePages(), expectedStalePages)

		err = tree.Close()
		require.NoError(t, err)
	})

	t.Run("recover previous snapshot", func(t *testing.T) {
		treeApp := tree.treeApp

		size, _ := treeApp.Size()
		newSize := int(firstRootID) + rand.Intn(int(size)-int(firstRootID))
		err := treeApp.SetOffset(int64(newSize))
		require.NoError(t, err)

		tree, err := Open("", opts)
		require.NoError(t, err)
		require.Equal(t, tree.rootPageID(), firstRootID)

		size, _ = treeApp.Size()
		require.Equal(t, int64(firstRootID), size-CommitEntrySize)
		require.Zero(t, tree.StalePages())
		require.Equal(t, uint64(n), tree.Ts())
	})

	t.Run("recover to previous snapshot due to checksum mismatch", func(t *testing.T) {
		_, err := Open("", opts)
		require.NoError(t, err)

		//size, _ := treeApp.Size()
		//newSize := int(firstRootID) + rand.Intn(int(size)-int(firstRootID))

		//for off := size; off < int64(newSize)-1; {

		//}
	})

	// TODO: test hlog recovery (hlog gets trimmed before the last entry pointed by treelog)
}

func TestIteratorSeek(t *testing.T) {
	minLen, maxLen := 10, 100

	numKeys := 1000
	keys, err := RandomKeys(numKeys, minLen, maxLen)
	require.NoError(t, err)

	unsortedKeys := keys[:numKeys/2]
	sortedKeys := keys[numKeys/2:]

	sort.Slice(sortedKeys, func(i, j int) bool {
		return bytes.Compare(sortedKeys[i], sortedKeys[j]) < 0
	})

	tree, err := newTBTree(
		128*1024*1024,
		(1+rand.Intn(100))*PageSize,
	)
	require.NoError(t, err)
	defer tree.Close()

	for i, key := range sortedKeys {
		err := tree.Insert(Entry{
			Ts:    uint64(i + 1),
			Key:   key,
			Value: []byte(fmt.Sprintf("value%d", i)),
		})
		require.NoError(t, err)
	}

	testIteratorOnSnapshot := func(t *testing.T, snap Snapshot, reversed bool) {
		opts := DefaultIteratorOptions()
		opts.Reversed = reversed

		it, err := snap.NewIterator(opts)
		require.NoError(t, err)

		t.Run("seek to existing key", func(t *testing.T) {
			seekAt := rand.Intn(len(sortedKeys))

			err = it.Seek(sortedKeys[seekAt])
			require.NoError(t, err)
			for {
				e, err := it.Next()
				if errors.Is(err, ErrNoMoreEntries) {
					break
				}
				require.Equal(t, sortedKeys[seekAt], e.Key)

				if reversed {
					seekAt--
				} else {
					seekAt++
				}
			}

			if reversed {
				require.Equal(t, -1, seekAt)
			} else {
				require.Equal(t, len(sortedKeys), seekAt)
			}
		})

		t.Run("seek to non existing key", func(t *testing.T) {
			seekAtKey := unsortedKeys[rand.Intn(len(unsortedKeys))]

			err = it.Seek(seekAtKey)
			require.NoError(t, err)

			expectedSeekAt := sort.Search(len(sortedKeys), func(i int) bool {
				return bytes.Compare(sortedKeys[i], seekAtKey) > 0
			})
			if reversed {
				expectedSeekAt--
			}

			for {
				e, err := it.Next()
				if errors.Is(err, ErrNoMoreEntries) {
					break
				}
				require.Equal(t, sortedKeys[expectedSeekAt], e.Key)

				if reversed {
					expectedSeekAt--
				} else {
					expectedSeekAt++
				}
			}

			if reversed {
				require.Equal(t, -1, expectedSeekAt)
			} else {
				require.Equal(t, len(sortedKeys), expectedSeekAt)
			}
		})

		err = it.Close()
		require.NoError(t, err)

		requireNoPageIsPinned(t, tree.pgBuf)
	}

	t.Run("iteration on write snapshot", func(t *testing.T) {
		snap, err := tree.WriteSnapshot()
		require.NoError(t, err)
		defer snap.Close()

		t.Run("forward iteration", func(t *testing.T) {
			testIteratorOnSnapshot(t, snap, false)
		})

		t.Run("backward iteration", func(t *testing.T) {
			testIteratorOnSnapshot(t, snap, true)
		})
	})

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	t.Run("iteration on read snapshot", func(t *testing.T) {
		snap, err := tree.ReadSnapshot()
		require.NoError(t, err)
		defer snap.Close()

		t.Run("forward iteration", func(t *testing.T) {
			testIteratorOnSnapshot(t, snap, false)
		})

		t.Run("backward iteration", func(t *testing.T) {
			testIteratorOnSnapshot(t, snap, true)
		})
	})
}

func TestConcurrentIterationOnMultipleSnapshots(t *testing.T) {
	pgBuf := NewPageBuffer(100 * PageSize)
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	n := 1000
	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithMaxActiveSnapshots(n).
		WithAppFactoryFunc(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	tree, err := Open("", opts)
	require.NoError(t, err)

	var key [4]byte

	snapshots := make([]Snapshot, n)

	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint32(key[:], uint32(i))

		err := tree.Insert(Entry{
			Ts:    uint64(i + 1),
			Key:   key[:],
			Value: key[:],
		})
		require.NoError(t, err)

		err = tree.FlushReset(context.Background())
		require.NoError(t, err)

		snap, err := tree.ReadSnapshot()
		require.NoError(t, err)

		snapshots[i] = snap
	}

	checkSnapshot := func(snap Snapshot, upTo int) {
		m := 0

		it, err := snap.NewIterator(DefaultIteratorOptions())
		require.NoError(t, err)
		for {
			kv, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
			require.NoError(t, err)

			require.Equal(t, uint64(m+1), kv.Ts)
			require.Equal(t, uint32(m), binary.BigEndian.Uint32(kv.Key))
			require.Equal(t, uint32(m), binary.BigEndian.Uint32(kv.Value))

			m++
		}
		require.Equal(t, upTo+1, m)
	}

	var wg sync.WaitGroup
	wg.Add(n)

	for upTo, snap := range snapshots {
		go func(snap Snapshot, upTo int) {
			defer wg.Done()

			checkSnapshot(snap, upTo)
		}(snap, upTo)
	}
	wg.Wait()

	requireNoPageIsPinned(t, pgBuf)

	for _, snap := range snapshots {
		err := snap.Close()
		require.NoError(t, err)
	}
	require.Equal(t, 0, tree.ActiveSnapshots())
}

func TestIteratorNextBetween(t *testing.T) {
	pgBuf := NewPageBuffer(100 * PageSize)
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactoryFunc(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	tree, err := Open("", opts)
	require.NoError(t, err)

	var key [4]byte

	numKeys := 10

	for m := 0; m < numKeys; m++ {
		for n := m; n < numKeys; n++ {
			binary.BigEndian.PutUint32(key[:], uint32(n))

			err := tree.Insert(Entry{
				Ts:    uint64(m + 1),
				Key:   key[:],
				Value: []byte(fmt.Sprintf("value%d", m+1)),
			})
			require.NoError(t, err)
		}
	}

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	itOpts := DefaultIteratorOptions()
	itOpts.StartTs = 10
	itOpts.EndTs = 1

	_, err = snap.NewIterator(itOpts)
	require.ErrorIs(t, err, ErrIllegalArguments)

	checkSnapshotBetween := func(initialTs, finalTs uint64) {
		itOpts := DefaultIteratorOptions()
		itOpts.StartTs = initialTs
		itOpts.EndTs = finalTs

		it, err := snap.NewIterator(itOpts)
		require.NoError(t, err)
		defer it.Close()

		err = it.Seek(nil)
		require.NoError(t, err)

		require.Greater(t, initialTs, uint64(0))

		n := initialTs - 1
		for {
			e, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}

			binary.BigEndian.PutUint32(key[:], uint32(n))

			expectedTs := finalTs
			if n+1 < expectedTs {
				expectedTs = n + 1
			}

			require.Equal(t, e.Ts, uint64(expectedTs))
			require.Equal(t, key[:], e.Key)
			require.Equal(t, fmt.Sprintf("value%d", e.Ts), string(e.Value))

			n++
		}
		require.Equal(t, n, uint64(numKeys))
	}

	for initialTs := 1; initialTs <= numKeys; initialTs++ {
		checkSnapshotBetween(uint64(initialTs), uint64(numKeys))
	}

	for finalTs := 1; finalTs <= numKeys; finalTs++ {
		checkSnapshotBetween(uint64(1), uint64(finalTs))
	}
}

func TestIterator(t *testing.T) {
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageBuffer(64 * 1024 * 1024)

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactoryFunc(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	tree, err := Open("", opts)
	require.NoError(t, err)

	n := 10000
	for i := 0; i < n; i += 2 {
		x := binary.BigEndian.AppendUint32(nil, uint32(i))

		err := tree.Insert(Entry{
			Ts:    uint64(i + 1),
			HOff:  OffsetNone,
			Key:   x,
			Value: x,
		})
		require.NoError(t, err)
	}

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	t.Run("forward iteration", func(t *testing.T) {
		it, err := tree.NewIterator(DefaultIteratorOptions())
		require.NoError(t, err)

		seekAt := rand.Intn(n)
		err = it.Seek(binary.BigEndian.AppendUint32(nil, uint32(seekAt)))
		require.NoError(t, err)

		if seekAt%2 == 1 {
			seekAt++
		}

		for {
			kv, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
			require.NoError(t, err)

			expectedKey := binary.BigEndian.AppendUint32(nil, uint32(seekAt))
			require.Equal(t, &Entry{
				Ts:    uint64(seekAt + 1),
				HOff:  OffsetNone,
				Key:   expectedKey,
				Value: expectedKey,
			}, kv)

			seekAt += 2
		}
		require.Equal(t, seekAt, n)
	})

	t.Run("backward iteration", func(t *testing.T) {
		opts := DefaultIteratorOptions()
		opts.Reversed = true

		it, err := tree.NewIterator(opts)
		require.NoError(t, err)

		seekAt := rand.Intn(n)
		err = it.Seek(binary.BigEndian.AppendUint32(nil, uint32(seekAt)))
		require.NoError(t, err)

		if seekAt%2 == 1 {
			seekAt--
		}

		for {
			kv, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
			require.NoError(t, err)

			expectedKey := binary.BigEndian.AppendUint32(nil, uint32(seekAt))
			require.Equal(t, &Entry{
				Ts:    uint64(seekAt + 1),
				HOff:  OffsetNone,
				Key:   expectedKey,
				Value: expectedKey,
			}, kv)

			seekAt -= 2
		}
		require.Equal(t, seekAt, -2)
	})
}

func TestSnapshotIsolation(t *testing.T) {
	tree, err := newTBTree(
		10*1024*1024,
		(1+rand.Intn(100))*PageSize,
	)
	require.NoError(t, err)

	var keyBuf [4]byte

	numKeys := 10
	numInserts := 1000
	for i := 1; i <= numInserts; i++ {
		for j := 0; j < numKeys; j++ {
			binary.BigEndian.PutUint32(keyBuf[:], uint32(j))

			err := tree.Insert(Entry{
				Ts:    uint64(i),
				HOff:  OffsetNone,
				HC:    0,
				Key:   keyBuf[:],
				Value: []byte(fmt.Sprintf("value%d-%d", j, i)),
			})
			require.NoError(t, err)
		}
	}

	checkSnapshot := func(txID uint64) {
		snap, err := tree.SnapshotAtTs(context.Background(), txID)
		require.NoError(t, err)
		defer snap.Close()

		require.Equal(t, tree.Ts(), uint64(numInserts))
		hcTotal := 0
		n := 0
		it, err := snap.NewIterator(DefaultIteratorOptions())
		require.NoError(t, err)
		for {
			e, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
			require.NoError(t, err)
			require.LessOrEqual(t, e.Ts, txID)

			binary.BigEndian.PutUint32(keyBuf[:], uint32(n))
			require.Equal(t, keyBuf[:], e.Key)
			require.Equal(t, []byte(fmt.Sprintf("value%d-%d", n, txID)), e.Value)
			n++

			hcTotal += int(e.HC) + 1
		}
		require.Equal(t, n*int(txID), hcTotal)

		if txID == 0 {
			require.Zero(t, n)
		} else {
			require.Equal(t, numKeys, n)
		}
	}

	t.Run("iteration", func(t *testing.T) {
		for tx := 1; tx <= numInserts; tx++ {
			checkSnapshot(uint64(tx))
		}
	})

	t.Run("get", func(t *testing.T) {
		for tx := 1; tx <= numInserts; tx++ {
			snap, err := tree.SnapshotAtTs(context.Background(), uint64(tx))
			require.NoError(t, err)

			nKey := rand.Intn(numKeys)
			binary.BigEndian.PutUint32(keyBuf[:], uint32(nKey))

			value, ts, hc, err := snap.Get(keyBuf[:])
			require.NoError(t, err)
			require.Equal(t, ts, uint64(tx))
			require.Equal(t, hc, uint64(ts))
			require.Equal(t, value, []byte(fmt.Sprintf("value%d-%d", nKey, tx)))

			snap.Close()
		}
	})

	t.Run("get between", func(t *testing.T) {
		for tx := 1; tx <= numInserts; tx++ {
			snap, err := tree.SnapshotAtTs(context.Background(), uint64(tx))
			require.NoError(t, err)

			nKey := rand.Intn(numKeys)
			binary.BigEndian.PutUint32(keyBuf[:], uint32(nKey))

			value, ts, hc, err := snap.GetBetween(keyBuf[:], 0, uint64(numInserts))
			require.NoError(t, err)
			require.Equal(t, ts, uint64(tx))
			require.Equal(t, hc, uint64(ts))
			require.Equal(t, value, []byte(fmt.Sprintf("value%d-%d", nKey, tx)))

			snap.Close()
		}
	})

	t.Run("history", func(t *testing.T) {
		for tx := 1; tx <= numInserts; tx++ {
			snap, err := tree.SnapshotAtTs(context.Background(), uint64(tx))
			require.NoError(t, err)

			nKey := rand.Intn(numKeys)
			binary.BigEndian.PutUint32(keyBuf[:], uint32(nKey))

			values, hc, err := snap.History(keyBuf[:], 0, false, numInserts)
			require.NoError(t, err)
			require.Len(t, values, tx)
			require.Equal(t, hc, uint64(tx))

			for i, v := range values {
				require.Equal(t, v.Value, []byte(fmt.Sprintf("value%d-%d", nKey, i+1)))
			}
			snap.Close()
		}
	})
}

func TestGetWithPrefix(t *testing.T) {
	tree, err := newTBTree(
		1024*1024,
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

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	prefix := key[:1+rand.Intn(len(key))]

	_, _, _, _, err = snap.GetWithPrefix(prefix, key)
	require.ErrorIs(t, err, ErrKeyNotFound)

	keyFound, value, ts, hc, err := snap.GetWithPrefix(prefix, nil)
	require.Equal(t, ts, uint64(nEntries))
	require.Equal(t, hc, uint64(nEntries))

	require.NoError(t, err)
	require.Equal(t, key, keyFound)
	require.Equal(t, []byte(fmt.Sprintf("value%d", nEntries-1)), value)
}

func TestGetBetween(t *testing.T) {
	tree, err := newTBTree(
		1024*1024,
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

	err = tree.FlushReset(context.Background())
	require.NoError(t, err)

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	for initialTs := 1; initialTs < nEntries+1; initialTs++ {
		_, ts, hc, err := snap.GetBetween(key, uint64(initialTs), uint64(nEntries))
		require.NoError(t, err)
		require.Equal(t, hc, uint64(nEntries))
		require.Equal(t, ts, uint64(nEntries))
	}

	for finalTs := uint64(1); finalTs <= uint64(nEntries); finalTs++ {
		_, ts, hc, err := snap.GetBetween(key, 1, finalTs)
		require.NoError(t, err)

		require.Equal(t, hc, uint64(nEntries))
		require.Equal(t, finalTs, ts)

		_, ts, hc, err = snap.GetBetween(key, finalTs, finalTs)
		require.NoError(t, err)

		require.Equal(t, hc, uint64(nEntries))
		require.Equal(t, finalTs, ts)
	}

	_, ts, hc, err := snap.GetBetween(key, uint64(1), uint64(nEntries+1))
	require.NoError(t, err)
	require.Equal(t, hc, uint64(nEntries))
	require.Equal(t, ts, uint64(nEntries))
}

func TestCompact(t *testing.T) {
	wb, err := newWriteBuffer(1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageBuffer(100 * PageSize)

	var compactedTreeApp appendable.Appendable

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactoryFunc(func(_, subpath string, _ *multiapp.Options) (appendable.Appendable, error) {
			memApp := memapp.New()
			if subpath == snapFolder("tree", 2) {
				compactedTreeApp = memApp
			}
			return memApp, nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	tree, err := Open(
		"",
		opts,
	)
	require.NoError(t, err)

	var keyBuf [4]byte

	n := 100

	insertKeys := func(n int, ts uint64, valuePrefix string) {
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint32(keyBuf[:], uint32(i)+1)

			err := tree.Insert(Entry{
				Ts:    ts,
				Key:   keyBuf[:],
				Value: []byte(fmt.Sprintf("%s-%d", valuePrefix, i)),
			})
			require.NoError(t, err)
		}
		err = tree.FlushReset(context.Background())
		require.NoError(t, err)
	}

	insertKeys(n, 1, "value")
	require.Equal(t, tree.StalePagePercentage(), float32(0))

	insertKeys(n, 2, "new-value")
	require.Greater(t, tree.StalePagePercentage(), float32(0))

	sizeBeforeCompaction, err := tree.treeApp.Size()
	require.NoError(t, err)

	require.Greater(t, tree.StalePages(), uint32(0))

	err = tree.Compact(context.Background())
	require.Equal(t, err, ErrCompactionThresholdNotReached)

	tree.compactionThld = 0.1

	concurrentCompactions := 10
	var doneCompactions atomic.Uint32

	var wg sync.WaitGroup
	wg.Add(concurrentCompactions)
	for i := 0; i < concurrentCompactions; i++ {
		go func() {
			defer wg.Done()

			err = tree.Compact(context.Background())
			if err == nil {
				doneCompactions.Add(1)
			}

			if err != nil {
				require.ErrorIs(t, err, ErrCompactionInProgress)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 1, int(doneCompactions.Load()))

	require.NotNil(t, compactedTreeApp)

	sizeAfterCompaction, err := compactedTreeApp.Size()
	require.NoError(t, err)
	require.Less(t, sizeAfterCompaction, sizeBeforeCompaction)

	hLog := tree.historyApp

	err = tree.Close()
	require.NoError(t, err)

	tree, err = Open("", opts.WithAppFactoryFunc(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
		switch subPath {
		case "history":
			return hLog, nil
		case "tree":
			return compactedTreeApp, nil
		}
		return memapp.New(), nil
	}))
	require.NoError(t, err)

	treeSize, err := tree.treeApp.Size()
	require.NoError(t, err)

	require.Equal(t, sizeAfterCompaction, treeSize)
	require.Equal(t, uint32(0), tree.StalePages())
	require.Equal(t, float32(0), tree.StalePagePercentage())

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	it, err := snap.NewIterator(DefaultIteratorOptions())
	require.NoError(t, err)

	err = it.Seek(nil)
	require.NoError(t, err)

	m := 0
	for {
		e, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		require.NoError(t, err)

		binary.BigEndian.PutUint32(keyBuf[:], uint32(m)+1)
		require.Equal(t, keyBuf[:], e.Key)
		require.Equal(t, string(e.Value), fmt.Sprintf("new-value-%d", m))

		m++
	}
	require.Equal(t, n, m)
}

/*
func BenchmarkInsert(b *testing.B) {
	treeApp := appendable.NewInMemoryAppendable()
	historyApp := appendable.NewInMemoryAppendable()

	wb := newWriteBuffer(128 * 1024 * 1024)
	pgBuf := newPageBuffer(treeApp, 64*1024*1024)

	tree, err := Open(0, wb, pgBuf, treeApp, historyApp)
	if err != nil {
		panic(err)
	}

	key := make([]byte, 1000)
	value := make([]byte, 100)

	seed := time.Now().UnixNano()
	rand.Seed(seed)

	b.ReportAllocs()
	//b.ResetTimer()

	n := 0
	for n < b.N {
		_ = make([]byte, 100)
		rand.Read(key)
		rand.Read(value)

		ks := 10 + rand.Intn(len(key)-10+1)
		vs := 1 + rand.Intn(len(value))

		err := tree.Insert(Entry{
			Ts:    uint64(n),
			Key:   key[:ks],
			Value: value[:vs],
		})
		if errors.Is(err, ErrWriteBufferFull) {
			break
		}
		n++
	}
	b.ReportMetric(float64(n), "insert_ops")
}
*/

func requireEntriesAreSorted(t *testing.T, snap Snapshot, expectedKVs int) {
	var currKV Entry
	n := 0

	it, err := snap.NewIterator(DefaultIteratorOptions())
	require.NoError(t, err)

	for {
		kv, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		n++

		require.NoError(t, err)
		if n > 0 {
			require.Greater(t, kv.Key, currKV.Key)
		}

		// NOTE: kv is no more reusable after calling next
		currKV = kv.Copy()
	}
	require.Equal(t, expectedKVs, n)
}

func newTBTree(writeBufferSize, pageBufferSize int) (*TBTree, error) {
	wb, err := newWriteBuffer(writeBufferSize)
	if err != nil {
		return nil, err
	}

	pgBuf := NewPageBuffer(pageBufferSize)

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactoryFunc(func(_, _ string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	return Open(
		"",
		opts,
	)
}

func newWriteBuffer(size int) (*WriteBuffer, error) {
	chunkSize := 1024 * 1024
	if size%chunkSize != 0 {
		return nil, fmt.Errorf("size must be a multiple of chunk size")
	}

	sw := NewSharedWriteBuffer(size, chunkSize)
	return NewWriteBuffer(sw, chunkSize, size)
}

func requireNoPageIsPinned(t *testing.T, buf *PageBuffer) {
	for i := range buf.descriptors {
		desc := &buf.descriptors[i]
		require.True(t, desc.lock.Free())
	}
}

func randomInserts(tree *TBTree, n int) (int, error) {
	totalBytesWritten := 0

	key := make([]byte, 1000)
	value := make([]byte, 100)

	ts := tree.Ts()
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		rand.Read(key)
		rand.Read(value)

		ks := 10 + rand.Intn(len(key)-10+1)
		vs := 1 + rand.Intn(len(value))

		err := tree.Insert(Entry{
			Ts:    uint64(ts+1) + uint64(i),
			Key:   key[:ks],
			Value: value[:vs],
		})
		if err != nil {
			return -1, err
		}
		totalBytesWritten += 8 + ks + vs
	}
	return totalBytesWritten, nil
}

type Range struct {
	Min int
	Max int
}

func (r *Range) Int(rnd *rand.Rand) int {
	return r.Min + rnd.Intn(r.Max-r.Min+1)
}

type EntryGenerator struct {
	Seed     int64
	NewEntry func(*rand.Rand, int) Entry
}

func (gen *EntryGenerator) WithSeed(seed int64) {
	gen.Seed = seed
}

func (gen *EntryGenerator) Times(n int, f func(int, Entry)) {
	seed := gen.Seed
	if gen.Seed == 0 {
		seed = time.Now().UnixNano()
	}

	src := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		f(i, gen.NewEntry(src, i))
	}
}

func RandomKeys(n int, minLen, maxLen int) ([][]byte, error) {
	rand.Seed(time.Now().UnixNano())
	uniqueKeys := make(map[string][]byte)

	for len(uniqueKeys) < n {
		keyLen := minLen + rand.Intn(maxLen-minLen+1)

		key := make([]byte, keyLen)
		_, err := rand.Read(key)
		if err != nil {
			return nil, fmt.Errorf("error generating random key: %v", err)
		}

		keyStr := string(key)
		if _, exists := uniqueKeys[keyStr]; !exists {
			uniqueKeys[keyStr] = key
		}
	}

	keys := make([][]byte, 0, len(uniqueKeys))
	for _, key := range uniqueKeys {
		keys = append(keys, key)
	}
	return keys, nil
}
