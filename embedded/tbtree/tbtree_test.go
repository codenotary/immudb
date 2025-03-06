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
	"math"
	"os"
	"sort"
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/metrics"

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

func randomGenerator(keyLenRange, valueLenRange Range) EntryGenerator {
	keyBuf := make([]byte, keyLenRange.Max)
	valueBuf := make([]byte, valueLenRange.Max)

	return EntryGenerator{
		NewEntry: func(rnd *rand.Rand, i int) Entry {
			rnd.Read(keyBuf)
			rnd.Read(valueBuf)

			ks := keyLenRange.Int(rnd)
			vs := valueLenRange.Int(rnd)

			return Entry{
				Ts:    uint64(i + 1),
				HOff:  OffsetNone,
				HC:    0,
				Key:   keyBuf[:ks],
				Value: valueBuf[:vs],
			}
		},
	}
}

func monotonicGenerator(n int, ascending bool) EntryGenerator {
	var keyBuf [4]byte

	return EntryGenerator{
		NewEntry: func(rnd *rand.Rand, i int) Entry {
			if ascending {
				binary.BigEndian.PutUint32(keyBuf[:], uint32(i))
			} else {
				binary.BigEndian.PutUint32(keyBuf[:], uint32(n-i))
			}

			return Entry{
				Ts:    uint64(i + 1),
				HOff:  OffsetNone,
				HC:    0,
				Key:   keyBuf[:],
				Value: keyBuf[:],
			}
		},
	}
}

// TODO: test concurrent readers during inserting

func TestInsert(t *testing.T) {
	type testCase struct {
		name string
		gen  EntryGenerator
	}

	numKeys := 10000

	testCases := []testCase{
		{
			name: "small keys",
			gen:  randomGenerator(Range{Min: 10, Max: 100}, Range{Min: 1, Max: 10}),
		},
		{
			name: "medium keys",
			gen:  randomGenerator(Range{Min: 100, Max: 500}, Range{Min: 1, Max: 10}),
		},
		{
			name: "random large keys",
			gen:  randomGenerator(Range{Min: 500, Max: MaxEntrySize - 30 - 10}, Range{Min: 1, Max: 10}),
		},
		{
			name: "random large values",
			gen:  randomGenerator(Range{Min: 100, Max: 100}, Range{Min: 500, Max: 1000}),
		},
		{
			name: "monotonic ascending keys",
			gen:  monotonicGenerator(numKeys, true),
		},
		{
			name: "monotonic descending keys",
			gen:  monotonicGenerator(numKeys, false),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree, err := newTBTree(
				128*1024*1024,
				(1+rand.Intn(100))*PageSize,
			)
			require.NoError(t, err)
			defer tree.Close()

			gen := &tc.gen
			gen.WithSeed(time.Now().UnixNano())

			gen.Times(numKeys, func(_ int, e Entry) {
				err := tree.InsertAdvance(e)
				require.NoError(t, err)
			})

			require.Equal(t, tree.wb.UsedPages(), tree.nSplits+1)
			require.True(t, tree.rootPageID().isMemPage())
			require.Equal(t, tree.Ts(), uint64(numKeys))

			writeSnap, err := tree.WriteSnapshot()
			require.NoError(t, err)
			requireEntriesAreSorted(t, writeSnap, numKeys)
			writeSnap.Close()

			err = tree.FlushReset()
			require.NoError(t, err)
			require.Zero(t, tree.wb.UsedPages())

			treeApp := tree.treeApp
			size, _ := treeApp.Size()
			require.Greater(t, size, int64(0))
			require.False(t, tree.rootPageID().isMemPage())

			requireNoPageIsPinned(t, tree.pgBuf)

			gen.Times(numKeys, func(_ int, e Entry) {
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

			requireEntriesAreSorted(t, snap, numKeys)
			require.Equal(t, tree.ActiveSnapshots(), 1)
		})
	}
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
		err := tree.InsertAdvance(e)
		require.NoError(t, err)
	})

	err = tree.FlushReset()
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
		err := tree.InsertAdvance(e)
		require.NoError(t, err)
	})

	err = tree.FlushReset()
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

func TestSnapshotRecovery(t *testing.T) {
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageCache((1+rand.Intn(100))*PageSize, metrics.NewNopPageCacheMetrics())

	treeApp := memapp.New()
	historyApp := memapp.New()

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactory(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			switch subPath {
			case "tree":
				return treeApp, nil
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

	type expectedSnapshot struct {
		rootID         PageID
		ts             uint64
		tLogSize       int64
		hLogSize       int64
		maxExpectedKey int
	}

	const numSnapshots = 10
	insertEntries := func(n int) {
		ts := tree.Ts()

		var buf [4]byte
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint32(buf[:], uint32(i))

			err := tree.InsertAdvance(Entry{
				Ts:    ts + uint64(i) + 1,
				HOff:  OffsetNone,
				HC:    0,
				Key:   buf[:],
				Value: buf[:],
			})
			require.NoError(t, err)
		}
	}

	maxExpectedKey := -1

	var snapshots [numSnapshots + 1]expectedSnapshot
	for n := 0; n < numSnapshots; n++ {
		numInserts := 10 + rand.Intn(1000)
		ts := tree.Ts()
		rootID := tree.rootPageID()

		tLogSize, err := tree.treeApp.Size()
		require.NoError(t, err)

		hLogSize, err := tree.historyApp.Size()
		require.NoError(t, err)

		snapshots[n] = expectedSnapshot{
			rootID:         rootID,
			ts:             ts,
			tLogSize:       tLogSize,
			hLogSize:       hLogSize,
			maxExpectedKey: maxExpectedKey,
		}

		if numInserts > maxExpectedKey {
			maxExpectedKey = numInserts
		}

		insertEntries(numInserts)

		err = tree.FlushReset()
		require.NoError(t, err)
	}

	historyLogSize, err := historyApp.Size()
	require.NoError(t, err)

	treeLogSize, err := treeApp.Size()
	require.NoError(t, err)

	snapshots[numSnapshots] = expectedSnapshot{
		rootID:         tree.rootPageID(),
		ts:             tree.Ts(),
		tLogSize:       treeLogSize,
		hLogSize:       historyLogSize,
		maxExpectedKey: maxExpectedKey,
	}

	latestSnapshotRootID := tree.rootPageID()
	latestTs := tree.Ts()

	err = tree.Close()
	require.NoError(t, err)

	t.Run("duplicate commit entry", func(t *testing.T) {
		// append to the tree log a data segment containing
		// which contains the commit entry multiple times

		commitEntryOff := treeLogSize - CommitEntrySize

		var commitEntry [CommitEntrySize]byte
		_, err = treeApp.ReadAt(commitEntry[:], commitEntryOff)
		require.NoError(t, err)

		magic := commitEntry[len(commitEntry)-CommitMagicSize:]
		require.Equal(t, magic, []byte{CommitMagic >> 8, CommitMagic & 0xFF})

		padDataSize := 1024*1024 + rand.Intn(1024*1024)

		var buf [4096]byte
		for n := 0; n < padDataSize; {
			if rand.Float32() < 0.25 {
				_, _, err := treeApp.Append(commitEntry[:])
				require.NoError(t, err)

				n += CommitEntrySize
			} else {
				n := 1000 + rand.Intn(len(buf)-1000+1)
				_, _ = rand.Read(buf[:n])
				_, _, err := treeApp.Append(buf[:])
				require.NoError(t, err)

				n += len(buf)
			}
		}

		err = treeApp.SetOffset(treeLogSize + int64(padDataSize))
		require.NoError(t, err)

		tree, err := Open("", opts)
		require.NoError(t, err)

		require.Equal(t, latestSnapshotRootID, tree.rootPageID())
		require.Equal(t, latestTs, tree.Ts())

		size, err := tree.treeApp.Size()
		require.NoError(t, err)
		require.Equal(t, treeLogSize, size)

		hLogSize, err := tree.historyApp.Size()
		require.NoError(t, err)
		require.Equal(t, historyLogSize, hLogSize)
	})

	err = treeApp.SetOffset(treeLogSize)
	require.NoError(t, err)

	randomOffsets := func(treeLog bool, historyLog bool) (int64, int64, expectedSnapshot) {
		treeLogOff := treeLogSize
		if treeLog {
			treeLogOff = 1 + int64(rand.Intn(int(treeLogSize)))
		}

		hLogOff := historyLogSize
		if historyLog {
			hLogOff = 1 + int64(rand.Intn(int(historyLogSize)))
		}

		if !treeLog && !historyLog {
			return treeLogOff, hLogOff, snapshots[numSnapshots]
		}

		i := sort.Search(numSnapshots, func(i int) bool {
			return snapshots[i].tLogSize >= treeLogOff
		}) - 1

		j := sort.Search(numSnapshots, func(i int) bool {
			return snapshots[i].hLogSize >= hLogOff
		}) - 1

		min := i
		if j < min {
			min = j
		}
		return treeLogOff, hLogOff, snapshots[min]
	}

	checkRecoveredSnapshot := func(tree *TBTree, expectedSnap expectedSnapshot) {
		size, err := tree.treeApp.Size()
		require.NoError(t, err)

		hLogSize, err := tree.historyApp.Size()
		require.NoError(t, err)

		require.Equal(t, expectedSnap.rootID, tree.rootPageID())
		require.Equal(t, expectedSnap.ts, tree.Ts())
		require.Equal(t, expectedSnap.tLogSize, size)
		require.Equal(t, expectedSnap.hLogSize, hLogSize)

		snap, err := tree.ReadSnapshot()
		if errors.Is(err, ErrNoSnapshotAvailable) {
			require.Zero(t, tree.Ts())
			return
		}
		require.NoError(t, err)
		defer snap.Close()

		it, err := snap.NewIterator(DefaultIteratorOptions())
		require.NoError(t, err)

		var buf [4]byte
		n := 0
		for {
			e, err := it.Next()
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}

			binary.BigEndian.PutUint32(buf[:], uint32(n))

			require.NoError(t, err)
			require.LessOrEqual(t, e.Ts, tree.Ts())
			require.Equal(t, buf[:], e.Key)

			n++
		}
		require.Equal(t, expectedSnap.maxExpectedKey, n)
	}

	type testCase struct {
		name          string
		transformTLog func(int64, appendable.Appendable) appendable.Appendable
		transformHLog func(int64, appendable.Appendable) appendable.Appendable
	}

	truncateLog := func(off int64, log appendable.Appendable) appendable.Appendable {
		return &truncateAppendable{
			Appendable:  log,
			truncatedAt: off,
		}
	}

	damageLog := func(off int64, log appendable.Appendable) appendable.Appendable {
		size, _ := log.Size()

		return &damagedAppendable{
			Appendable: &truncateAppendable{
				Appendable:  log,
				truncatedAt: size,
			},
			startOff: off,
		}
	}

	testCases := []testCase{
		{
			name:          "recover latest snapshot",
			transformTLog: nil,
			transformHLog: nil,
		},
		{
			name:          "truncated tree log",
			transformTLog: truncateLog,
			transformHLog: nil,
		},
		{
			name:          "truncated history log",
			transformTLog: nil,
			transformHLog: truncateLog,
		},
		{
			name:          "truncated tree and history log",
			transformTLog: truncateLog,
			transformHLog: truncateLog,
		},
		{
			name:          "damaged tree log",
			transformTLog: damageLog,
			transformHLog: nil,
		},
		{
			name:          "damaged history log",
			transformTLog: nil,
			transformHLog: damageLog,
		},
		{
			name:          "damaged tree log and history log",
			transformTLog: damageLog,
			transformHLog: damageLog,
		},
		{
			name:          "truncated tree log and damaged history log",
			transformTLog: truncateLog,
			transformHLog: damageLog,
		},
		{
			name:          "damaged tree log and truncated history log",
			transformTLog: damageLog,
			transformHLog: truncateLog,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tLogOff, hLogOff, snap := randomOffsets(
				tc.transformTLog != nil,
				tc.transformHLog != nil,
			)

			opts = opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
				switch subPath {
				case "tree":
					if tc.transformTLog != nil {
						return tc.transformTLog(tLogOff, treeApp), nil
					}

					return &truncateAppendable{
						Appendable:  treeApp,
						truncatedAt: treeLogSize,
					}, nil
				case "history":
					if tc.transformHLog != nil {
						return tc.transformHLog(hLogOff, historyApp), nil
					}

					return &truncateAppendable{
						Appendable:  historyApp,
						truncatedAt: historyLogSize,
					}, nil
				}
				return nil, fmt.Errorf("unknown path: %s", subPath)
			})

			tree, err := Open("", opts)
			require.NoError(t, err)

			checkRecoveredSnapshot(tree, snap)
		})
	}
}

func TestStalePageCalculation(t *testing.T) {
	/*
		TODO: extract to a separate Stale Pages test

		var expectedStalePages uint32
		for slot := 0; slot < wb.UsedPages(); slot++ {
			pg, err := wb.Get(memPageID(slot))
			require.NoError(t, err)

			if pg.IsCopied() {
				expectedStalePages++
			}
		}*/
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
		err := tree.InsertAdvance(Entry{
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

	err = tree.FlushReset()
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
	pgBuf := NewPageCache(100*PageSize, metrics.NewNopPageCacheMetrics())
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	n := 1000
	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithMaxActiveSnapshots(n).
		WithAppFactory(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
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

		err := tree.InsertAdvance(Entry{
			Ts:    uint64(i + 1),
			Key:   key[:],
			Value: key[:],
		})
		require.NoError(t, err)

		err = tree.FlushReset()
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
	pgBuf := NewPageCache(100*PageSize, metrics.NewNopPageCacheMetrics())
	wb, err := newWriteBuffer(128 * 1024 * 1024)
	require.NoError(t, err)

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactory(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
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

			err := tree.InsertAdvance(Entry{
				Ts:    uint64(m + 1),
				Key:   key[:],
				Value: []byte(fmt.Sprintf("value%d", m+1)),
			})
			require.NoError(t, err)
		}
	}

	err = tree.FlushReset()
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

	pgBuf := NewPageCache(64*1024*1024, metrics.NewNopPageCacheMetrics())

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactory(func(rootPath, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
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

		err := tree.InsertAdvance(Entry{
			Ts:    uint64(i + 1),
			HOff:  OffsetNone,
			Key:   x,
			Value: x,
		})
		require.NoError(t, err)
	}

	err = tree.FlushReset()
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

func TestSnapshotVisibility(t *testing.T) {
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

			err := tree.InsertAdvance(Entry{
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

		err := tree.InsertAdvance(e)
		require.NoError(t, err)
	}

	err = tree.FlushReset()
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

		err := tree.InsertAdvance(e)
		require.NoError(t, err)
	}

	err = tree.FlushReset()
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

func TestCompaction(t *testing.T) {
	wb, err := newWriteBuffer(1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageCache(100*PageSize, metrics.NewNopPageCacheMetrics())

	var compactedTreeApp appendable.Appendable

	opts := DefaultOptions().
		WithCompactionThld(0.75).
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactory(func(_, subpath string, _ *multiapp.Options) (appendable.Appendable, error) {
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

			err := tree.InsertAdvance(Entry{
				Ts:    ts,
				Key:   keyBuf[:],
				Value: []byte(fmt.Sprintf("%s-%d", valuePrefix, i)),
			})
			require.NoError(t, err)
		}
		err = tree.FlushReset()
		require.NoError(t, err)
	}

	insertKeys(n, 1, "value")
	require.Equal(t, tree.StalePagePercentage(), float32(0))

	insertKeys(n, 2, "new-value")
	require.Greater(t, tree.StalePagePercentage(), float32(0))

	snap, err := tree.ReadSnapshot()
	require.NoError(t, err)

	requireEntriesAreSorted(t, snap, n)

	countCachedPages := func() int {
		cachedPages := 0
		for tp := range pgBuf.descTable {
			if tp.TreeID() == tree.ID() {
				cachedPages++
			}
		}
		return cachedPages
	}
	require.Greater(t, countCachedPages(), 0)

	require.NoError(t, snap.Close())

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

			err := tree.Compact(context.Background())
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

	opts = opts.WithAppFactory(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
		switch subPath {
		case "history":
			return hLog, nil
		case "tree":
			return compactedTreeApp, nil
		}
		return memapp.New(), nil
	})

	tree, err = Open("", opts)
	require.NoError(t, err)

	treeSize, err := tree.treeApp.Size()
	require.NoError(t, err)

	require.Equal(t, sizeAfterCompaction, treeSize)
	require.Equal(t, uint32(0), tree.StalePages())
	require.Equal(t, float32(0), tree.StalePagePercentage())
	require.Zero(t, countCachedPages(), 0) // cached pages are invalidated

	snap, err = tree.ReadSnapshot()
	require.NoError(t, err)
	defer snap.Close()

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

func TestFlushEdgeCases(t *testing.T) {
	wb, err := newWriteBuffer(1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageCache(100*PageSize, metrics.NewNopPageCacheMetrics())

	opts := DefaultOptions().
		WithCompactionThld(0.75).
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	t.Run("flush empty tree", func(t *testing.T) {
		tree, err := Open("", opts.WithAppFactory(func(_, _ string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}))
		require.NoError(t, err)
		defer tree.Close()

		err = tree.FlushReset()
		require.NoError(t, err)

		size, err := tree.treeApp.Size()
		require.NoError(t, err)
		require.Zero(t, size)
	})

	t.Run("flush empty tree with non zero timestamp", func(t *testing.T) {
		var treeLog appendable.Appendable

		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			log := memapp.New()
			if subPath == TreeLogFileName {
				treeLog = log
			}
			return log, nil
		}))
		require.NoError(t, err)
		defer tree.Close()

		numAdvancements := 10
		for n := 0; n < numAdvancements; n++ {
			err = tree.Advance(tree.Ts()+1, 0)
			require.NoError(t, err)

			err = tree.FlushReset()
			require.NoError(t, err)
		}

		err = tree.Close()
		require.NoError(t, err)

		tree, err = Open("", opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			if subPath == TreeLogFileName {
				return treeLog, nil
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)
		require.Equal(t, uint64(numAdvancements), tree.Ts())
		require.Zero(t, tree.IndexedEntryCount())
	})

	t.Run("flush non-empty tree with advanced timestamp", func(t *testing.T) {
		var treeLog appendable.Appendable

		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			log := memapp.New()
			if subPath == TreeLogFileName {
				treeLog = log
			}
			return log, nil
		}))
		require.NoError(t, err)

		r := Range{Min: 10, Max: 100}
		gen := randomGenerator(r, r)

		gen.WithSeed(time.Now().UnixNano())

		n := 100
		gen.Times(n, func(i int, e Entry) {
			err := tree.InsertAdvance(e)
			require.NoError(t, err)
		})

		err = tree.FlushReset()
		require.NoError(t, err)

		numTsAdvancements := 10
		for n := 0; n < numTsAdvancements; n++ {
			ts := tree.Ts()
			err = tree.Advance(ts+1, 50)
			require.NoError(t, err)

			err = tree.FlushReset()
			require.NoError(t, err)
		}

		require.NoError(t, tree.Close())

		tree, err = Open("", opts.WithAppFactory(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			if subPath == TreeLogFileName {
				return treeLog, nil
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)
		require.Equal(t, uint64(n+numTsAdvancements), tree.Ts())
	})
}

func TestOpenShouldRecoverLatestSnapshot(t *testing.T) {
	wb, err := newWriteBuffer(1024 * 1024)
	require.NoError(t, err)

	pgBuf := NewPageCache(100*PageSize, metrics.NewNopPageCacheMetrics())

	dirEntries := []os.DirEntry{
		&dirEntry{
			name:  "tree",
			isDir: true,
		},
		&dirEntry{
			name:  "history",
			isDir: true,
		},
		&dirEntry{
			name:  snapFolder("tree", 2),
			isDir: true,
		},
		&dirEntry{
			name:  snapFolder("tree", 4),
			isDir: true,
		},
		&dirEntry{
			name:  snapFolder("tree", 8),
			isDir: true,
		},
		// additional entries that should be ignored
		&dirEntry{
			name:  "dir",
			isDir: true,
		},
		&dirEntry{
			name:  "file",
			isDir: false,
		},
	}

	opts := DefaultOptions().
		WithCompactionThld(0.75).
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf)

	treeLogAtTs := func(ts uint64) appendable.Appendable {
		optsCopy := *opts

		optsCopy.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

		tree, err := Open("", &optsCopy)
		require.NoError(t, err)
		defer tree.Close()

		err = tree.Advance(ts, math.MaxUint32)
		require.NoError(t, err)

		err = tree.FlushReset()
		require.NoError(t, err)

		return tree.treeApp
	}

	// ReadDir func is supposed to return entries in lexicographic order.
	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].Name() < dirEntries[j].Name()
	})

	opts = opts.
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return dirEntries, nil
		})

	t.Run("recover last available snapshot", func(t *testing.T) {
		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			ts, err := parseSnapFolder(subPath)
			if err == nil {
				return treeLogAtTs(ts), nil
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)

		require.Equal(t, uint64(8), tree.Ts())
		require.NoError(t, tree.Close())
	})

	t.Run("recover a previous snapshot", func(t *testing.T) {
		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			ts, err := parseSnapFolder(subPath)
			if err == nil && ts > 2 {
				return nil, fmt.Errorf("damaged snapshot")
			}

			if err == nil {
				return treeLogAtTs(ts), nil
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)

		require.Equal(t, uint64(2), tree.Ts())
		require.NoError(t, tree.Close())
	})

	t.Run("recover from tree log", func(t *testing.T) {
		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			ts, err := parseSnapFolder(subPath)
			if err == nil && ts > 0 {
				return nil, fmt.Errorf("damaged snapshot")
			}

			if subPath == "tree" {
				return treeLogAtTs(100), nil
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)

		require.Equal(t, uint64(100), tree.Ts())
		require.NoError(t, tree.Close())
	})

	t.Run("recover empty tree", func(t *testing.T) {
		tree, err := Open("", opts.WithAppFactory(func(_, subPath string, _ *multiapp.Options) (appendable.Appendable, error) {
			ts, err := parseSnapFolder(subPath)
			if err == nil && ts > 0 {
				return nil, fmt.Errorf("damaged snapshot")
			}
			return memapp.New(), nil
		}))
		require.NoError(t, err)

		require.Equal(t, uint64(0), tree.Ts())
		require.NoError(t, tree.Close())
	})
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

func requireEntriesAreSorted(t *testing.T, snap Snapshot, expectedEntries int) {
	var currKV Entry
	n := 0

	it, err := snap.NewIterator(DefaultIteratorOptions())
	require.NoError(t, err)
	err = it.Seek(nil)
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
	require.Equal(t, expectedEntries, n)
}

func newTBTree(writeBufferSize, pageBufferSize int) (*TBTree, error) {
	wb, err := newWriteBuffer(writeBufferSize)
	if err != nil {
		return nil, err
	}

	pgBuf := NewPageCache(pageBufferSize, metrics.NewNopPageCacheMetrics())

	opts := DefaultOptions().
		WithWriteBuffer(wb).
		WithPageBuffer(pgBuf).
		WithAppFactory(func(_, _ string, _ *multiapp.Options) (appendable.Appendable, error) {
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
	return NewWriteBuffer(sw, chunkSize, size, metrics.NewNopWriteBufferMetrics())
}

func requireNoPageIsPinned(t *testing.T, buf *PageCache) {
	for i := range buf.descriptors {
		desc := &buf.descriptors[i]
		require.True(t, desc.lock.Free())
	}
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

type damagedAppendable struct {
	appendable.Appendable

	startOff int64
}

func (app *damagedAppendable) ReadAt(bs []byte, off int64) (int, error) {
	n := len(bs)
	m := int64(n)
	if off >= app.startOff {
		m = 0
	} else if off+int64(n) > app.startOff {
		m = app.startOff - off
	}

	if m > 0 {
		_, err := app.Appendable.ReadAt(bs[:m], off)
		if err != nil {
			return -1, err
		}
	}

	if int64(n)-m > 0 {
		rand.Read(bs[m:])
	}
	return n, nil
}

type truncateAppendable struct {
	appendable.Appendable

	truncatedAt int64
}

func (app *truncateAppendable) ReadAt(bs []byte, off int64) (int, error) {
	size, err := app.Size()
	if err != nil {
		return -1, err
	}

	if off >= size {
		return -1, fmt.Errorf("invalid offset")
	}
	return app.Appendable.ReadAt(bs, off)
}

func (app *truncateAppendable) SetOffset(off int64) error {
	app.truncatedAt = off
	return nil
}

func (app *truncateAppendable) Size() (int64, error) {
	// avoid truncation of underlying log
	if app.truncatedAt < 0 {
		return app.Appendable.Size()
	}
	return app.truncatedAt, nil
}

type dirEntry struct {
	name  string
	isDir bool
}

func (e *dirEntry) Name() string {
	return e.name
}

func (e *dirEntry) IsDir() bool {
	return e.isDir
}

func (e *dirEntry) Type() os.FileMode {
	return 0
}

func (e *dirEntry) Info() (os.FileInfo, error) {
	return nil, fmt.Errorf("unexpected call to Info()")
}
