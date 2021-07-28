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
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"

	"github.com/stretchr/testify/require"
)

func TestEdgeCases(t *testing.T) {
	defer os.RemoveAll("edge_cases")

	_, err := Open("edge_cases", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = OpenWith("edge_cases", nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	nLog := &mocked.MockedAppendable{}
	hLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	// Should fail reading maxNodeSize from metadata
	cLog.MetadataFn = func() []byte {
		return nil
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedCLog)

	// Should fail reading cLogSize
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(MetaMaxNodeSize, 1)
		return md.Bytes()
	}
	injectedError := errors.New("error")
	cLog.SizeFn = func() (int64, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Should fail truncating clog
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	cLog.SetOffsetFn = func(off int64) error {
		return injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Should succeed
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(MetaMaxNodeSize, 1)
		return md.Bytes()
	}
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize - 1, nil
	}
	cLog.SetOffsetFn = func(off int64) error {
		return nil
	}
	hLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.NoError(t, err)

	// Should fail validating cLogSize
	hLog.SizeFn = func() (int64, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Should fail validating hLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	hLog.SizeFn = func() (int64, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Should fail when unable to read from cLog
	hLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Should fail when unable to read current root node type
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		require.EqualValues(t, 0, off)
		require.Len(t, bs, cLogEntrySize)
		for i := range bs {
			bs[i] = 0
		}
		return len(bs), nil
	}
	nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, injectedError)

	// Invalid root node type
	nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		nLogBuffer := []byte{0xFF, 0, 0, 0, 0}
		require.Less(t, off, int64(len(nLogBuffer)))
		l := copy(bs, nLogBuffer[off:])
		return l, nil
	}
	_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
	require.ErrorIs(t, err, ErrReadingFileContent)

	// Error while reading a single leaf node content
	nLogBuffer := []byte{
		LeafNodeType, // Node type
		0, 0, 0, 0,   // Size, ignored
		0, 0, 0, 1, // 1 child
		0, 0, 0, 1, // key size
		123,        // key
		0, 0, 0, 1, // value size
		23,                     // value
		0, 0, 0, 0, 0, 0, 0, 0, // Timestamp
		0, 0, 0, 0, 0, 0, 0, 0, // history log offset
		0, 0, 0, 0, 0, 0, 0, 0, // history log count
	}
	for i := 1; i < len(nLogBuffer); i++ {
		injectedError := fmt.Errorf("Injected error %d", i)
		buff := nLogBuffer[:i]
		nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			if off >= int64(len(buff)) {
				return 0, injectedError
			}

			return copy(bs, buff[off:]), nil
		}
		_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	}

	// Error while reading an inner node content
	nLogBuffer = []byte{
		InnerNodeType, // Node type
		0, 0, 0, 0,    // Size, ignored
		0, 0, 0, 1, // 1 child
		0, 0, 0, 1, // min key size
		0,          // min key
		0, 0, 0, 1, // max key size
		1,                      // max key
		0, 0, 0, 0, 0, 0, 0, 0, // Timestamp
		0, 0, 0, 0, // size
		0, 0, 0, 0, 0, 0, 0, 0, // offset
	}
	for i := 1; i < len(nLogBuffer); i++ {
		injectedError := fmt.Errorf("Injected error %d", i)
		buff := nLogBuffer[:i]
		nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			if off >= int64(len(buff)) {
				return 0, injectedError
			}

			return copy(bs, buff[off:]), nil
		}
		_, err = OpenWith("edge_cases", nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	}

	opts := DefaultOptions().
		WithMaxActiveSnapshots(1).
		WithMaxNodeSize(MinNodeSize).
		WithFlushThld(1000)
	tree, err := Open("edge_cases", opts)
	require.NoError(t, err)
	require.Equal(t, uint64(0), tree.Ts())

	require.Nil(t, tree.warn("message", nil))
	require.Equal(t, ErrorMaxKVLenExceeded, tree.warn("%v", ErrorMaxKVLenExceeded))

	err = tree.Insert(make([]byte, tree.maxNodeSize), []byte{})
	require.Equal(t, ErrorMaxKVLenExceeded, err)

	_, _, _, err = tree.Get(nil)
	require.Equal(t, ErrIllegalArguments, err)

	for i := 0; i < 100; i++ {
		err = tree.Insert(make([]byte, 1), []byte{2})
		require.NoError(t, err)
	}

	tss, err := tree.History(make([]byte, 1), 0, true, 10)
	require.NoError(t, err)
	require.Len(t, tss, 10)

	tss, err = tree.History(make([]byte, 1), 0, false, 10)
	require.NoError(t, err)
	require.Len(t, tss, 10)

	err = tree.Sync()
	require.NoError(t, err)

	s1, err := tree.Snapshot()
	require.NoError(t, err)

	_, err = s1.History(make([]byte, 1), 0, false, 100)
	require.NoError(t, err)

	_, err = s1.History(make([]byte, 1), 101, false, 100)
	require.Equal(t, ErrOffsetOutOfRange, err)

	_, err = tree.Snapshot()
	require.Equal(t, ErrorToManyActiveSnapshots, err)

	err = tree.Close()
	require.Equal(t, ErrSnapshotsNotClosed, err)

	_, err = tree.CompactIndex()
	require.Equal(t, ErrSnapshotsNotClosed, err)

	err = s1.Close()
	require.NoError(t, err)

	for i := 1; i < 100; i++ {
		var k [4]byte
		binary.BigEndian.PutUint32(k[:], uint32(i))

		s1, err := tree.Snapshot()
		require.NoError(t, err)

		_, err = s1.History([]byte{2}, 0, false, 1)
		require.Equal(t, ErrKeyNotFound, err)

		err = s1.Close()
		require.NoError(t, err)

		err = tree.Insert(k[:], []byte{2})
		require.NoError(t, err)
	}

	require.NoError(t, tree.Close())
}

func monotonicInsertions(t *testing.T, tbtree *TBtree, itCount int, kCount int, ascMode bool) {
	for i := 0; i < itCount; i++ {
		for j := 0; j < kCount; j++ {
			k := make([]byte, 4)
			if ascMode {
				binary.BigEndian.PutUint32(k, uint32(j))
			} else {
				binary.BigEndian.PutUint32(k, uint32(kCount-j))
			}

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+j))

			ts := uint64(i*kCount+j) + 1

			snapshot, err := tbtree.Snapshot()
			require.NoError(t, err)
			snapshotTs := snapshot.Ts()
			require.Equal(t, ts-1, snapshotTs)

			v1, ts1, hc, err := snapshot.Get(k)

			if i == 0 {
				require.Equal(t, ErrKeyNotFound, err)
			} else {
				require.NoError(t, err)

				expectedV := make([]byte, 8)
				binary.BigEndian.PutUint64(expectedV, uint64((i-1)<<4+j))
				require.Equal(t, expectedV, v1)

				expectedTs := uint64((i-1)*kCount+j) + 1
				require.Equal(t, expectedTs, ts1)

				require.Equal(t, uint64(i), hc)
			}

			if j == kCount-1 {
				exists, err := tbtree.ExistKeyWith(k, k, false)
				require.NoError(t, err)
				require.False(t, exists)
			}

			if i%2 == 1 {
				err = snapshot.Close()
				require.NoError(t, err)
			}

			err = tbtree.Insert(k, v)
			require.NoError(t, err)

			_, _, err = tbtree.Flush()
			require.NoError(t, err)

			if i%2 == 0 {
				err = snapshot.Close()
				require.NoError(t, err)
			}

			snapshot, err = tbtree.Snapshot()
			require.NoError(t, err)
			snapshotTs = snapshot.Ts()
			require.Equal(t, ts, snapshotTs)

			v1, ts1, hc, err = snapshot.Get(k)

			require.NoError(t, err)
			require.Equal(t, v, v1)
			require.Equal(t, ts, ts1)
			require.Equal(t, uint64(i+1), hc)

			err = snapshot.Close()
			require.NoError(t, err)
		}
	}
}

func checkAfterMonotonicInsertions(t *testing.T, tbtree *TBtree, itCount int, kCount int, ascMode bool) {
	snapshot, err := tbtree.Snapshot()
	require.NoError(t, err)

	i := itCount

	for j := 0; j < kCount; j++ {
		k := make([]byte, 4)
		if ascMode {
			binary.BigEndian.PutUint32(k, uint32(j))
		} else {
			binary.BigEndian.PutUint32(k, uint32(kCount-j))
		}

		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i<<4+j))

		v1, ts1, hc1, err := snapshot.Get(k)

		require.NoError(t, err)

		expectedV := make([]byte, 8)
		binary.BigEndian.PutUint64(expectedV, uint64((i-1)<<4+j))
		require.Equal(t, expectedV, v1)

		expectedTs := uint64((i-1)*kCount+j) + 1
		require.Equal(t, expectedTs, ts1)

		require.Equal(t, uint64(itCount), hc1)
	}

	err = snapshot.Close()
	require.NoError(t, err)
}

func randomInsertions(t *testing.T, tbtree *TBtree, kCount int, override bool) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	for i := 0; i < kCount; i++ {
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k, rnd.Uint32())

		if !override {
			snapshot, err := tbtree.Snapshot()
			require.NoError(t, err)
			require.Equal(t, uint64(i), snapshot.Ts())

			for {
				_, _, _, err = snapshot.Get(k)
				if err == ErrKeyNotFound {

					exists, err := tbtree.ExistKeyWith(k, nil, false)
					require.NoError(t, err)
					require.False(t, exists)

					break
				}
				binary.BigEndian.PutUint32(k, rnd.Uint32())
			}

			err = snapshot.Close()
			require.NoError(t, err)
		}

		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i))

		ts := uint64(i + 1)

		err := tbtree.Insert(k, v)
		require.NoError(t, err)

		exists, err := tbtree.ExistKeyWith(k, nil, false)
		require.NoError(t, err)
		require.True(t, exists)

		v0, ts0, hc0, err := tbtree.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, v0)
		require.Equal(t, ts, ts0)
		if override {
			require.Greater(t, hc0, uint64(0))
		} else {
			require.Equal(t, uint64(1), hc0)
		}

		_, _, err = tbtree.Flush()
		require.NoError(t, err)

		snapshot, err := tbtree.Snapshot()
		require.NoError(t, err)
		snapshotTs := snapshot.Ts()
		require.Equal(t, ts, snapshotTs)

		v1, ts1, hc1, err := snapshot.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, v1)
		require.Equal(t, ts, ts1)
		if override {
			require.Greater(t, hc1, uint64(0))
		} else {
			require.Equal(t, uint64(1), hc1)
		}

		tss, err := snapshot.History(k, 0, true, 1)
		require.NoError(t, err)
		require.Equal(t, ts, tss[0])

		err = snapshot.Close()
		require.NoError(t, err)
	}
}

func TestInvalidOpening(t *testing.T) {
	_, err := Open("", nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = Open("tbtree_test.go", DefaultOptions())
	require.Equal(t, ErrorPathIsNotADirectory, err)

	_, err = OpenWith("tbtree_test", nil, nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = Open("invalid\x00_dir_name", DefaultOptions())
	require.EqualError(t, err, "stat invalid\x00_dir_name: invalid argument")

	require.NoError(t, os.MkdirAll("ro_path", 0500))
	defer os.RemoveAll("ro_path")

	_, err = Open("ro_path/subpath", DefaultOptions())
	require.EqualError(t, err, "mkdir ro_path/subpath: permission denied")

	for _, brokenPath := range []string{"nodes", "history", "commit"} {
		t.Run("error opening "+brokenPath, func(t *testing.T) {
			err = os.MkdirAll("test_broken_path", 0777)
			require.NoError(t, err)
			defer os.RemoveAll("test_broken_path")

			err = ioutil.WriteFile("test_broken_path/"+brokenPath, []byte{}, 0666)
			require.NoError(t, err)

			_, err = Open("test_broken_path", DefaultOptions())
			require.ErrorIs(t, err, multiapp.ErrorPathIsNotADirectory)
		})
	}
}

func TestTBTreeHistory(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxNodeSize(256).WithFlushThld(100)
	tbtree, err := Open("test_tree_history", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_history")

	err = tbtree.BulkInsert([]*KV{{K: []byte("k0"), V: []byte("v0")}})
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_history", opts)
	require.NoError(t, err)

	err = tbtree.BulkInsert([]*KV{{K: []byte("k0"), V: []byte("v00")}})
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_history", opts)
	require.NoError(t, err)

	tss, err := tbtree.History([]byte("k0"), 0, false, 10)
	require.NoError(t, err)
	require.Equal(t, 2, len(tss))
}

func TestTBTreeInsertionInAscendingOrder(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxNodeSize(256).WithFlushThld(100)
	tbtree, err := Open("test_tree_iasc", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_iasc")

	require.Equal(t, opts, tbtree.GetOptions())

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	itCount := 100
	keyCount := 100
	monotonicInsertions(t, tbtree, itCount, keyCount, true)

	err = tbtree.BulkInsert(nil)
	require.Equal(t, err, ErrIllegalArguments)

	err = tbtree.BulkInsert([]*KV{{}})
	require.Equal(t, err, ErrIllegalArguments)

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	_, err = tbtree.History(nil, 0, false, 10)
	require.Equal(t, err, ErrIllegalArguments)

	_, err = tbtree.History([]byte("key"), 0, false, 0)
	require.Equal(t, err, ErrIllegalArguments)

	exists, err := tbtree.ExistKeyWith([]byte("key"), []byte("longerkey"), false)
	require.NoError(t, err)
	require.False(t, exists)

	err = tbtree.Close()
	require.NoError(t, err)

	_, _, err = tbtree.Flush()
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.History([]byte("key"), 0, false, 10)
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Close()
	require.Equal(t, err, ErrAlreadyClosed)

	_, _, _, err = tbtree.Get([]byte("key"))
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.ExistKeyWith([]byte("key"), nil, false)
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Sync()
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Insert(nil, nil)
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.Snapshot()
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.CompactIndex()
	require.Equal(t, err, ErrAlreadyClosed)

	tbtree, err = Open("test_tree_iasc", DefaultOptions().WithMaxNodeSize(256))
	require.NoError(t, err)

	require.Equal(t, tbtree.root.ts(), uint64(itCount*keyCount))

	checkAfterMonotonicInsertions(t, tbtree, itCount, keyCount, true)
}

func TestTBTreeInsertionInDescendingOrder(t *testing.T) {
	tbtree, err := Open("test_tree_idesc", DefaultOptions().WithMaxNodeSize(256))
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_idesc")

	itCount := 10
	keyCount := 1000

	monotonicInsertions(t, tbtree, itCount, keyCount, false)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_idesc", DefaultOptions().WithMaxNodeSize(256))
	require.NoError(t, err)

	require.Equal(t, tbtree.root.ts(), uint64(itCount*keyCount))

	checkAfterMonotonicInsertions(t, tbtree, itCount, keyCount, false)

	snapshot, err := tbtree.Snapshot()
	require.NotNil(t, snapshot)
	require.NoError(t, err)

	rspec := &ReaderSpec{
		SeekKey:   []byte{},
		Prefix:    nil,
		DescOrder: false,
	}
	reader, err := snapshot.NewReader(rspec)
	require.NoError(t, err)

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

	err = reader.Close()
	require.NoError(t, err)

	err = snapshot.Close()
	require.NoError(t, err)

	err = tbtree.Insert(prevk, prevk)
	require.NoError(t, err)

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	snapshot, err = tbtree.Snapshot()
	require.NoError(t, err)

	v, ts, hc, err := snapshot.Get(prevk)
	require.NoError(t, err)
	require.Equal(t, uint64(itCount*keyCount+1), ts)
	require.Equal(t, prevk, v)
	require.Equal(t, uint64(itCount+1), hc)

	snapshot.Close()
}

func TestTBTreeInsertionInRandomOrder(t *testing.T) {
	opts := DefaultOptions().WithMaxNodeSize(DefaultMaxNodeSize).WithCacheSize(1000).WithSynced(false)
	tbtree, err := Open("test_tree_irnd", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_irnd")

	randomInsertions(t, tbtree, 10_000, true)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestRandomInsertionWithConcurrentReaderOrder(t *testing.T) {
	opts := DefaultOptions().WithMaxNodeSize(DefaultMaxNodeSize).WithCacheSize(1000)
	tbtree, err := Open("test_tree_c", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_c")

	keyCount := 1000

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		randomInsertions(t, tbtree, keyCount, false)
		wg.Done()
	}()

	for {
		snapshot, err := tbtree.Snapshot()
		require.NotNil(t, snapshot)
		require.NoError(t, err)

		rspec := &ReaderSpec{
			SeekKey:   []byte{},
			Prefix:    nil,
			DescOrder: false,
		}

		reader, err := snapshot.NewReader(rspec)
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			snapshot.Close()
			continue
		}

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

		reader.Close()
		snapshot.Close()

		if keyCount == i {
			break
		}
	}

	wg.Wait()

	err = tbtree.Close()
	require.NoError(t, err)
}

func BenchmarkRandomInsertion(b *testing.B) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	for i := 0; i < b.N; i++ {
		opts := DefaultOptions().
			WithMaxNodeSize(DefaultMaxNodeSize).
			WithCacheSize(10_000).
			WithSynced(false).
			WithFlushThld(100_000)

		tbtree, _ := Open("test_tree_brnd", opts)
		defer os.RemoveAll("test_tree_brnd")

		kCount := 1_000_000

		for i := 0; i < kCount; i++ {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, rnd.Uint32())

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			tbtree.Insert(k, v)
		}

		tbtree.Close()
	}
}
