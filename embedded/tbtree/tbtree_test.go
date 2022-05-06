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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"

	"github.com/stretchr/testify/require"
)

func TestEdgeCases(t *testing.T) {
	path, err := ioutil.TempDir("", "test_tree_edge_cases")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	_, err = Open(path, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = OpenWith(path, nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	nLog := &mocked.MockedAppendable{}
	hLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	injectedError := errors.New("error")

	t.Run("Should fail reading maxNodeSize from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			return nil
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, ErrCorruptedCLog)
	})

	t.Run("Should fail reading version from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(MetaVersion, 1)
			return md.Bytes()
		}

		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, ErrIncompatibleDataFormat)
	})

	t.Run("Should fail reading cLogSize", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(MetaVersion, Version)
			md.PutInt(MetaMaxNodeSize, requiredNodeSize(1, 1))
			md.PutInt(MetaMaxKeySize, 1)
			md.PutInt(MetaMaxValueSize, 1)
			return md.Bytes()
		}

		cLog.SizeFn = func() (int64, error) {
			return 0, injectedError
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail truncating clog", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize + 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return injectedError
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should succeed", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(MetaVersion, Version)
			md.PutInt(MetaMaxNodeSize, requiredNodeSize(1, 1))
			md.PutInt(MetaMaxKeySize, 1)
			md.PutInt(MetaMaxValueSize, 1)
			return md.Bytes()
		}
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize - 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return nil
		}
		hLog.SetOffsetFn = func(off int64) error {
			return nil
		}
		hLog.OffsetFn = func() int64 {
			return 0
		}
		hLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.NoError(t, err)
	})

	t.Run("Should fail when unable to read from cLog", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize, nil
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedError
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail when unable to read current root node type", func(t *testing.T) {
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			require.EqualValues(t, 0, off)
			require.Len(t, bs, cLogEntrySize)
			for i := range bs {
				bs[i] = 0
			}
			binary.BigEndian.PutUint64(bs[8:], 1)  // set final size
			binary.BigEndian.PutUint32(bs[16:], 1) // set a min node size
			return len(bs), nil
		}
		nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedError
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Invalid root node type", func(t *testing.T) {
		nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			nLogBuffer := []byte{0xFF, 0, 0, 0, 0}
			require.Less(t, off, int64(len(nLogBuffer)))
			l := copy(bs, nLogBuffer[off:])
			return l, nil
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			binary.BigEndian.PutUint64(bs[8:], 1)  // set final size
			binary.BigEndian.PutUint32(bs[16:], 1) // set a min node size

			nLogChecksum, err := appendable.Checksum(nLog, 0, 1)
			copy(bs[20:], nLogChecksum[:])

			hLogChecksum := sha256.Sum256(nil)
			copy(bs[68:], hLogChecksum[:])

			return len(bs), err
		}
		_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, ErrReadingFileContent)
	})

	t.Run("Error while reading a single leaf node content", func(t *testing.T) {
		nLogBuffer := []byte{
			LeafNodeType, // Node type
			0, 1,         // 1 child
			0, 1, // key size
			123,  // key
			0, 1, // value size
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
			_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
			require.ErrorIs(t, err, injectedError)
		}
	})

	t.Run("Error while reading an inner node content", func(t *testing.T) {
		nLogBuffer := []byte{
			InnerNodeType, // Node type
			0, 1,          // 1 child
			0, 1, // min key size
			0,                      // min key
			0, 0, 0, 0, 0, 0, 0, 0, // Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // offset
			0, 0, 0, 0, 0, 0, 0, 0, // min offset
		}
		for i := 1; i < len(nLogBuffer); i++ {
			injectedError := fmt.Errorf("Injected error %d", i)
			buff := nLogBuffer[:i]

			cLog.MetadataFn = func() []byte {
				md := appendable.NewMetadata(nil)
				md.PutInt(MetaVersion, Version)
				md.PutInt(MetaMaxNodeSize, requiredNodeSize(1, 1))
				md.PutInt(MetaMaxKeySize, 1)
				md.PutInt(MetaMaxValueSize, 1)
				return md.Bytes()
			}

			nLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
				if off >= int64(len(buff)) {
					return 0, injectedError
				}

				return copy(bs, buff[off:]), nil
			}

			_, err = OpenWith(path, nLog, hLog, cLog, DefaultOptions())
			require.ErrorIs(t, err, injectedError)
		}
	})

	opts := DefaultOptions().
		WithMaxActiveSnapshots(1).
		WithFlushThld(1000)
	tree, err := Open(path, opts)
	require.NoError(t, err)
	require.Equal(t, uint64(0), tree.Ts())

	require.Error(t, tree.wrapNwarn("message"))
	require.ErrorIs(t, tree.wrapNwarn("%w", ErrorMaxKeySizeExceeded), ErrorMaxKeySizeExceeded)
	require.ErrorIs(t, tree.wrapNwarn("%w", ErrorMaxValueSizeExceeded), ErrorMaxValueSizeExceeded)

	err = tree.Insert(make([]byte, tree.maxKeySize+1), make([]byte, tree.maxValueSize))
	require.Equal(t, ErrorMaxKeySizeExceeded, err)

	err = tree.Insert(make([]byte, tree.maxKeySize), make([]byte, tree.maxValueSize+1))
	require.Equal(t, ErrorMaxValueSizeExceeded, err)

	_, _, _, err = tree.Get(nil)
	require.Equal(t, ErrIllegalArguments, err)

	for i := 0; i < 100; i++ {
		err = tree.Insert(make([]byte, 1), []byte{2})
		require.NoError(t, err)
	}

	tss, hCount, err := tree.History(make([]byte, 1), 0, true, 10)
	require.NoError(t, err)
	require.Len(t, tss, 10)
	require.EqualValues(t, 100, hCount)

	tss, hCount, err = tree.History(make([]byte, 1), 0, false, 10)
	require.NoError(t, err)
	require.Len(t, tss, 10)
	require.EqualValues(t, 100, hCount)

	err = tree.Sync()
	require.NoError(t, err)

	s1, err := tree.Snapshot()
	require.NoError(t, err)

	_, _, err = s1.History(make([]byte, 1), 0, false, 100)
	require.NoError(t, err)

	_, _, err = s1.History(make([]byte, 1), 101, false, 100)
	require.Equal(t, ErrOffsetOutOfRange, err)

	_, err = tree.Snapshot()
	require.Equal(t, ErrorToManyActiveSnapshots, err)

	err = tree.Close()
	require.Equal(t, ErrSnapshotsNotClosed, err)

	err = s1.Close()
	require.NoError(t, err)

	for i := 1; i < 100; i++ {
		var k [4]byte
		binary.BigEndian.PutUint32(k[:], uint32(i))

		s1, err := tree.Snapshot()
		require.NoError(t, err)

		_, _, err = s1.History([]byte{2}, 0, false, 1)
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
				exists, err := tbtree.ExistKeyWith(k, k)
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

					exists, err := tbtree.ExistKeyWith(k, nil)
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

		exists, err := tbtree.ExistKeyWith(k, nil)
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

		tss, _, err := snapshot.History(k, 0, true, 1)
		require.NoError(t, err)
		require.Equal(t, ts, tss[0])

		err = snapshot.Close()
		require.NoError(t, err)
	}
}

func TestInvalidOpening(t *testing.T) {
	_, err := Open("", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = Open("tbtree_test.go", DefaultOptions())
	require.ErrorIs(t, err, ErrorPathIsNotADirectory)

	_, err = OpenWith("tbtree_test", nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

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

func TestSnapshotRecovery(t *testing.T) {
	d, err := ioutil.TempDir("", "test_tree_recovery")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	// Starting with some historical garbage
	hpath := filepath.Join(d, historyFolder)
	happ, err := multiapp.Open(hpath, multiapp.DefaultOptions())
	require.NoError(t, err)

	_, _, err = happ.Append([]byte{1, 2, 3})
	require.NoError(t, err)

	err = happ.Close()
	require.NoError(t, err)

	// Starting with an invalid folder name
	os.MkdirAll(filepath.Join(d, fmt.Sprintf("%s1z", commitFolderPrefix)), 0777)

	tree, err := Open(d, DefaultOptions().WithCompactionThld(1))
	require.NoError(t, err)

	snapc, err := tree.SnapshotCount()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snapc)

	err = tree.BulkInsert([]*KV{
		{K: []byte("key1"), V: []byte("value1")},
	})
	require.NoError(t, err)

	_, err = tree.Compact()
	require.ErrorIs(t, err, ErrCompactionThresholdNotReached)

	_, _, err = tree.Flush()
	require.NoError(t, err)

	c, err := tree.Compact()
	require.NoError(t, err)
	require.Equal(t, uint64(1), c)

	snapc, err = tree.SnapshotCount()
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapc)

	err = tree.BulkInsert([]*KV{
		{K: []byte("key2"), V: []byte("value2")},
		{K: []byte("key3"), V: []byte("value3")},
	})
	require.NoError(t, err)

	err = tree.BulkInsert([]*KV{
		{K: []byte("key4"), V: []byte("value4")},
	})
	require.NoError(t, err)

	c, err = tree.Compact()
	require.NoError(t, err)
	require.Equal(t, uint64(3), c)

	_, _, err = tree.Flush()
	require.NoError(t, err)

	snapc, err = tree.SnapshotCount()
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapc)

	err = tree.Close()
	require.NoError(t, err)

	_, err = tree.SnapshotCount()
	require.Equal(t, err, ErrAlreadyClosed)

	tree, err = Open(d, DefaultOptions())
	require.NoError(t, err)

	snapc, err = tree.SnapshotCount()
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapc)

	err = tree.Close()
	require.NoError(t, err)

	os.RemoveAll(filepath.Join(d, snapFolder(nodesFolderPrefix, c)))

	tree, err = Open(d, DefaultOptions())
	require.NoError(t, err)

	snapc, err = tree.SnapshotCount()
	require.NoError(t, err)
	require.Equal(t, uint64(0), snapc)

	err = tree.Close()
	require.NoError(t, err)

	injectedError := errors.New("factory error")

	metaFaultyAppFactory := func(prefix string) AppFactoryFunc {
		return func(
			rootPath string,
			subPath string,
			opts *multiapp.Options,
		) (appendable.Appendable, error) {
			if strings.HasPrefix(subPath, prefix) {
				return nil, injectedError
			}

			path := filepath.Join(rootPath, subPath)
			return multiapp.Open(path, opts)
		}
	}

	t.Run("Should fail opening hLog", func(t *testing.T) {
		_, err = Open(d, DefaultOptions().WithAppFactory(metaFaultyAppFactory(historyFolder)))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail opening nLog", func(t *testing.T) {
		_, err = Open(d, DefaultOptions().WithAppFactory(metaFaultyAppFactory(nodesFolderPrefix)))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail opening cLog", func(t *testing.T) {
		_, err = Open(d, DefaultOptions().WithAppFactory(metaFaultyAppFactory(commitFolderPrefix)))
		require.ErrorIs(t, err, injectedError)
	})
}

func TestTBTreeSplitTooBigKeys(t *testing.T) {
	d, err := ioutil.TempDir("", "test_tree_split_too_big_keys")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	opts := DefaultOptions()

	opts.WithMaxKeySize(opts.maxNodeSize / 2)
	_, err = Open(d, opts)
	require.ErrorIs(t, err, ErrIllegalArguments)

	opts.WithMaxKeySize(requiredNodeSize(opts.maxKeySize, opts.maxValueSize) - 1)
	_, err = Open(d, opts)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestTBTreeSplitWithKeyUpdates(t *testing.T) {
	d, err := ioutil.TempDir("", "test_tree_split_key_updates")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	opts := DefaultOptions()

	tree, err := Open(d, opts)
	require.NoError(t, err)

	for i := byte(0); i < 14; i++ {
		key := make([]byte, opts.maxKeySize/4)
		key[0] = i

		err = tree.BulkInsert([]*KV{
			{K: key, V: make([]byte, 1)},
		})
		require.NoError(t, err)
	}

	// updating entries with bigger values should be handled
	for i := byte(0); i < 14; i++ {
		key := make([]byte, opts.maxKeySize/4)
		key[0] = i

		err = tree.BulkInsert([]*KV{
			{K: key, V: key},
		})
		require.NoError(t, err)
	}

	_, _, err = tree.Flush()
	require.NoError(t, err)

	err = tree.Close()
	require.NoError(t, err)
}

func TestTBTreeSplitMultiLeafSplit(t *testing.T) {
	d, err := ioutil.TempDir("", "test_tree_multi_leaf_split")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	opts := DefaultOptions()
	opts.WithMaxKeySize(opts.maxNodeSize / 4)

	tree, err := Open(d, opts)
	require.NoError(t, err)

	for i := byte(1); i < 4; i++ {
		key := make([]byte, opts.maxKeySize)
		key[0] = i

		err = tree.BulkInsert([]*KV{
			{K: key, V: make([]byte, 1)},
		})
		require.NoError(t, err)
	}

	for i := byte(1); i < 5; i++ {
		key := make([]byte, opts.maxKeySize/8)
		key[0] = i + 3

		err = tree.BulkInsert([]*KV{
			{K: key, V: make([]byte, 1)},
		})
		require.NoError(t, err)
	}

	key := make([]byte, opts.maxKeySize)

	err = tree.BulkInsert([]*KV{
		{K: key, V: make([]byte, opts.maxValueSize)},
	})
	require.NoError(t, err)

	_, _, err = tree.Flush()
	require.NoError(t, err)

	err = tree.Close()
	require.NoError(t, err)
}

func TestTBTreeCompactionEdgeCases(t *testing.T) {
	d, err := ioutil.TempDir("", "test_tree_compaction_edge_cases")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	tree, err := Open(d, DefaultOptions())
	require.NoError(t, err)

	err = tree.BulkInsert([]*KV{{K: []byte("k0"), V: []byte("v0")}})
	require.NoError(t, err)

	snap, err := tree.Snapshot()
	require.NoError(t, err)

	injectedError := errors.New("error")

	nLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	t.Run("Should fail while dumping the snapshot", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail while appending to cLog", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		nLog.FlushFn = func() error {
			return nil
		}
		nLog.SyncFn = func() error {
			return nil
		}
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail while flushing nLog", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		nLog.FlushFn = func() error {
			return injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail while syncing nLog", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		nLog.FlushFn = func() error {
			return nil
		}
		nLog.SyncFn = func() error {
			return injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail while flushing cLog", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		nLog.FlushFn = func() error {
			return nil
		}
		nLog.SyncFn = func() error {
			return nil
		}
		cLog.FlushFn = func() error {
			return injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("Should fail while syncing cLog", func(t *testing.T) {
		nLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, len(bs), nil
		}
		nLog.FlushFn = func() error {
			return nil
		}
		nLog.SyncFn = func() error {
			return nil
		}
		cLog.FlushFn = func() error {
			return nil
		}
		cLog.SyncFn = func() error {
			return injectedError
		}
		err = tree.fullDumpTo(snap, nLog, cLog, func(int, int, int) {})
		require.ErrorIs(t, err, injectedError)
	})
}

func TestTBTreeHistory(t *testing.T) {
	opts := DefaultOptions().WithFlushThld(100)
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

	tss, hCount, err := tbtree.History([]byte("k0"), 0, false, 10)
	require.NoError(t, err)
	require.Equal(t, 2, len(tss))
	require.EqualValues(t, 2, hCount)
}

func TestTBTreeInsertionInAscendingOrder(t *testing.T) {
	opts := DefaultOptions().WithFlushThld(100)
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

	_, _, err = tbtree.History(nil, 0, false, 10)
	require.Equal(t, err, ErrIllegalArguments)

	_, _, err = tbtree.History([]byte("key"), 0, false, 0)
	require.Equal(t, err, ErrIllegalArguments)

	exists, err := tbtree.ExistKeyWith([]byte("key"), []byte("longerkey"))
	require.NoError(t, err)
	require.False(t, exists)

	err = tbtree.Close()
	require.NoError(t, err)

	_, _, err = tbtree.Flush()
	require.Equal(t, err, ErrAlreadyClosed)

	_, _, err = tbtree.History([]byte("key"), 0, false, 10)
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Close()
	require.Equal(t, err, ErrAlreadyClosed)

	_, _, _, err = tbtree.Get([]byte("key"))
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.ExistKeyWith([]byte("key"), nil)
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Sync()
	require.Equal(t, err, ErrAlreadyClosed)

	err = tbtree.Insert([]byte("key"), []byte("value"))
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.Snapshot()
	require.Equal(t, err, ErrAlreadyClosed)

	_, err = tbtree.Compact()
	require.Equal(t, err, ErrAlreadyClosed)

	tbtree, err = Open("test_tree_iasc", DefaultOptions())
	require.NoError(t, err)

	require.Equal(t, tbtree.root.ts(), uint64(itCount*keyCount))

	checkAfterMonotonicInsertions(t, tbtree, itCount, keyCount, true)
}

func TestTBTreeInsertionInDescendingOrder(t *testing.T) {
	tbtree, err := Open("test_tree_idesc", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_idesc")

	itCount := 10
	keyCount := 1000

	monotonicInsertions(t, tbtree, itCount, keyCount, false)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_idesc", DefaultOptions())
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
	opts := DefaultOptions().WithCacheSize(1000)
	tbtree, err := Open("test_tree_irnd", opts)
	require.NoError(t, err)
	defer os.RemoveAll("test_tree_irnd")

	randomInsertions(t, tbtree, 10_000, true)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestRandomInsertionWithConcurrentReaderOrder(t *testing.T) {
	opts := DefaultOptions().WithCacheSize(1000)
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

func TestTBTreeReOpen(t *testing.T) {
	tbtree, err := Open("test_tree_reopen", DefaultOptions())
	require.NoError(t, err)

	defer os.RemoveAll("test_tree_reopen")

	err = tbtree.Insert([]byte("k0"), []byte("v0"))
	require.NoError(t, err)

	_, _, err = tbtree.Flush()
	require.NoError(t, err)

	err = tbtree.Insert([]byte("k1"), []byte("v1"))
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)

	t.Run("reopening btree after gracefully close should read all data", func(t *testing.T) {
		tbtree, err := Open("test_tree_reopen", DefaultOptions())
		require.NoError(t, err)

		_, _, _, err = tbtree.Get([]byte("k0"))
		require.NoError(t, err)

		_, _, _, err = tbtree.Get([]byte("k1"))
		require.NoError(t, err)

		err = tbtree.Close()
		require.NoError(t, err)
	})
}

func TestTBTreeSelfHealingHistory(t *testing.T) {
	tbtree, err := Open("test_tree_self_healing_history", DefaultOptions())
	require.NoError(t, err)

	defer os.RemoveAll("test_tree_self_healing_history")

	err = tbtree.Insert([]byte("k0"), []byte("v0"))
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)

	os.RemoveAll("test_tree_self_healing_history/history")

	tbtree, err = Open("test_tree_self_healing_history", DefaultOptions())
	require.NoError(t, err)

	_, _, _, err = tbtree.Get([]byte("k0"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_self_healing_history", DefaultOptions())
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestTBTreeSelfHealingNodes(t *testing.T) {
	tbtree, err := Open("test_tree_self_healing_nodes", DefaultOptions())
	require.NoError(t, err)

	defer os.RemoveAll("test_tree_self_healing_nodes")

	err = tbtree.Insert([]byte("k0"), []byte("v0"))
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)

	os.RemoveAll("test_tree_self_healing_nodes/nodes")

	tbtree, err = Open("test_tree_self_healing_nodes", DefaultOptions())
	require.NoError(t, err)

	_, _, _, err = tbtree.Get([]byte("k0"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tbtree.Close()
	require.NoError(t, err)

	tbtree, err = Open("test_tree_self_healing_nodes", DefaultOptions())
	require.NoError(t, err)

	err = tbtree.Close()
	require.NoError(t, err)
}

func TestTBTreeIncreaseTs(t *testing.T) {
	tbtree, err := Open("test_tree_increase_ts", DefaultOptions().WithFlushThld(2))
	require.NoError(t, err)

	defer os.RemoveAll("test_tree_increase_ts")

	require.Equal(t, uint64(0), tbtree.Ts())

	err = tbtree.Insert([]byte("k0"), []byte("v0"))
	require.NoError(t, err)

	require.Equal(t, uint64(1), tbtree.Ts())

	err = tbtree.IncreaseTs(tbtree.Ts() - 1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = tbtree.IncreaseTs(tbtree.Ts())
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = tbtree.IncreaseTs(tbtree.Ts() + 1)
	require.NoError(t, err)

	err = tbtree.IncreaseTs(tbtree.Ts() + 1)
	require.NoError(t, err)

	require.Equal(t, uint64(3), tbtree.Ts())

	err = tbtree.Close()
	require.NoError(t, err)

	err = tbtree.IncreaseTs(tbtree.Ts() + 1)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func BenchmarkRandomInsertion(b *testing.B) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	for i := 0; i < b.N; i++ {
		opts := DefaultOptions().
			WithCacheSize(10_000).
			WithFlushThld(100_000).
			WithSyncThld(1_000_000)

		tbtree, _ := Open("test_tree_brnd", opts)
		defer os.RemoveAll("test_tree_brnd")

		kCount := 1_000_000

		for i := 0; i < kCount; i++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint32(k, rnd.Uint32())

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			tbtree.Insert(k, v)
		}

		tbtree.Close()
	}
}

func BenchmarkRandomRead(b *testing.B) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	opts := DefaultOptions().
		WithCacheSize(100_000).
		WithFlushThld(100_000)

	tbtree, _ := Open("test_tree_brnd", opts)
	defer os.RemoveAll("test_tree_brnd")

	kCount := 1_000_000

	for i := 0; i < kCount; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))

		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i))

		tbtree.Insert(k, v)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, rnd.Uint64()%uint64(kCount))
		tbtree.Get(k)
	}

	tbtree.Close()
}

func BenchmarkAscendingBulkInsertion(b *testing.B) {
	opts := DefaultOptions().
		WithCacheSize(100_000).
		WithFlushThld(100_000).
		WithSyncThld(1_000_000)

	tbtree, err := Open("bench_tree_asc", opts)
	if err != nil {
		panic(err)
	}

	defer tbtree.Close()

	defer os.RemoveAll("bench_tree_asc")

	kBulkCount := 1000
	kBulkSize := 1000
	ascMode := true

	for i := 0; i < b.N; i++ {
		err = bulkInsert(tbtree, kBulkCount, kBulkSize, ascMode)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDescendingBulkInsertion(b *testing.B) {
	opts := DefaultOptions().
		WithCacheSize(10_000).
		WithFlushThld(100_000).
		WithSyncThld(1_000_000)

	tbtree, err := Open("bench_tree_asc", opts)
	if err != nil {
		panic(err)
	}

	defer tbtree.Close()

	defer os.RemoveAll("bench_tree_asc")

	kBulkCount := 1000
	kBulkSize := 1000
	ascMode := false

	for i := 0; i < b.N; i++ {
		err = bulkInsert(tbtree, kBulkCount, kBulkSize, ascMode)
		if err != nil {
			panic(err)
		}
	}
}

func bulkInsert(tbtree *TBtree, bulkCount, bulkSize int, asc bool) error {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	kvs := make([]*KV, bulkSize)

	for i := 0; i < bulkCount; i++ {
		for j := 0; j < bulkSize; j++ {
			key := make([]byte, 8)
			if asc {
				binary.BigEndian.PutUint64(key, uint64(i*bulkSize+j))
			} else {
				binary.BigEndian.PutUint64(key, uint64((bulkCount-i)*bulkSize-j))
			}

			value := make([]byte, 32)
			rnd.Read(value)

			kvs[j] = &KV{K: key, V: value}
		}

		err := tbtree.BulkInsert(kvs)
		if err != nil {
			return err
		}
	}

	return nil
}

func BenchmarkRandomBulkInsertion(b *testing.B) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	for i := 0; i < b.N; i++ {
		opts := DefaultOptions().
			WithCacheSize(1000).
			WithFlushThld(100_000).
			WithSyncThld(1_000_000).
			WithFlushBufferSize(DefaultFlushBufferSize * 4)

		tbtree, err := Open("test_tree_brnd", opts)
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll("test_tree_brnd")

		kBulkCount := 1000
		kBulkSize := 1000

		kvs := make([]*KV, kBulkSize)

		for i := 0; i < kBulkCount; i++ {
			for j := 0; j < kBulkSize; j++ {
				k := make([]byte, 8)
				v := make([]byte, 32)

				rnd.Read(k)
				rnd.Read(v)

				kvs[j] = &KV{K: k, V: v}
			}

			err = tbtree.BulkInsert(kvs)
			if err != nil {
				panic(err)
			}
		}

		tbtree.Close()
	}
}

func TestLastUpdateBetween(t *testing.T) {
	tbtree, err := Open("test_tree_last_update", DefaultOptions())
	require.NoError(t, err)

	defer os.RemoveAll("test_tree_last_update")

	keyUpdatesCount := 32

	for i := 0; i < keyUpdatesCount; i++ {
		err = tbtree.Insert([]byte("key1"), []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)
	}

	_, leaf, off, err := tbtree.root.findLeafNode([]byte("key1"), nil, 0, nil, false)
	require.NoError(t, err)
	require.NotNil(t, leaf)
	require.GreaterOrEqual(t, len(leaf.values), off)

	_, _, err = leaf.values[off].lastUpdateBetween(nil, 1, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	for i := 0; i < keyUpdatesCount; i++ {
		for f := i; f < keyUpdatesCount; f++ {
			tx, hc, err := leaf.values[off].lastUpdateBetween(nil, uint64(i+1), uint64(f+1))
			require.NoError(t, err)
			require.Equal(t, uint64(f), hc)
			require.Equal(t, uint64(f+1), tx)
		}
	}
}
