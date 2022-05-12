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
package ahtree

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"

	"github.com/stretchr/testify/require"
)

func TestNodeNumberCalculation(t *testing.T) {
	var nodesUptoTests = []struct {
		n        uint64
		expected uint64
	}{
		{1, 1},
		{2, 3},
		{3, 5},
		{4, 8},
		{5, 10},
		{6, 13},
		{7, 16},
		{8, 20},
		{9, 22},
		{10, 25},
		{11, 28},
		{12, 32},
		{13, 35},
		{14, 39},
		{15, 43},
		{16, 48},
	}

	for _, tt := range nodesUptoTests {
		actual := nodesUpto(tt.n)
		require.Equal(t, tt.expected, actual)

		require.Equal(t, tt.expected, nodesUntil(tt.n)+uint64(levelsAt(tt.n))+1)
	}
}

func TestEdgeCases(t *testing.T) {

	dummySetOffset := func(off int64) error {
		return nil
	}
	dummyAppend := func() func([]byte) (int64, int, error) {
		written := 0
		return func(bs []byte) (int64, int, error) {
			offs := written
			written += len(bs)
			return int64(offs), len(bs), nil
		}
	}
	dummyFlush := func() error {
		return nil
	}

	pLog := &mocked.MockedAppendable{SetOffsetFn: dummySetOffset}
	dLog := &mocked.MockedAppendable{SetOffsetFn: dummySetOffset}
	cLog := &mocked.MockedAppendable{SetOffsetFn: dummySetOffset}

	injectedErr := errors.New("error")

	t.Run("should fail on illegal arguments", func(t *testing.T) {
		_, err := Open("ahtree_test", nil)
		require.Equal(t, ErrIllegalArguments, err)

		_, err = OpenWith(nil, nil, nil, nil)
		require.Equal(t, ErrIllegalArguments, err)

		_, err = OpenWith(nil, nil, nil, DefaultOptions())
		require.Equal(t, ErrIllegalArguments, err)
	})

	t.Run("should fail while querying cLog size", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return 0, injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail while setting cLog offset", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize - 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail while setting pLog offset", func(t *testing.T) {
		cLog.SetOffsetFn = dummySetOffset
		cLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		pLog.SetOffsetFn = func(off int64) error {
			return injectedErr
		}
		pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			require.Fail(t, "Should fail before appending data")
			return 0, 0, nil
		}
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail while appending payload", func(t *testing.T) {
		cLog.SetOffsetFn = dummySetOffset
		pLog.SetOffsetFn = dummySetOffset
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize - 1, nil
		}

		t.Run("writing length", func(t *testing.T) {
			pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
				return 0, 0, injectedErr
			}

			tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
			require.NoError(t, err)

			_, _, err = tree.Append([]byte{1, 2, 3})
			require.ErrorIs(t, err, injectedErr)
		})

		t.Run("writing data", func(t *testing.T) {
			written := 0
			pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
				off = int64(written)
				written += len(bs)
				if written > 4 {
					return 0, 0, injectedErr
				}
				return off, len(bs), nil
			}
			tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
			require.NoError(t, err)

			_, _, err = tree.Append([]byte{1, 2, 3})
			require.ErrorIs(t, err, injectedErr)
		})
	})

	t.Run("should fail flushing pLog", func(t *testing.T) {
		pLog.AppendFn = dummyAppend()
		pLog.FlushFn = func() error {
			return injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail on dLog SetOffset", func(t *testing.T) {
		pLog.FlushFn = dummyFlush
		dLog.AppendFn = dummyAppend()
		dLog.SetOffsetFn = func(off int64) error {
			return injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail flushing dLog", func(t *testing.T) {
		dLog.SetOffsetFn = dummySetOffset
		dLog.FlushFn = func() error {
			return injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail on cLog SetOffset during append", func(t *testing.T) {
		dLog.FlushFn = dummyFlush

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		cLog.SetOffsetFn = func(off int64) error {
			return injectedErr
		}

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail writing cLog", func(t *testing.T) {
		cLog.SetOffsetFn = dummySetOffset
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail flushing cLog", func(t *testing.T) {
		cLog.AppendFn = dummyAppend()
		cLog.FlushFn = func() error {
			return injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail calculating hashes on append", func(t *testing.T) {
		cLog.FlushFn = dummyFlush
		dLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.NoError(t, err)

		tree.dCache.Pop(uint64(0))

		_, _, err = tree.Append([]byte{4, 5, 6})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail while validating plog size", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize + 1, nil
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			binary.BigEndian.PutUint64(bs[:], 0)
			binary.BigEndian.PutUint32(bs[offsetSize:], 8)
			return cLogEntrySize, nil
		}
		pLog.SizeFn = func() (int64, error) {
			return 0, nil
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.Equal(t, ErrorCorruptedData, err)
	})

	t.Run("should fail while validating dLog size", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize + 1, nil
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			binary.BigEndian.PutUint64(bs[:], 0)
			binary.BigEndian.PutUint32(bs[offsetSize:], 8)
			return cLogEntrySize, nil
		}
		pLog.SizeFn = func() (int64, error) {
			return 8, nil
		}
		dLog.SizeFn = func() (int64, error) {
			return 0, nil
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.Equal(t, ErrorCorruptedDigests, err)
	})

	t.Run("should fail reading dLog size", func(t *testing.T) {
		dLog.SizeFn = func() (int64, error) {
			return 0, injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail reading pLog size", func(t *testing.T) {
		pLog.SizeFn = func() (int64, error) {
			return 0, injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail reading last cLog entry", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		dLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		pLog.SizeFn = func() (int64, error) {
			return 0, nil
		}

		metadata := appendable.NewMetadata(nil)
		metadata.PutInt(MetaVersion, Version)

		pLog.MetadataFn = metadata.Bytes
		dLog.MetadataFn = metadata.Bytes
		cLog.MetadataFn = metadata.Bytes

		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize, nil
		}

		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail reading pLog size", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySize, nil
		}
		pLog.SizeFn = func() (int64, error) {
			return 0, injectedErr
		}

		_, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail flushing pLog", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		pLog.SizeFn = func() (int64, error) {
			return 0, nil
		}
		pLog.SetOffsetFn = func(off int64) error {
			return nil
		}
		pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, nil
		}
		pLog.FlushFn = func() error {
			return injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail appending to dLog", func(t *testing.T) {
		pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, nil
		}
		pLog.FlushFn = func() error {
			return nil
		}
		dLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			return 0, 0, injectedErr
		}

		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		_, _, err = tree.Append(nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, _, err = tree.Append([]byte{1, 2, 3})
		require.ErrorIs(t, err, injectedErr)
	})

	t.Run("should fail due to invalid path", func(t *testing.T) {
		_, err := Open("options.go", DefaultOptions())
		require.Equal(t, ErrorPathIsNotADirectory, err)
	})

	t.Run("should fail due to invalid cache size", func(t *testing.T) {
		_, err := Open("ahtree_test", DefaultOptions().WithDataCacheSlots(-1))
		require.Equal(t, ErrIllegalArguments, err)
	})

	t.Run("should fail due to invalid digests cache size", func(t *testing.T) {
		_, err := Open("ahtree_test", DefaultOptions().WithDigestsCacheSlots(-1))
		require.Equal(t, ErrIllegalArguments, err)
	})

	dir, err := ioutil.TempDir("", "ahtree_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	tree, err := Open(dir, DefaultOptions().WithSynced(false))
	require.NoError(t, err)

	t.Run("should fail to get tree root when tree is empty", func(t *testing.T) {
		_, _, err := tree.Root()
		require.Equal(t, ErrEmptyTree, err)

		_, err = tree.rootAt(1)
		require.Equal(t, ErrEmptyTree, err)

		_, err = tree.rootAt(0)
		require.Equal(t, ErrIllegalArguments, err)
	})

	t.Run("should fail to get data when tree is empty", func(t *testing.T) {
		_, err := tree.DataAt(0)
		require.Equal(t, ErrIllegalArguments, err)
	})

	t.Run("should not error when syncing empty tree", func(t *testing.T) {
		err := tree.Sync()
		require.NoError(t, err)
	})

	t.Run("should fail on inclusion proof for non-existing root node", func(t *testing.T) {
		_, err := tree.InclusionProof(1, 2)
		require.ErrorIs(t, err, ErrUnexistentData)
	})

	_, _, err = tree.Append([]byte{1})
	require.NoError(t, err)

	t.Run("should fail after close", func(t *testing.T) {

		err := tree.Close()
		require.NoError(t, err)

		_, _, err = tree.Append(nil)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tree.InclusionProof(1, 2)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tree.ConsistencyProof(1, 2)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, _, err = tree.Root()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tree.rootAt(1)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tree.DataAt(1)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		err = tree.Sync()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		err = tree.ResetSize(0)
		require.ErrorIs(t, err, ErrAlreadyClosed)

		err = tree.Close()
		require.Equal(t, ErrAlreadyClosed, err)
	})
}

func TestReadOnly(t *testing.T) {
	_, err := Open("ahtree_test", DefaultOptions().WithReadOnly(true))
	defer os.RemoveAll("ahtree_test")
	require.Error(t, err)

	tree, err := Open("ahtree_test", DefaultOptions().WithReadOnly(false))
	require.NoError(t, err)
	err = tree.Close()
	require.NoError(t, err)

	tree, err = Open("ahtree_test", DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	_, _, err = tree.Append(nil)
	require.Equal(t, ErrReadOnly, err)

	err = tree.Sync()
	require.Equal(t, ErrReadOnly, err)

	err = tree.Close()
	require.NoError(t, err)
}

func TestAppend(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithDigestsCacheSlots(100).WithDataCacheSlots(100)
	tree, err := Open("ahtree_test", opts)
	require.NoError(t, err)
	defer os.RemoveAll("ahtree_test")

	N := 1024

	for i := 1; i <= N; i++ {
		p := []byte{byte(i)}

		_, _, err := tree.Append(p)
		require.NoError(t, err)

		ri, err := tree.RootAt(uint64(i))
		require.NoError(t, err)

		n, r, err := tree.Root()
		require.NoError(t, err)
		require.Equal(t, uint64(i), n)
		require.Equal(t, r, ri)

		sz := tree.Size()
		require.Equal(t, uint64(i), sz)

		rp, err := tree.DataAt(uint64(i))
		require.NoError(t, err)
		require.Equal(t, p, rp)

		_, err = tree.RootAt(uint64(i) + 1)
		require.Equal(t, ErrUnexistentData, err)

		_, err = tree.DataAt(uint64(i) + 1)
		require.Equal(t, ErrUnexistentData, err)
	}

	rp, err := tree.DataAt(uint64(1))
	require.NoError(t, err)
	require.Equal(t, []byte{byte(1)}, rp)

	err = tree.Sync()
	require.NoError(t, err)

	err = tree.Close()
	require.NoError(t, err)
}

func TestIntegrity(t *testing.T) {
	tree, err := Open("ahtree_test", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("ahtree_test")

	N := 1024

	for i := 1; i <= N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)
	}

	n, _, err := tree.Root()
	require.NoError(t, err)

	for i := uint64(1); i <= n; i++ {
		r, err := tree.RootAt(i)
		require.NoError(t, err)

		for j := uint64(1); j <= i; j++ {
			iproof, err := tree.InclusionProof(j, i)
			require.NoError(t, err)

			d, err := tree.DataAt(j)
			require.NoError(t, err)

			pd := make([]byte, 1+len(d))
			pd[0] = LeafPrefix
			copy(pd[1:], d)

			verifies := VerifyInclusion(iproof, j, i, sha256.Sum256(pd), r)
			require.True(t, verifies)
		}
	}
}

func TestOpenFail(t *testing.T) {
	_, err := Open("/dev/null", DefaultOptions().WithSynced(false))
	require.Error(t, err)
	os.Mkdir("ro_dir1", 0500)
	defer os.RemoveAll("ro_dir1")
	_, err = Open("ro_dir/bla", DefaultOptions().WithSynced(false))
	require.Error(t, err)
	_, err = Open("wrongdir\000", DefaultOptions().WithSynced(false))
	require.Error(t, err)
	defer os.RemoveAll("tt1")
	_, err = Open("tt1", DefaultOptions().WithSynced(false).WithAppFactory(
		func(rootPath, subPath string, opts *multiapp.Options) (a appendable.Appendable, e error) {
			if subPath == "tree" {
				e = errors.New("simulated error")
			}
			return
		}))
	_, err = Open("tt1", DefaultOptions().WithSynced(false).WithAppFactory(
		func(rootPath, subPath string, opts *multiapp.Options) (a appendable.Appendable, e error) {
			if subPath == "commit" {
				e = errors.New("simulated error")
			}
			return
		}))
	require.Error(t, err)
}

func TestInclusionAndConsistencyProofs(t *testing.T) {
	tree, err := Open("ahtree_test", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("ahtree_test")

	N := 1024

	for i := 1; i <= N; i++ {
		_, r, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)

		iproof, err := tree.InclusionProof(uint64(i), uint64(i))
		require.NoError(t, err)

		h := sha256.Sum256([]byte{LeafPrefix, byte(i)})

		verifies := VerifyInclusion(iproof, uint64(i), uint64(i), h, r)
		require.True(t, verifies)
	}

	_, err = tree.InclusionProof(2, 1)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = tree.ConsistencyProof(2, 1)
	require.Equal(t, ErrIllegalArguments, err)

	for i := 1; i <= N; i++ {
		for j := i; j <= N; j++ {
			iproof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			jroot, err := tree.RootAt(uint64(j))
			require.NoError(t, err)

			h := sha256.Sum256([]byte{LeafPrefix, byte(i)})

			verifies := VerifyInclusion(iproof, uint64(i), uint64(j), h, jroot)
			require.True(t, verifies)

			cproof, err := tree.ConsistencyProof(uint64(i), uint64(j))
			require.NoError(t, err)

			iroot, err := tree.RootAt(uint64(i))
			require.NoError(t, err)

			verifies = VerifyConsistency(cproof, uint64(i), uint64(j), iroot, jroot)
			require.True(t, verifies)
		}
	}

	for i := 1; i <= N; i++ {
		iproof, err := tree.InclusionProof(uint64(i), uint64(N))
		require.NoError(t, err)

		h := sha256.Sum256([]byte{LeafPrefix, byte(i)})
		root, err := tree.RootAt(uint64(i))
		require.NoError(t, err)

		verifies := VerifyLastInclusion(iproof, uint64(i), h, root)

		if i < N {
			require.False(t, verifies)
		} else {
			require.True(t, verifies)
		}
	}

	err = tree.Close()
	require.NoError(t, err)
}

func TestReOpenningImmudbStore(t *testing.T) {
	defer os.RemoveAll("ahtree_test")

	ItCount := 5
	ACount := 100

	for it := 0; it < ItCount; it++ {
		tree, err := Open("ahtree_test", DefaultOptions().WithSynced(false))
		require.NoError(t, err)

		for i := 0; i < ACount; i++ {
			p := []byte{byte(i)}

			_, _, err := tree.Append(p)
			require.NoError(t, err)
		}

		err = tree.Close()
		require.NoError(t, err)
	}

	tree, err := Open("ahtree_test", DefaultOptions().WithSynced(false))
	require.NoError(t, err)

	for i := 1; i <= ItCount*ACount; i++ {
		for j := i; j <= ItCount*ACount; j++ {
			proof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			root, _ := tree.RootAt(uint64(j))

			h := sha256.Sum256([]byte{LeafPrefix, byte((i - 1) % ACount)})

			verifies := VerifyInclusion(proof, uint64(i), uint64(j), h, root)
			require.True(t, verifies)
		}
	}

	err = tree.Close()
	require.NoError(t, err)
}

func TestReset(t *testing.T) {
	path, err := ioutil.TempDir("", "ahtree_test_reset")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	tree, err := Open(path, DefaultOptions())
	require.NoError(t, err)

	N := 32

	for i := 1; i <= N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)
	}

	err = tree.ResetSize(0)
	require.NoError(t, err)
	require.Zero(t, tree.Size())

	N = 1024

	for i := 1; i <= N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)
	}

	err = tree.ResetSize(uint64(N + 1))
	require.ErrorIs(t, err, ErrCannotResetToLargerSize)

	err = tree.ResetSize(uint64(N))
	require.NoError(t, err)
	require.Equal(t, uint64(N), tree.Size())

	N = 512

	err = tree.ResetSize(uint64(N))
	require.NoError(t, err)
	require.Equal(t, uint64(N), tree.Size())

	for i := 1; i <= N; i++ {
		for j := i; j <= N; j++ {
			iproof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			jroot, err := tree.RootAt(uint64(j))
			require.NoError(t, err)

			h := sha256.Sum256([]byte{LeafPrefix, byte(i)})

			verifies := VerifyInclusion(iproof, uint64(i), uint64(j), h, jroot)
			require.True(t, verifies)

			cproof, err := tree.ConsistencyProof(uint64(i), uint64(j))
			require.NoError(t, err)

			iroot, err := tree.RootAt(uint64(i))
			require.NoError(t, err)

			verifies = VerifyConsistency(cproof, uint64(i), uint64(j), iroot, jroot)
			require.True(t, verifies)
		}
	}

	for i := 1; i <= N; i++ {
		iproof, err := tree.InclusionProof(uint64(i), uint64(N))
		require.NoError(t, err)

		h := sha256.Sum256([]byte{LeafPrefix, byte(i)})
		root, err := tree.RootAt(uint64(i))
		require.NoError(t, err)

		verifies := VerifyLastInclusion(iproof, uint64(i), h, root)

		if i < N {
			require.False(t, verifies)
		} else {
			require.True(t, verifies)
		}
	}

	err = tree.Close()
	require.NoError(t, err)

	err = tree.ResetSize(uint64(N))
	require.ErrorIs(t, err, ErrAlreadyClosed)

	tree, err = Open(path, DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	err = tree.ResetSize(1)
	require.ErrorIs(t, err, ErrReadOnly)

	err = tree.Close()
	require.NoError(t, err)
}

func appendableFromBuffer(sourceData []byte) *mocked.MockedAppendable {

	data := make([]byte, len(sourceData))
	copy(data, sourceData)

	currOffs := int64(len(data))

	return &mocked.MockedAppendable{
		SizeFn:      func() (int64, error) { return int64(len(data)), nil },
		OffsetFn:    func() int64 { return currOffs },
		SetOffsetFn: func(off int64) error { currOffs = off; return nil },
		FlushFn:     func() error { return nil },
		SyncFn:      func() error { return nil },
		CloseFn:     func() error { return nil },
		AppendFn: func(bs []byte) (off int64, n int, err error) {
			off = currOffs
			n = len(bs)
			data = append(data[:currOffs], bs...)
			currOffs += int64(n)
			return off, n, nil
		},
		ReadAtFn: func(bs []byte, off int64) (int, error) {
			if off > int64(len(data)) {
				return 0, io.EOF
			}
			n := copy(bs, data[off:])
			if n < len(bs) {
				return n, io.EOF
			}
			return n, nil
		},
	}
}

func TestResetCornerCases(t *testing.T) {

	t.Run("should fail on cLog read error", func(t *testing.T) {
		injectedErr := errors.New("injected error")
		pLog := appendableFromBuffer(nil)
		dLog := appendableFromBuffer(make([]byte, 3*sha256.Size))
		cLog := appendableFromBuffer(make([]byte, 12*2))
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			if off == 0 {
				return 0, injectedErr
			}
			return len(bs), nil

		}
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		err = tree.ResetSize(1)
		require.ErrorIs(t, err, injectedErr)

		err = tree.Close()
		require.NoError(t, err)
	})

	t.Run("should fail on getting pLogSize", func(t *testing.T) {
		injectedErr := errors.New("injected error")
		pLog := appendableFromBuffer(nil)
		dLog := appendableFromBuffer(make([]byte, 3*sha256.Size))
		cLog := appendableFromBuffer(make([]byte, 12*2))
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		pLog.SizeFn = func() (int64, error) { return 0, injectedErr }

		err = tree.ResetSize(1)
		require.ErrorIs(t, err, injectedErr)

		err = tree.Close()
		require.NoError(t, err)
	})

	t.Run("should fail on corrupted older cLog entries", func(t *testing.T) {
		pLog := appendableFromBuffer(nil)
		dLog := appendableFromBuffer(make([]byte, 3*sha256.Size))
		cLog := appendableFromBuffer([]byte{
			1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, // Corrupted entry, offset way outside pLog size
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // Correct entry to allow opening without an error
		})
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		err = tree.ResetSize(1)
		require.ErrorIs(t, err, ErrorCorruptedData)

		err = tree.Close()
		require.NoError(t, err)
	})

	t.Run("should fail on dLog size error", func(t *testing.T) {
		injectedErr := errors.New("injected error")
		pLog := appendableFromBuffer(nil)
		dLog := appendableFromBuffer(make([]byte, 3*sha256.Size))
		cLog := appendableFromBuffer(make([]byte, 2*12))
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		dLog.SizeFn = func() (int64, error) { return 0, injectedErr }

		err = tree.ResetSize(1)
		require.ErrorIs(t, err, injectedErr)

		err = tree.Close()
		require.NoError(t, err)
	})

	t.Run("should fail on incorrect dlog size", func(t *testing.T) {
		pLog := appendableFromBuffer(nil)
		dLog := appendableFromBuffer(make([]byte, 3*sha256.Size))
		cLog := appendableFromBuffer(make([]byte, 2*12))
		tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
		require.NoError(t, err)

		dLog.SizeFn = func() (int64, error) { return 0, nil }

		err = tree.ResetSize(1)
		require.ErrorIs(t, err, ErrorCorruptedDigests)

		err = tree.Close()
		require.NoError(t, err)
	})
}

func BenchmarkAppend(b *testing.B) {
	tree, _ := Open("ahtree_test", DefaultOptions().WithSynced(false))
	defer os.RemoveAll("ahtree_test")

	for i := 0; i < b.N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		if err != nil {
			panic(err)
		}
	}
}
