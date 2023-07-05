/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

type EdgeCasesTestSuite struct {
	suite.Suite

	pLog        *mocked.MockedAppendable
	cLog        *mocked.MockedAppendable
	dLog        *mocked.MockedAppendable
	injectedErr error
}

func TestEdgeCasesTestSuite(t *testing.T) {
	suite.Run(t, new(EdgeCasesTestSuite))
}

func (t *EdgeCasesTestSuite) SetupTest() {

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
	dummySync := func() error {
		return nil
	}
	dummySize := func() (int64, error) {
		return 0, nil
	}

	dummyClose := func() error {
		return nil
	}

	t.pLog = &mocked.MockedAppendable{
		SizeFn:      dummySize,
		AppendFn:    dummyAppend(),
		SetOffsetFn: dummySetOffset,
		FlushFn:     dummyFlush,
		CloseFn:     dummyClose,
		SyncFn:      dummySync,
	}

	t.cLog = &mocked.MockedAppendable{
		SizeFn:      dummySize,
		AppendFn:    dummyAppend(),
		SetOffsetFn: dummySetOffset,
		FlushFn:     dummyFlush,
		CloseFn:     dummyClose,
		SyncFn:      dummySync,
	}

	t.dLog = &mocked.MockedAppendable{
		SizeFn:      dummySize,
		AppendFn:    dummyAppend(),
		SetOffsetFn: dummySetOffset,
		FlushFn:     dummyFlush,
		CloseFn:     dummyClose,
		SyncFn:      dummySync,
	}

	t.injectedErr = errors.New("injected error")
}

func (t *EdgeCasesTestSuite) TestShouldFailOnIllegalArguments() {
	_, err := Open(t.T().TempDir(), nil)
	t.Require().ErrorIs(err, ErrInvalidOptions)
	t.Require().ErrorIs(err, ErrIllegalArguments)

	_, err = OpenWith(nil, nil, nil, nil)
	t.Require().ErrorIs(err, ErrIllegalArguments)

	_, err = OpenWith(nil, nil, nil, DefaultOptions())
	t.Require().ErrorIs(err, ErrIllegalArguments)

	_, err = OpenWith(t.pLog, t.dLog, t.cLog, nil)
	t.Require().ErrorIs(err, ErrIllegalArguments)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileQueryingCLogSize() {
	t.cLog.SizeFn = func() (int64, error) {
		return 0, t.injectedErr
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileSettingCLogOffset() {
	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize - 1, nil
	}
	t.cLog.SetOffsetFn = func(off int64) error {
		return t.injectedErr
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldGailWhileSettingPLogOffset() {
	t.pLog.SetOffsetFn = func(off int64) error {
		return t.injectedErr
	}
	t.pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
		t.Require().Fail("Should fail before appending data")
		return 0, 0, nil
	}
	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileAppendingPayloadWritingLength() {
	t.pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
		return 0, 0, t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileAppendingPayloadWritingData() {
	written := 0
	t.pLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
		off = int64(written)
		written += len(bs)
		if written > 4 {
			return 0, 0, t.injectedErr
		}
		return off, len(bs), nil
	}
	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailFlushingPLog() {
	t.pLog.FlushFn = func() error {
		return t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().NoError(err)

	err = tree.Sync()
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailOnDLogSetOffset() {
	t.dLog.SetOffsetFn = func(off int64) error {
		return t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailFlushingDLog() {
	t.dLog.FlushFn = func() error {
		return t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().NoError(err)

	err = tree.Sync()
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailOnCLogSetOffsetDuringAppend() {
	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions().WithSyncThld(1))
	t.Require().NoError(err)

	t.cLog.SetOffsetFn = func(off int64) error {
		return t.injectedErr
	}

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailWritingCLog() {
	t.cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
		return 0, 0, t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions().WithSyncThld(1))
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailFlushingCLog() {
	t.cLog.FlushFn = func() error {
		return t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions().WithSyncThld(2))
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{4, 5, 6})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailCalculatingHashesOnAppend() {
	t.dLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().NoError(err)

	tree.dCache.Pop(uint64(0))

	_, _, err = tree.Append([]byte{4, 5, 6})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileValidatingPLogSize() {
	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	t.cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs[:], 0)
		binary.BigEndian.PutUint32(bs[offsetSize:], 8)
		return cLogEntrySize, nil
	}
	t.pLog.SizeFn = func() (int64, error) {
		return 0, nil
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, ErrorCorruptedData)
}

func (t *EdgeCasesTestSuite) TestShouldFailWhileValidatingDLogSize() {
	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	t.cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs[:], 0)
		binary.BigEndian.PutUint32(bs[offsetSize:], 8)
		return cLogEntrySize, nil
	}
	t.pLog.SizeFn = func() (int64, error) {
		return szSize + 8, nil
	}
	t.dLog.SizeFn = func() (int64, error) {
		return 0, nil
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, ErrorCorruptedDigests)
}

func (t *EdgeCasesTestSuite) TestShouldFailReadingDLogSize() {
	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	t.cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs[:], 0)
		binary.BigEndian.PutUint32(bs[offsetSize:], 8)
		return cLogEntrySize, nil
	}
	t.pLog.SizeFn = func() (int64, error) {
		return szSize + 8, nil
	}
	t.dLog.SizeFn = func() (int64, error) {
		return 0, t.injectedErr
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailReadingPLogSize() {
	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	t.cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs[:], 0)
		binary.BigEndian.PutUint32(bs[offsetSize:], 8)
		return cLogEntrySize, nil
	}
	t.pLog.SizeFn = func() (int64, error) {
		return 0, t.injectedErr
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailReadingLastCLogEntry() {
	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)

	t.dLog.MetadataFn = metadata.Bytes
	t.cLog.MetadataFn = metadata.Bytes

	t.cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}

	t.cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, t.injectedErr
	}

	_, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailAppendingToDLog() {
	t.dLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
		return 0, 0, t.injectedErr
	}

	tree, err := OpenWith(t.pLog, t.dLog, t.cLog, DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append(nil)
	t.Require().ErrorIs(err, ErrIllegalArguments)

	_, _, err = tree.Append([]byte{1, 2, 3})
	t.Require().ErrorIs(err, t.injectedErr)
}

func (t *EdgeCasesTestSuite) TestShouldFailDueToInvalidPath() {
	_, err := Open("options.go", DefaultOptions())
	t.Require().ErrorIs(err, ErrorPathIsNotADirectory)
}

func (t *EdgeCasesTestSuite) TestShouldFailDueToInvalidCacheSize() {
	_, err := Open(t.T().TempDir(), DefaultOptions().WithDataCacheSlots(-1))
	t.Require().ErrorIs(err, ErrInvalidOptions)
}

func (t *EdgeCasesTestSuite) TestShouldFailDueToInvalidDigestsCacheSize() {
	_, err := Open(t.T().TempDir(), DefaultOptions().WithDigestsCacheSlots(-1))
	t.Require().ErrorIs(err, ErrInvalidOptions)
}

func (t *EdgeCasesTestSuite) TestWithEmptyFiles() {
	tree, err := Open(t.T().TempDir(), DefaultOptions())
	t.Require().NoError(err)

	t.Run("should fail to get tree root when tree is empty", func() {
		_, _, err := tree.Root()
		t.Require().ErrorIs(err, ErrEmptyTree)

		_, err = tree.rootAt(1)
		t.Require().ErrorIs(err, ErrEmptyTree)

		_, err = tree.rootAt(0)
		t.Require().ErrorIs(err, ErrIllegalArguments)
	})

	t.Run("should fail to get data when tree is empty", func() {
		_, err := tree.DataAt(0)
		t.Require().ErrorIs(err, ErrIllegalArguments)
	})

	t.Run("should not error when syncing empty tree", func() {
		err := tree.Sync()
		t.Require().NoError(err)
	})

	t.Run("should fail on inclusion proof for non-existing root node", func() {
		_, err := tree.InclusionProof(1, 2)
		t.Require().ErrorIs(err, ErrUnexistentData)
	})

	err = tree.Close()
	t.Require().NoError(err)
}

func (t *EdgeCasesTestSuite) TestFailAfterClose() {
	tree, err := Open(t.T().TempDir(), DefaultOptions())
	t.Require().NoError(err)

	_, _, err = tree.Append([]byte{1})
	t.Require().NoError(err)

	err = tree.Close()
	t.Require().NoError(err)

	_, _, err = tree.Append(nil)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	_, err = tree.InclusionProof(1, 2)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	_, err = tree.ConsistencyProof(1, 2)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	_, _, err = tree.Root()
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	_, err = tree.rootAt(1)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	_, err = tree.DataAt(1)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	err = tree.Sync()
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	err = tree.ResetSize(0)
	t.Require().ErrorIs(err, ErrAlreadyClosed)

	err = tree.Close()
	t.Require().ErrorIs(err, ErrAlreadyClosed)
}

func TestReadOnly(t *testing.T) {
	dir := t.TempDir()

	tree, err := Open(dir, DefaultOptions().WithReadOnly(false))
	require.NoError(t, err)
	err = tree.Close()
	require.NoError(t, err)

	tree, err = Open(dir, DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	_, _, err = tree.Append(nil)
	require.ErrorIs(t, err, ErrReadOnly)

	err = tree.Sync()
	require.ErrorIs(t, err, ErrReadOnly)

	err = tree.Close()
	require.NoError(t, err)
}

func TestAppend(t *testing.T) {
	opts := DefaultOptions().
		WithDigestsCacheSlots(100).
		WithDataCacheSlots(100)

	tree, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	N := 100

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
		require.ErrorIs(t, err, ErrUnexistentData)

		_, err = tree.DataAt(uint64(i) + 1)
		require.ErrorIs(t, err, ErrUnexistentData)
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
	tree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

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
	_, err := Open("/dev/null", DefaultOptions())
	require.Error(t, err)

	roDir := filepath.Join(t.TempDir(), "ro_dir1")
	os.Mkdir(roDir, 0500)
	_, err = Open(filepath.Join(roDir, "bla"), DefaultOptions())
	require.Error(t, err)

	_, err = Open("wrongdir\000", DefaultOptions())
	require.Error(t, err)

	tt1Dir := os.TempDir()

	_, err = Open(tt1Dir, DefaultOptions().WithAppFactory(
		func(rootPath, subPath string, opts *multiapp.Options) (a appendable.Appendable, e error) {
			if subPath == "tree" {
				e = errors.New("simulated error")
			}
			return
		}))
	require.Error(t, err)

	_, err = Open(tt1Dir, DefaultOptions().WithAppFactory(
		func(rootPath, subPath string, opts *multiapp.Options) (a appendable.Appendable, e error) {
			if subPath == "commit" {
				e = errors.New("simulated error")
			}
			return
		}))
	require.Error(t, err)
}

func TestInclusionAndConsistencyProofs(t *testing.T) {
	tree, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

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
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = tree.ConsistencyProof(2, 1)
	require.ErrorIs(t, err, ErrIllegalArguments)

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
	dir := t.TempDir()

	ItCount := 5
	ACount := 100

	for it := 0; it < ItCount; it++ {
		tree, err := Open(dir, DefaultOptions())
		require.NoError(t, err)

		for i := 0; i < ACount; i++ {
			p := []byte{byte(i)}

			_, _, err := tree.Append(p)
			require.NoError(t, err)
		}

		err = tree.Close()
		require.NoError(t, err)
	}

	tree, err := Open(dir, DefaultOptions())
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
	path := t.TempDir()

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
		pLog.SizeFn = func() (int64, error) {
			return 2 * szSize, nil
		}
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
		pLog.SizeFn = func() (int64, error) {
			return 2 * szSize, nil
		}
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
		pLog.SizeFn = func() (int64, error) {
			return szSize, nil
		}
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
		pLog.SizeFn = func() (int64, error) {
			return 2 * szSize, nil
		}
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
		pLog.SizeFn = func() (int64, error) {
			return 2 * szSize, nil
		}
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
	opts := DefaultOptions().
		WithWriteBufferSize(1 << 26). //64Mb
		WithRetryableSync(true).
		WithAutoSync(true).
		WithSyncThld(100_000).
		WithFileSize(1 << 29)

	tree, err := Open(b.TempDir(), opts)
	require.NoError(b, err)

	var bs [32]byte

	for i := 0; i < b.N; i++ {
		for j := 0; j < 1_000_000; j++ {
			_, _, err := tree.Append(bs[:])
			require.NoError(b, err)
		}
	}

	tree.Close()
}

func TestAppendAfterReopening(t *testing.T) {
	opts := DefaultOptions().
		WithWriteBufferSize(1 << 26). //64Mb
		WithRetryableSync(true).
		WithAutoSync(true).
		WithSyncThld(2_000).
		WithFileSize(1 << 16).
		WithDigestsCacheSlots(2)

	path := t.TempDir()

	for i := 0; i < 10; i++ {
		tree, err := Open(path, opts)
		require.NoError(t, err)

		var bs [1]byte

		for j := 0; j < 1025; j++ {
			_, _, err = tree.Append(bs[:])
			require.NoError(t, err)
		}

		tree.Close()
	}
}
