/*
Copyright 2019-2020 vChain, Inc.

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
	"errors"
	"os"
	"testing"

	"codenotary.io/immudb-v2/appendable"
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
	_, err := Open("ahtree_test", nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = OpenWith(nil, nil, nil, nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = OpenWith(nil, nil, nil, DefaultOptions())
	require.Error(t, ErrIllegalArguments, err)

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaFileSize, DefaultOptions().fileSize)

	pLog := &MockedAppendable{metadata: metadata.Bytes()}
	dLog := &MockedAppendable{metadata: metadata.Bytes()}
	cLog := &MockedAppendable{}

	_, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.Error(t, ErrCorruptedCLog, err)

	cLog.metadata = metadata.Bytes()

	cLog.szErr = errors.New("error")
	_, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.Error(t, ErrCorruptedCLog, err)

	cLog.sz = cLogEntrySize + 1
	cLog.szErr = nil
	_, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.Error(t, ErrCorruptedCLog, err)

	cLog.sz = 0
	dLog.szErr = errors.New("error")
	_, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.Error(t, ErrCorruptedCLog, err)

	dLog.szErr = nil
	pLog.appendErr = errors.New("error")
	tree, err := OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.NoError(t, err)
	_, _, err = tree.Append(nil)
	require.Error(t, err)

	pLog.appendErr = nil
	dLog.appendErr = errors.New("error")
	tree, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.NoError(t, err)
	_, _, err = tree.Append(nil)
	require.Error(t, err)

	dLog.appendErr = nil
	cLog.appendErr = errors.New("error")
	tree, err = OpenWith(pLog, dLog, cLog, DefaultOptions())
	require.NoError(t, err)
	_, _, err = tree.Append(nil)
	require.Error(t, err)

	_, err = Open("options.go", DefaultOptions())
	require.Error(t, ErrorPathIsNotADirectory, err)

	_, err = Open("ahtree_test", DefaultOptions().SetDataCacheSlots(-1))
	require.Error(t, ErrIllegalArguments, err)

	_, err = Open("ahtree_test", DefaultOptions().SetDigestsCacheSlots(-1))
	require.Error(t, ErrIllegalArguments, err)

	tree, err = Open("ahtree_test", DefaultOptions().SetSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("ahtree_test")

	_, _, err = tree.Root()
	require.Error(t, ErrEmptyTree, err)

	_, err = tree.rootAt(1)
	require.Error(t, ErrEmptyTree, err)

	_, err = tree.rootAt(0)
	require.Error(t, ErrIllegalArguments, err)

	_, err = tree.DataAt(0)
	require.Error(t, ErrIllegalArguments, err)

	err = tree.Sync()
	require.NoError(t, err)

	err = tree.Close()
	require.NoError(t, err)

	_, _, err = tree.Append(nil)
	require.Error(t, ErrAlreadyClosed, err)

	_, err = tree.InclusionProof(1, 2)
	require.Error(t, ErrAlreadyClosed, err)

	_, err = tree.ConsistencyProof(1, 2)
	require.Error(t, ErrAlreadyClosed, err)

	_, _, err = tree.Root()
	require.Error(t, ErrAlreadyClosed, err)

	_, err = tree.rootAt(1)
	require.Error(t, ErrAlreadyClosed, err)

	_, err = tree.DataAt(1)
	require.Error(t, ErrAlreadyClosed, err)

	err = tree.Sync()
	require.Error(t, ErrAlreadyClosed, err)

	err = tree.Close()
	require.Error(t, ErrAlreadyClosed, err)
}

func TestReadOnly(t *testing.T) {
	_, err := Open("ahtree_test", DefaultOptions().SetReadOnly(true))
	defer os.RemoveAll("ahtree_test")
	require.Error(t, err)

	tree, err := Open("ahtree_test", DefaultOptions().SetReadOnly(false))
	require.NoError(t, err)
	err = tree.Close()
	require.NoError(t, err)

	tree, err = Open("ahtree_test", DefaultOptions().SetReadOnly(true))
	require.NoError(t, err)

	_, _, err = tree.Append(nil)
	require.Error(t, ErrReadOnly, err)

	err = tree.Sync()
	require.Error(t, ErrReadOnly, err)

	err = tree.Close()
	require.NoError(t, err)
}

type MockedAppendable struct {
	metadata []byte

	sz    int64
	szErr error

	offset    int64
	offsetErr error

	appendOff int64
	appendN   int
	appendErr error

	flushErr error

	syncErr error

	readAtN   int
	readAtErr error

	closeErr error
}

func (a *MockedAppendable) Metadata() []byte {
	return a.metadata
}

func (a *MockedAppendable) Size() (int64, error) {
	return a.sz, a.szErr
}

func (a *MockedAppendable) Offset() int64 {
	return a.offset
}

func (a *MockedAppendable) SetOffset(off int64) error {
	return a.offsetErr
}

func (a *MockedAppendable) Append(bs []byte) (off int64, n int, err error) {
	return a.appendOff, a.appendN, a.appendErr
}

func (a *MockedAppendable) Flush() error {
	return a.flushErr
}

func (a *MockedAppendable) Sync() error {
	return a.syncErr
}

func (a *MockedAppendable) ReadAt(bs []byte, off int64) (int, error) {
	return a.readAtN, a.readAtErr
}

func (a *MockedAppendable) Close() error {
	return a.closeErr
}

func TestOptions(t *testing.T) {
	opts := &Options{}

	defaultOpts := DefaultOptions()

	opts.SetSynced(!defaultOpts.synced)
	opts.SetReadOnly(!defaultOpts.readOnly)
	opts.SetFileSize(defaultOpts.fileSize * 10)
	opts.SetFileMode(defaultOpts.fileMode & 0xFF)
	opts.SetDataCacheSlots(defaultOpts.dataCacheSlots * 10)
	opts.SetDigestsCacheSlots(defaultOpts.digestsCacheSlots * 10)
	opts.SetCompressionFormat(appendable.ZLibCompression)
	opts.SetCompresionLevel(appendable.BestCompression)

	require.Equal(t, defaultOpts.synced, !opts.synced)
	require.Equal(t, defaultOpts.readOnly, !opts.readOnly)
	require.Equal(t, defaultOpts.fileSize*10, opts.fileSize)
	require.Equal(t, defaultOpts.fileMode&0xFF, opts.fileMode)
	require.Equal(t, defaultOpts.dataCacheSlots*10, opts.dataCacheSlots)
	require.Equal(t, defaultOpts.digestsCacheSlots*10, opts.digestsCacheSlots)
	require.Equal(t, appendable.ZLibCompression, opts.compressionFormat)
	require.Equal(t, appendable.BestCompression, opts.compressionLevel)
}

func TestAppend(t *testing.T) {
	tree, err := Open("ahtree_test", DefaultOptions().SetSynced(false))
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
		require.Error(t, ErrUnexistentData, err)

		_, err = tree.DataAt(uint64(i) + 1)
		require.Error(t, ErrUnexistentData, err)
	}

	err = tree.Sync()
	require.NoError(t, err)

	err = tree.Close()
	require.NoError(t, err)
}

func TestInclusionAndConsistencyProofs(t *testing.T) {
	tree, err := Open("ahtree_test", DefaultOptions().SetSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("ahtree_test")

	N := 1024

	for i := 1; i <= N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)
	}

	_, err = tree.InclusionProof(2, 1)
	require.Error(t, ErrIllegalArguments, err)

	_, err = tree.ConsistencyProof(2, 1)
	require.Error(t, ErrIllegalArguments, err)

	for i := 1; i <= N; i++ {
		for j := i; j <= N; j++ {
			iproof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			jroot, _ := tree.RootAt(uint64(j))

			h := sha256.Sum256([]byte{byte(i)})

			verifies := VerifyInclusion(iproof, uint64(i), uint64(j), h, jroot)
			require.True(t, verifies)

			cproof, err := tree.ConsistencyProof(uint64(i), uint64(j))
			require.NoError(t, err)

			iroot, _ := tree.RootAt(uint64(i))

			verifies = VerifyConsistency(cproof, uint64(i), uint64(j), iroot, jroot)

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
		tree, err := Open("ahtree_test", DefaultOptions().SetSynced(false))
		require.NoError(t, err)

		for i := 0; i < ACount; i++ {
			p := []byte{byte(i)}

			_, _, err := tree.Append(p)
			require.NoError(t, err)
		}

		err = tree.Close()
		require.NoError(t, err)
	}

	tree, err := Open("ahtree_test", DefaultOptions().SetSynced(false))
	require.NoError(t, err)

	for i := 1; i <= ItCount*ACount; i++ {
		for j := i; j <= ItCount*ACount; j++ {
			proof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			root, _ := tree.RootAt(uint64(j))

			h := sha256.Sum256([]byte{byte((i - 1) % ACount)})

			verifies := VerifyInclusion(proof, uint64(i), uint64(j), h, root)
			require.True(t, verifies)
		}
	}

	err = tree.Close()
	require.NoError(t, err)
}

func BenchmarkAppend(b *testing.B) {
	tree, _ := Open("ahtree_test", DefaultOptions().SetSynced(false))
	defer os.RemoveAll("ahtree_test")

	for i := 0; i < b.N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		if err != nil {
			panic(err)
		}
	}
}
