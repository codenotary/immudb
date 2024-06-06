/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package singleapp

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"

	"github.com/stretchr/testify/require"
)

func TestSingleApp(t *testing.T) {
	buf := make([]byte, DefaultWriteBufferSize*5)

	opts := DefaultOptions().
		WithReadBufferSize(DefaultReadBufferSize * 2).
		WithWriteBuffer(buf)

	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(0), sz)

	require.Equal(t, appendable.DefaultCompressionFormat, a.CompressionFormat())
	require.Equal(t, appendable.DefaultCompressionLevel, a.CompressionLevel())

	err = a.SetOffset(0)
	require.NoError(t, err)

	require.Equal(t, int64(0), a.Offset())

	md := a.Metadata()
	require.Nil(t, md)

	_, _, err = a.Append(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = a.Append([]byte{})
	require.ErrorIs(t, err, ErrIllegalArguments)

	off, n, err := a.Append([]byte{0})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 1, n)

	off, n, err = a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(1), off)
	require.Equal(t, 3, n)

	off, n, err = a.Append([]byte{4, 5, 6, 7, 8, 9, 10})
	require.NoError(t, err)
	require.Equal(t, int64(4), off)
	require.Equal(t, 7, n)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 4)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte{0, 1, 2, 3}, bs)

	bs = make([]byte, 4)
	n, err = a.ReadAt(bs, 7)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, []byte{7, 8, 9, 10}, bs)

	n, err = a.ReadAt(bs, 1000)
	require.Equal(t, n, 0)
	require.ErrorIs(t, err, io.EOF)

	err = a.Sync()
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppSetOffsetWithRetryableSyncOn(t *testing.T) {
	opts := DefaultOptions().
		WithWriteBuffer(make([]byte, 64))

	testSingleAppSetOffsetWith(opts, t)
}

func TestSingleAppSetOffsetWithRetryableSyncOff(t *testing.T) {
	opts := DefaultOptions().
		WithRetryableSync(false).
		WithWriteBuffer(make([]byte, 64))

	testSingleAppSetOffsetWith(opts, t)
}

func testSingleAppSetOffsetWith(opts *Options, t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	err = a.SetOffset(-1)
	require.ErrorIs(t, err, ErrNegativeOffset)

	// prealloc buffer used when writing data
	writeBuf := make([]byte, len(opts.writeBuffer)*3)

	// prealloc buffer used when reading data
	readBuf := make([]byte, len(writeBuf))

	for i := 1; i <= len(writeBuf); i++ {
		// gen some random data
		rand.Read(writeBuf[:i])

		off, n, err := a.Append(writeBuf[:i])
		require.NoError(t, err)
		require.Equal(t, int64(0), off)
		require.Equal(t, i, n)

		err = a.SetOffset(int64(n + 1))
		require.ErrorIs(t, err, ErrIllegalArguments)

		// incremental left truncation
		for j := 0; j <= n; j++ {
			err = a.SetOffset(int64(n - j))
			require.NoError(t, err)
			require.Equal(t, int64(n-j), a.Offset())

			sz, err := a.Size()
			require.NoError(t, err)
			require.Equal(t, int64(n-j), sz)

			// read entire content should match what was appended
			rn, err := a.ReadAt(readBuf[:sz], 0)
			require.NoError(t, err)
			require.Equal(t, sz, int64(rn))
			require.Equal(t, writeBuf[:sz], readBuf[:sz])
		}
	}

	require.Zero(t, a.Offset())

	sz, err := a.Size()
	require.NoError(t, err)
	require.Zero(t, sz)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppSwitchToReadOnlyMode(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), DefaultOptions())
	require.NoError(t, err)

	off, n, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 3, n)

	err = a.SwitchToReadOnlyMode()
	require.NoError(t, err)

	err = a.SwitchToReadOnlyMode()
	require.ErrorIs(t, err, ErrReadOnly)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppReOpening(t *testing.T) {
	dir := t.TempDir()

	a, err := Open(filepath.Join(dir, "testdata.aof"), DefaultOptions())
	require.NoError(t, err)

	off, n, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 3, n)

	err = a.Copy(filepath.Join(dir, "testdata_copy.aof"))
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)

	a, err = Open(filepath.Join(dir, "testdata_copy.aof"), DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(3), sz)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.SwitchToReadOnlyMode()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.SetOffset(sz)
	require.ErrorIs(t, err, ErrReadOnly)

	_, _, err = a.Append([]byte{})
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Flush()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Sync()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppCorruptedFileReadingMetadata(t *testing.T) {
	f, err := ioutil.TempFile(t.TempDir(), "singleapp_test_")
	require.NoError(t, err)

	// should fail reading metadata len
	_, err = Open(f.Name(), DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedMetadata)

	mLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(mLenBs, 1)

	w := bufio.NewWriter(f)
	_, err = w.Write(mLenBs)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// should failt reading metadata
	_, err = Open(f.Name(), DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedMetadata)
}

func TestSingleAppCorruptedFileReadingCompresionFormat(t *testing.T) {
	f, err := ioutil.TempFile(t.TempDir(), "singleapp_test_")
	require.NoError(t, err)

	m := appendable.NewMetadata(nil)

	mBs := m.Bytes()
	mLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(mLenBs, uint32(len(mBs)))

	w := bufio.NewWriter(f)
	_, err = w.Write(mLenBs)
	require.NoError(t, err)

	_, err = w.Write(mBs)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// should failt reading metadata
	_, err = Open(f.Name(), DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedMetadata)
}

func TestSingleAppCorruptedFileReadingCompresionLevel(t *testing.T) {
	f, err := ioutil.TempFile(t.TempDir(), "singleapp_test_")
	require.NoError(t, err)

	m := appendable.NewMetadata(nil)
	m.PutInt(metaCompressionFormat, appendable.NoCompression)

	mBs := m.Bytes()
	mLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(mLenBs, uint32(len(mBs)))

	w := bufio.NewWriter(f)
	_, err = w.Write(mLenBs)
	require.NoError(t, err)

	_, err = w.Write(mBs)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// should failt reading metadata
	_, err = Open(f.Name(), DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedMetadata)
}

func TestSingleAppCorruptedFileReadingCompresionWrappedMetadata(t *testing.T) {
	f, err := ioutil.TempFile(t.TempDir(), "singleapp_test_")
	require.NoError(t, err)

	m := appendable.NewMetadata(nil)
	m.PutInt(metaCompressionFormat, appendable.NoCompression)
	m.PutInt(metaCompressionLevel, appendable.DefaultCompression)

	mBs := m.Bytes()
	mLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(mLenBs, uint32(len(mBs)))

	w := bufio.NewWriter(f)
	_, err = w.Write(mLenBs)
	require.NoError(t, err)

	_, err = w.Write(mBs)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// should failt reading metadata
	_, err = Open(f.Name(), DefaultOptions())
	require.ErrorIs(t, err, ErrCorruptedMetadata)
}

func TestSingleAppEdgeCases(t *testing.T) {
	dir := t.TempDir()

	_, err := Open(filepath.Join(dir, "testdata.aof"), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = Open(filepath.Join(dir, "testdata.aof"), DefaultOptions().WithReadOnly(true))
	require.Error(t, err)

	a, err := Open(filepath.Join(dir, "testdata.aof"), DefaultOptions().WithRetryableSync(false))
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	_, err = a.ReadAt(nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = a.Close()
	require.NoError(t, err)

	_, err = a.Size()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Copy(filepath.Join(dir, "copy.aof"))
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.SetOffset(0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, err = a.Append([]byte{})
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = a.ReadAt([]byte{}, 0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Flush()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Sync()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.DiscardUpto(1)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestSingleAppZLibCompression(t *testing.T) {
	opts := DefaultOptions().WithCompressionFormat(appendable.ZLibCompression)
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	off, _, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	_, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppFlateCompression(t *testing.T) {
	opts := DefaultOptions().WithCompressionFormat(appendable.FlateCompression)
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	off, _, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	_, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppGZipCompression(t *testing.T) {
	opts := DefaultOptions().WithCompressionFormat(appendable.GZipCompression)
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	off, _, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	_, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppLZWCompression(t *testing.T) {
	opts := DefaultOptions().WithCompressionFormat(appendable.LZWCompression)
	a, err := Open(filepath.Join(t.TempDir(), "testdata.aof"), opts)
	require.NoError(t, err)

	off, _, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	_, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppCantCreateFile(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "exists"), 0644)

	_, err := Open(filepath.Join(dir, "exists"), DefaultOptions())
	require.ErrorContains(t, err, "exists")

	app, err := Open(filepath.Join(dir, "valid"), DefaultOptions())
	require.NoError(t, err)
	err = app.Copy(filepath.Join(dir, "exists"))
	require.ErrorContains(t, err, "exists")
}

func TestSingleAppDiscard(t *testing.T) {
	app, err := Open(filepath.Join(t.TempDir(), "testdata_discard.aof"), DefaultOptions())
	require.NoError(t, err)

	err = app.DiscardUpto(0)
	require.NoError(t, err)

	err = app.DiscardUpto(1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	off, n, err := app.Append([]byte{1, 2, 3})
	require.NoError(t, err)

	err = app.DiscardUpto(off + int64(n))
	require.NoError(t, err)

	err = app.Close()
	require.NoError(t, err)
}

func BenchmarkAppendFlush(b *testing.B) {
	opts := DefaultOptions().
		WithRetryableSync(false).
		WithWriteBuffer(make([]byte, DefaultWriteBufferSize))

	app, err := Open(filepath.Join(b.TempDir(), "testdata_benchmark_flush.aof"), opts)
	require.NoError(b, err)

	b.ResetTimer()

	chunck := make([]byte, 512)

	for i := 0; i < b.N; i++ {
		for j := 1; j <= 1000; j++ {
			_, _, err = app.Append(chunck)
			require.NoError(b, err)

			err = app.Flush()
			require.NoError(b, err)
		}
	}

	err = app.Close()
	require.NoError(b, err)
}

func BenchmarkAppendFlushless(b *testing.B) {
	opts := DefaultOptions().
		WithRetryableSync(false).
		WithWriteBuffer(make([]byte, DefaultWriteBufferSize*16))

	app, err := Open(filepath.Join(b.TempDir(), "testdata_benchmark_flushless.aof"), opts)
	require.NoError(b, err)

	b.ResetTimer()

	chunck := make([]byte, 512)

	for i := 0; i < b.N; i++ {
		for j := 1; j <= 1000; j++ {
			_, _, err = app.Append(chunck)
			require.NoError(b, err)
		}
	}

	err = app.Close()
	require.NoError(b, err)
}
