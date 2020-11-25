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
package singleapp

import (
	"bufio"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"

	"github.com/stretchr/testify/require"
)

func TestSingleApp(t *testing.T) {
	a, err := Open("testdata.aof", DefaultOptions())
	defer os.Remove("testdata.aof")
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
	require.Error(t, ErrIllegalArguments, err)

	off, n, err := a.Append([]byte{})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 0, n)

	off, n, err = a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 3, n)

	off, n, err = a.Append([]byte{4, 5, 6, 7, 8, 9, 10})
	require.NoError(t, err)
	require.Equal(t, int64(3), off)
	require.Equal(t, 7, n)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	bs = make([]byte, 4)
	n, err = a.ReadAt(bs, 6)
	require.NoError(t, err)
	require.Equal(t, []byte{7, 8, 9, 10}, bs)

	err = a.Sync()
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppReOpening(t *testing.T) {
	a, err := Open("testdata.aof", DefaultOptions())
	defer os.Remove("testdata.aof")
	require.NoError(t, err)

	off, n, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 3, n)

	err = a.Close()
	require.NoError(t, err)

	a, err = Open("testdata.aof", DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(3), sz)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	_, _, err = a.Append([]byte{})
	require.Error(t, ErrReadOnly, err)

	err = a.Flush()
	require.Error(t, ErrReadOnly, err)

	err = a.Sync()
	require.Error(t, ErrReadOnly, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestSingleAppCorruptedFileReadingMetadata(t *testing.T) {
	f, err := ioutil.TempFile(".", "singleapp_test_")
	require.NoError(t, err)

	defer os.Remove(f.Name())

	// should fail reading metadata len
	_, err = Open(f.Name(), DefaultOptions())
	require.Error(t, ErrCorruptedMetadata, err)

	mLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(mLenBs, 1)

	w := bufio.NewWriter(f)
	_, err = w.Write(mLenBs)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// should failt reading metadata
	_, err = Open(f.Name(), DefaultOptions())
	require.Error(t, ErrCorruptedMetadata, err)
}

func TestSingleAppCorruptedFileReadingCompresionFormat(t *testing.T) {
	f, err := ioutil.TempFile(".", "singleapp_test_")
	require.NoError(t, err)

	defer os.Remove(f.Name())

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
	require.Error(t, ErrCorruptedMetadata, err)
}

func TestSingleAppCorruptedFileReadingCompresionLevel(t *testing.T) {
	f, err := ioutil.TempFile(".", "singleapp_test_")
	require.NoError(t, err)

	defer os.Remove(f.Name())

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
	require.Error(t, ErrCorruptedMetadata, err)
}

func TestSingleAppCorruptedFileReadingCompresionWrappedMetadata(t *testing.T) {
	f, err := ioutil.TempFile(".", "singleapp_test_")
	require.NoError(t, err)

	defer os.Remove(f.Name())

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
	require.Error(t, ErrCorruptedMetadata, err)
}

func TestSingleAppEdgeCases(t *testing.T) {
	_, err := Open("testdata.aof", nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = Open("testdata.aof", DefaultOptions().WithReadOnly(true))
	require.Error(t, err)

	a, err := Open("testdata.aof", DefaultOptions().WithSynced(false))
	defer os.RemoveAll("testdata.aof")
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	_, err = a.ReadAt(nil, 0)
	require.Error(t, ErrIllegalArguments, err)

	err = a.Close()
	require.NoError(t, err)

	_, err = a.Size()
	require.Error(t, ErrAlreadyClosed, err)

	err = a.SetOffset(0)
	require.Error(t, ErrAlreadyClosed, err)

	_, _, err = a.Append([]byte{})
	require.Error(t, ErrAlreadyClosed, err)

	_, err = a.ReadAt([]byte{}, 0)
	require.Error(t, ErrIllegalArguments, err)

	err = a.Flush()
	require.Error(t, ErrAlreadyClosed, err)

	err = a.Sync()
	require.Error(t, ErrAlreadyClosed, err)

	err = a.Close()
	require.Error(t, ErrAlreadyClosed, err)
}

func TestSingleAppZLibCompression(t *testing.T) {
	opts := DefaultOptions().WithCompressionFormat(appendable.ZLibCompression)
	a, err := Open("testdata.aof", opts)
	defer os.Remove("testdata.aof")
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
	a, err := Open("testdata.aof", opts)
	defer os.Remove("testdata.aof")
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
	a, err := Open("testdata.aof", opts)
	defer os.Remove("testdata.aof")
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
	a, err := Open("testdata.aof", opts)
	defer os.Remove("testdata.aof")
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
