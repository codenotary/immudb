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

package multiapp

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"

	"github.com/stretchr/testify/require"
)

func TestMultiApp(t *testing.T) {
	md := appendable.NewMetadata(nil)
	md.PutInt("mkey1", 1)

	a, err := Open(filepath.Join(t.TempDir(), "multiapp"), DefaultOptions().WithMetadata(md.Bytes()))
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(0), sz)

	require.Equal(t, appendable.DefaultCompressionFormat, a.CompressionFormat())
	require.Equal(t, appendable.DefaultCompressionLevel, a.CompressionLevel())

	err = a.SetOffset(0)
	require.NoError(t, err)

	require.Equal(t, int64(0), a.Offset())

	mkey1, found := appendable.NewMetadata(a.Metadata()).GetInt("mkey1")
	require.True(t, found)
	require.Equal(t, 1, mkey1)

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

	err = a.Sync()
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestMultiApOffsetAndCacheEviction(t *testing.T) {
	a, err := Open(t.TempDir(), DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	require.NoError(t, err)

	off, n, err := a.Append([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	err = a.SetOffset(0)
	require.NoError(t, err)

	_, _, err = a.Append([]byte{7, 6, 5, 4, 3, 2, 1, 0})
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	b := make([]byte, n)
	_, err = a.ReadAt(b, off)
	require.NoError(t, err)

	require.Equal(t, []byte{7, 6, 5, 4, 3, 2, 1, 0}, b)
}

func TestMultiAppClosedAndDeletedFiles(t *testing.T) {
	path := t.TempDir()

	a, err := Open(path, DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	require.NoError(t, err)

	_, n, err := a.Append([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	require.NoError(t, err)

	err = a.appendables.Apply(func(k int64, v appendable.Appendable) error {
		return v.Close()
	})
	require.NoError(t, err)

	err = a.Close()
	require.ErrorIs(t, err, singleapp.ErrAlreadyClosed)

	fname := filepath.Join(a.path, appendableName(0, a.fileExt))
	os.Remove(fname)

	a, err = Open(path, DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	require.NoError(t, err)

	b := make([]byte, n)
	_, err = a.ReadAt(b, 0)
	require.ErrorIs(t, err, io.EOF)
}

func TestMultiAppClosedFiles(t *testing.T) {
	a, err := Open(t.TempDir(), DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(2))
	require.NoError(t, err)

	_, _, err = a.Append([]byte{0, 1, 2})
	require.NoError(t, err)

	err = a.appendables.Apply(func(k int64, v appendable.Appendable) error {
		return v.Close()
	})
	require.NoError(t, err)

	_, _, err = a.Append([]byte{3, 4, 5})
	require.ErrorIs(t, err, singleapp.ErrAlreadyClosed)
}

func TestMultiAppReOpening(t *testing.T) {
	path := t.TempDir()

	a, err := Open(path, DefaultOptions().WithFileSize(1))
	require.NoError(t, err)

	off, n, err := a.Append([]byte{1, 2})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 2, n)

	off, n, err = a.Append([]byte{3})
	require.NoError(t, err)
	require.Equal(t, int64(2), off)
	require.Equal(t, 1, n)

	copyPath := t.TempDir()

	err = a.Copy(copyPath)
	require.NoError(t, err)

	defer os.RemoveAll(copyPath)

	err = a.Close()
	require.NoError(t, err)

	a, err = Open(copyPath, DefaultOptions())
	require.NoError(t, err)

	err = a.SwitchToReadOnlyMode()
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(3), sz)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []byte{1, 2, 3}, bs)

	_, _, err = a.Append([]byte{})
	require.ErrorIs(t, err, ErrReadOnly)

	_, _, err = a.Append([]byte{0})
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Flush()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.SwitchToReadOnlyMode()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Sync()
	require.ErrorIs(t, err, ErrReadOnly)

	err = a.Close()
	require.NoError(t, err)
}

func TestMultiAppEdgeCases(t *testing.T) {
	path := t.TempDir()

	_, err := Open(path, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = Open("multi_app_test.go", DefaultOptions())
	require.ErrorIs(t, err, ErrorPathIsNotADirectory)

	_, err = Open(path, DefaultOptions().WithReadOnly(true))
	require.Error(t, err)

	a, err := Open(path, DefaultOptions())
	require.NoError(t, err)

	_, err = a.ReadAt(nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = a.ReadAt([]byte{}, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = a.Copy("multi_app_test.go")
	require.ErrorContains(t, err, "not a directory")

	err = a.Close()
	require.NoError(t, err)

	_, err = a.Size()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Copy("copy")
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.SetOffset(0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, err = a.Append([]byte{})
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = a.ReadAt(make([]byte, 1), 0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Flush()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.SwitchToReadOnlyMode()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Sync()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = a.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMultiAppCompression(t *testing.T) {
	path := t.TempDir()

	a, err := Open(path, DefaultOptions().WithCompressionFormat(appendable.ZLibCompression))
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

func TestMultiAppAppendableForCurrentChunk(t *testing.T) {
	path := t.TempDir()

	a, err := Open(path, DefaultOptions().WithFileSize(10))
	require.NoError(t, err)

	testData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	off, n, err := a.Append(testData)
	require.NoError(t, err)
	require.EqualValues(t, 0, off)
	require.EqualValues(t, n, 12)

	app, err := a.appendableFor(11)
	require.NoError(t, err)
	require.Equal(t, a.currApp, app)
}

func TestMultiappOpenIncorrectPath(t *testing.T) {
	testfile := filepath.Join(t.TempDir(), "testfile")
	require.NoError(t, ioutil.WriteFile(testfile, []byte{}, 0777))

	a, err := Open(testfile, DefaultOptions())
	require.Error(t, err)
	require.Nil(t, a)
}

func TestMultiappOpenFolderWithBogusFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "test_bogus_dir")
	require.NoError(t, os.MkdirAll(dir, 0777))
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "bogus_file"), []byte{}, 0777))

	a, err := Open(dir, DefaultOptions())
	require.Error(t, err)
	require.Nil(t, a)
}

func TestMultiAppDiscard(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir, DefaultOptions().WithFileSize(1))
	require.NoError(t, err)

	err = a.DiscardUpto(0)
	require.NoError(t, err)

	err = a.DiscardUpto(1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	off0, n0, err := a.Append([]byte{1, 2})
	require.NoError(t, err)
	require.Equal(t, int64(0), off0)
	require.Equal(t, 2, n0)

	off1, n1, err := a.Append([]byte{3, 4})
	require.NoError(t, err)
	require.Equal(t, int64(2), off1)
	require.Equal(t, 2, n1)

	err = a.DiscardUpto(off0 + int64(n0))
	require.NoError(t, err)

	err = a.DiscardUpto(off1 + int64(n1))
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)

	err = a.DiscardUpto(1)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	a, err = Open(dir, DefaultOptions().WithFileSize(1))
	require.NoError(t, err)

	off2, n2, err := a.Append([]byte{5})
	require.NoError(t, err)
	require.Equal(t, int64(4), off2)
	require.Equal(t, 1, n2)

	err = a.Close()
	require.NoError(t, err)
}
