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

package fs

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/immuos"

	"github.com/stretchr/testify/require"
)

func testArchiveUnarchive(
	t *testing.T,
	srcDir string,
	dst string,
	archive func() error,
	unarchive func() error,
) {

}

func TestTarUnTar(t *testing.T) {
	baseDir := t.TempDir()
	srcDir := filepath.Join(baseDir, "test-tar-untar")
	dst := srcDir + ".tar.gz"
	tarer := NewStandardTarer()

	require.NoError(t, os.MkdirAll(srcDir, 0755))

	srcSubDir1 := filepath.Join(srcDir, "dir1")
	require.NoError(t, os.MkdirAll(srcSubDir1, 0755))

	srcSubDir2 := filepath.Join(srcSubDir1, "dir2")
	require.NoError(t, os.MkdirAll(srcSubDir2, 0755))

	files := []struct {
		Name    string
		Content []byte
	}{
		{filepath.Join(srcSubDir1, "file1"), []byte("file1\ncontent1")},
		{filepath.Join(srcSubDir2, "file2"), []byte("file2\ncontent2")},
	}

	for _, fl := range files {
		require.NoError(t, ioutil.WriteFile(fl.Name, fl.Content, 0644))
	}

	require.NoError(t, tarer.TarIt(srcDir, dst))
	_, err := os.Stat(dst)
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(srcDir))

	require.NoError(t, tarer.UnTarIt(dst, baseDir))

	for _, fl := range files {
		fileContent1, err := ioutil.ReadFile(fl.Name)
		require.NoError(t, err)
		require.Equal(t, fl.Content, fileContent1)
	}
}

func TestTarSrcNonExistent(t *testing.T) {
	src := filepath.Join(t.TempDir(), "non-existent-tar-src")

	tarer := NewStandardTarer()
	err := tarer.TarIt(src, src+".tar.gz")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTarDstAlreadyExists(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	dst := filepath.Join(t.TempDir(), "existing-tar-dest")

	err := ioutil.WriteFile(src, []byte(src), 0644)
	require.NoError(t, err)
	err = ioutil.WriteFile(dst, []byte(dst), 0644)
	require.NoError(t, err)

	tarer := NewStandardTarer()
	err = tarer.TarIt(src, dst)
	require.ErrorIs(t, err, os.ErrExist)
}

func TestTarDstCreateErr(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	require.NoError(t, os.MkdirAll(src, 0755))
	dst := filepath.Join(t.TempDir(), "some-tar-dst")
	tarer := NewStandardTarer()
	errCreate := errors.New("Create error")
	tarer.OS.(*immuos.StandardOS).CreateF = func(name string) (*os.File, error) {
		return nil, errCreate
	}
	require.Equal(t, errCreate, tarer.TarIt(src, dst))
}

func TestTarWalkError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	dst := filepath.Join(t.TempDir(), "some-tar-dst")
	tarer := NewStandardTarer()
	errWalk := errors.New("Walk error")
	tarer.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	require.Equal(t, errWalk, tarer.TarIt(src, dst))
}

func TestTarWalkOpenError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	require.NoError(t, os.MkdirAll(src, 0755))
	srcFile := filepath.Join(src, "f.txt")
	ioutil.WriteFile(srcFile, []byte("f content"), 0644)
	dst := filepath.Join(t.TempDir(), "some-tar-dst")
	tarer := NewStandardTarer()
	errWalkOpen := errors.New("Walk open error")
	tarer.OS.(*immuos.StandardOS).OpenF = func(name string) (*os.File, error) {
		return nil, errWalkOpen
	}
	require.Equal(t, errWalkOpen, tarer.TarIt(src, dst))
}

func TestUnTarOpenError(t *testing.T) {
	tarer := NewStandardTarer()
	require.Error(t, tarer.UnTarIt(
		filepath.Join(t.TempDir(), "src"),
		filepath.Join(t.TempDir(), "dst"),
	))
}

func TestUnTarMdkirDst(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	require.NoError(t, os.MkdirAll(src, 0755))
	tarer := NewStandardTarer()
	errMkdirAll := errors.New("MkdirAll error")
	tarer.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		return errMkdirAll
	}
	require.Equal(t, errMkdirAll, tarer.UnTarIt(src, filepath.Join(t.TempDir(), "dst")))
}

func TestUnTarNonArchiveSrc(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-file.txt")
	require.NoError(t, ioutil.WriteFile(src, []byte("content"), 0644))
	tarer := NewStandardTarer()
	dst := filepath.Join(t.TempDir(), "dst")
	err := tarer.UnTarIt(src, dst)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestUnTarMkdirAllSubDirError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	ioutil.WriteFile(filepath.Join(srcSub, "some-file.txt"), []byte("content"), 0644)
	tarer := NewStandardTarer()
	dst := filepath.Join(t.TempDir(), "some-tar-dst.tar.gz")
	require.NoError(t, tarer.TarIt(src, dst))
	errMkdirAll := errors.New("MkdirAll subdir error")
	counter := 0
	tarer.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		counter++
		if counter == 2 {
			return errMkdirAll
		}
		return os.MkdirAll(path, perm)
	}
	err := tarer.UnTarIt(dst, filepath.Join(t.TempDir(), "dst2"))
	require.ErrorIs(t, err, errMkdirAll)
}

func TestUnTarOpenFileError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-tar-src")
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	ioutil.WriteFile(filepath.Join(srcSub, "some-file.txt"), []byte("content"), 0644)
	tarer := NewStandardTarer()
	dst := filepath.Join(t.TempDir(), "some-tar-dst.tar.gz")
	require.NoError(t, tarer.TarIt(src, dst))
	errOpenFile := errors.New("OpenFile error")
	tarer.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	err := tarer.UnTarIt(dst, filepath.Join(t.TempDir(), "dst2"))
	require.ErrorIs(t, err, errOpenFile)
}
