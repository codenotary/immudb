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

package fs

import (
	"errors"
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
	unarchive func() error) {
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	defer os.RemoveAll(srcDir)
	srcSubDir1 := filepath.Join(srcDir, "dir1")
	require.NoError(t, os.MkdirAll(srcSubDir1, 0755))
	srcSubDir2 := filepath.Join(srcSubDir1, "dir2")
	require.NoError(t, os.MkdirAll(srcSubDir2, 0755))
	file1 := [2]string{filepath.Join(srcSubDir1, "file1"), "file1\ncontent1"}
	file2 := [2]string{filepath.Join(srcSubDir2, "file2"), "file2\ncontent2"}
	require.NoError(t, ioutil.WriteFile(file1[0], []byte(file1[1]), 0644))
	require.NoError(t, ioutil.WriteFile(file2[0], []byte(file2[1]), 0644))

	require.NoError(t, archive())
	_, err := os.Stat(dst)
	require.NoError(t, err)
	defer os.Remove(dst)
	require.NoError(t, os.RemoveAll(srcDir))

	require.NoError(t, unarchive())
	fileContent1, err := ioutil.ReadFile(file1[0])
	require.NoError(t, err)
	require.Equal(t, file1[1], string(fileContent1))
	fileContent2, err := ioutil.ReadFile(file2[0])
	require.NoError(t, err)
	require.Equal(t, file2[1], string(fileContent2))
}

func testSrcNonExistent(t *testing.T, readSrc func() error) {
	err := readSrc()
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrNotExist), "expected: ErrNotExist, actual: %v", err)
}

func testDstAlreadyExists(t *testing.T, src string, dst string, writeDst func() error) {
	require.NoError(t, ioutil.WriteFile(src, []byte(src), 0644))
	defer os.Remove(src)
	require.NoError(t, ioutil.WriteFile(dst, []byte(dst), 0644))
	defer os.Remove(dst)
	err := writeDst()
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrExist), "expected: ErrExist, actual: %v", err)
}

func TestTarUnTar(t *testing.T) {
	src := "test-tar-untar"
	dst := src + ".tar.gz"
	tarer := NewStandardTarer()
	testArchiveUnarchive(
		t,
		src,
		dst,
		func() error { return tarer.TarIt(src, dst) },
		func() error { return tarer.UnTarIt(dst, ".") },
	)
}

func TestTarSrcNonExistent(t *testing.T) {
	src := "non-existent-tar-src"
	testSrcNonExistent(t, func() error {
		tarer := NewStandardTarer()
		return tarer.TarIt(src, src+".tar.gz")
	})
}

func TestTarDstAlreadyExists(t *testing.T) {
	src := "some-tar-src"
	dst := "existing-tar-dest"
	testDstAlreadyExists(t, src, dst, func() error {
		tarer := NewStandardTarer()
		return tarer.TarIt(src, dst)
	})
}

func TestTarDstCreateErr(t *testing.T) {
	src := "some-tar-src"
	require.NoError(t, os.MkdirAll(src, 0755))
	defer os.RemoveAll(src)
	dst := "some-tar-dst"
	tarer := NewStandardTarer()
	errCreate := errors.New("Create error")
	tarer.OS.(*immuos.StandardOS).CreateF = func(name string) (*os.File, error) {
		return nil, errCreate
	}
	require.Equal(t, errCreate, tarer.TarIt(src, dst))
}

func TestTarWalkError(t *testing.T) {
	src := "some-tar-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	dst := "some-tar-dst"
	defer os.RemoveAll(dst)
	tarer := NewStandardTarer()
	errWalk := errors.New("Walk error")
	tarer.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	require.Equal(t, errWalk, tarer.TarIt(src, dst))
}

func TestTarWalkOpenError(t *testing.T) {
	src := "some-tar-src"
	require.NoError(t, os.MkdirAll(src, 0755))
	defer os.RemoveAll(src)
	srcFile := filepath.Join(src, "f.txt")
	ioutil.WriteFile(srcFile, []byte("f content"), 0644)
	dst := "some-tar-dst"
	defer os.RemoveAll(dst)
	tarer := NewStandardTarer()
	errWalkOpen := errors.New("Walk open error")
	tarer.OS.(*immuos.StandardOS).OpenF = func(name string) (*os.File, error) {
		return nil, errWalkOpen
	}
	require.Equal(t, errWalkOpen, tarer.TarIt(src, dst))
}

func TestUnTarOpenError(t *testing.T) {
	tarer := NewStandardTarer()
	require.Error(t, tarer.UnTarIt("src", "dst"))
}

func TestUnTarMdkirDst(t *testing.T) {
	src := "some-tar-src"
	require.NoError(t, os.MkdirAll(src, 0755))
	defer os.Remove(src)
	tarer := NewStandardTarer()
	errMkdirAll := errors.New("MkdirAll error")
	tarer.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		return errMkdirAll
	}
	require.Equal(t, errMkdirAll, tarer.UnTarIt(src, "dst"))
}

func TestUnTarNonArchiveSrc(t *testing.T) {
	src := "some-file.txt"
	require.NoError(t, ioutil.WriteFile(src, []byte("content"), 0644))
	defer os.Remove(src)
	tarer := NewStandardTarer()
	dst := "dst"
	defer os.Remove(dst)
	require.Error(t, tarer.UnTarIt(src, dst))
}

func TestUnTarMkdirAllSubDirError(t *testing.T) {
	src := "some-tar-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	ioutil.WriteFile(filepath.Join(srcSub, "some-file.txt"), []byte("content"), 0644)
	tarer := NewStandardTarer()
	dst := "some-tar-dst.tar.gz"
	require.NoError(t, tarer.TarIt(src, dst))
	defer os.Remove(dst)
	errMkdirAll := errors.New("MkdirAll subdir error")
	counter := 0
	tarer.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		counter++
		if counter == 2 {
			return errMkdirAll
		}
		return os.MkdirAll(path, perm)
	}
	defer os.RemoveAll("dst2")
	require.Equal(t, errMkdirAll, tarer.UnTarIt(dst, "dst2"))
}

func TestUnTarOpenFileError(t *testing.T) {
	src := "some-tar-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	ioutil.WriteFile(filepath.Join(srcSub, "some-file.txt"), []byte("content"), 0644)
	tarer := NewStandardTarer()
	dst := "some-tar-dst.tar.gz"
	require.NoError(t, tarer.TarIt(src, dst))
	defer os.Remove(dst)
	errOpenFile := errors.New("OpenFile error")
	tarer.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	defer os.RemoveAll("dst2")
	require.Equal(t, errOpenFile, tarer.UnTarIt(dst, "dst2"))
}
