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

package fs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

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
	testArchiveUnarchive(
		t,
		src,
		dst,
		func() error { return TarIt(src, dst) },
		func() error { return UnTarIt(dst, ".") },
	)
}

func TestTarSrcNonExistent(t *testing.T) {
	src := "non-existent-tar-src"
	testSrcNonExistent(t, func() error {
		return TarIt(src, src+".tar.gz")
	})
}

func TestTarDstAlreadyExists(t *testing.T) {
	src := "some-tar-src"
	dst := "existing-tar-dest"
	testDstAlreadyExists(t, src, dst, func() error {
		return TarIt(src, dst)
	})
}
