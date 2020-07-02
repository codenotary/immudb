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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyFile(t *testing.T) {
	src := "test-copy-file-source.txt"
	require.NoError(
		t, ioutil.WriteFile(src, []byte("test CopyFile file content"), 0644))
	defer os.Remove(src)
	dst := "test-copy-file-destination.txt"
	defer os.Remove(dst)
	require.NoError(t, CopyFile(src, dst))
}

func TestCopyFileSrcNonExistent(t *testing.T) {
	testSrcNonExistent(t, func() error {
		return CopyFile("non-existent-copy-file-src", "non-existent-copy-file-dst")
	})
}

func TestCopyFileSrcIsDir(t *testing.T) {
	src := "test-copy-file-source-is-dir"
	defer os.RemoveAll(src)
	require.NoError(t, os.MkdirAll(src, 0755))
	err := CopyFile(src, "test-copy-file-source-is-dir-dst")
	require.Error(t, err)
	require.True(
		t,
		strings.HasSuffix(err.Error(), "is a directory"),
		"expected 'is a directory' error, actual: %v", err)
}

func TestCopyDir(t *testing.T) {
	srcDir := "test-copy-dir-source"
	defer os.RemoveAll(srcDir)

	srcSubDir1 := filepath.Join(srcDir, "dir1")
	srcSubDir2 := filepath.Join(srcDir, "dir2")
	require.NoError(t, os.MkdirAll(srcSubDir1, 0755))
	require.NoError(t, os.MkdirAll(srcSubDir2, 0755))

	file0 := [2]string{filepath.Join(srcDir, "file0"), "file0\ncontent0"}
	file1 := [2]string{filepath.Join(srcSubDir1, "file1"), "file1\ncontent1"}
	file2 := [2]string{filepath.Join(srcSubDir2, "file2"), "file2\ncontent2"}
	require.NoError(t, ioutil.WriteFile(file0[0], []byte(file0[1]), 0644))
	require.NoError(t, ioutil.WriteFile(file1[0], []byte(file1[1]), 0644))
	require.NoError(t, ioutil.WriteFile(file2[0], []byte(file2[1]), 0644))

	dst := "test-copy-dir-destination"
	defer os.RemoveAll(dst)

	require.NoError(t, CopyDir(srcDir, dst))

	fileContent0, err := ioutil.ReadFile(file0[0])
	require.NoError(t, err)
	require.Equal(t, file0[1], string(fileContent0))
	fileContent1, err := ioutil.ReadFile(file1[0])
	require.NoError(t, err)
	require.Equal(t, file1[1], string(fileContent1))
	fileContent2, err := ioutil.ReadFile(file2[0])
	require.NoError(t, err)
	require.Equal(t, file2[1], string(fileContent2))
}

func TestCopyDirSrcNonExistent(t *testing.T) {
	testSrcNonExistent(t, func() error {
		return CopyDir("non-existent-copy-file-src", "non-existent-copy-file-dst")
	})
}

func TestCopyDirSrcNotDir(t *testing.T) {
	src := "test-copy-dir-src-not-dir-src"
	defer os.Remove(src)
	require.NoError(t, ioutil.WriteFile(src, []byte(src), 0644))
	err := CopyDir(src, "test-copy-dir-src-not-dir-dst")
	require.Error(t, err)
	require.Equal(t, "source is not a directory", err.Error())
}

func TestCopyDirDstAlreadyExists(t *testing.T) {
	src := "test-copy-dir-dst-already-exists-src"
	require.NoError(t, os.Mkdir(src, 0755))
	defer os.Remove(src)
	dst := "test-copy-dir-dst-already-exists-dst"
	require.NoError(t, os.Mkdir(dst, 0755))
	defer os.Remove(dst)
	err := CopyDir(src, dst)
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrExist), "expected: ErrExist, actual: %v", err)
}
