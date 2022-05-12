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
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/stretchr/testify/require"
)

func TestCopyFile(t *testing.T) {
	src := "test-copy-file-source.txt"
	require.NoError(
		t, ioutil.WriteFile(src, []byte("test CopyFile file content"), 0644))
	defer os.Remove(src)
	dst := "test-copy-file-destination.txt"
	defer os.Remove(dst)
	copier := NewStandardCopier()
	require.NoError(t, copier.CopyFile(src, dst))
}

func TestCopyFileSrcNonExistent(t *testing.T) {
	testSrcNonExistent(t, func() error {
		copier := NewStandardCopier()
		return copier.CopyFile("non-existent-copy-file-src", "non-existent-copy-file-dst")
	})
}

func TestCopyFileSrcIsDir(t *testing.T) {
	src := "test-copy-file-source-is-dir"
	defer os.RemoveAll(src)
	require.NoError(t, os.MkdirAll(src, 0755))
	copier := NewStandardCopier()
	err := copier.CopyFile(src, "test-copy-file-source-is-dir-dst")
	if err == nil || !strings.Contains(err.Error(), "is a directory") {
		t.Errorf("want 'is a directory' error, got %v", err)
	}
}

func TestCopyFileOpenDstError(t *testing.T) {
	src := "test-copy-file-open-dst-error-src"
	require.NoError(
		t, ioutil.WriteFile(src, []byte("test CopyFile open dst error src file content"), 0644))
	defer os.Remove(src)
	copier := NewStandardCopier()
	errOpenFile := errors.New("OpenFile error")
	copier.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	err := copier.CopyFile(src, "test-copy-file-open-dst-error-dst")
	require.Equal(t, errOpenFile, err)
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

	copier := NewStandardCopier()
	require.NoError(t, copier.CopyDir(srcDir, dst))

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
		copier := NewStandardCopier()
		return copier.CopyDir("non-existent-copy-file-src", "non-existent-copy-file-dst")
	})
}

func TestCopyDirSrcNotDir(t *testing.T) {
	src := "test-copy-dir-src-not-dir-src"
	defer os.Remove(src)
	require.NoError(t, ioutil.WriteFile(src, []byte(src), 0644))
	copier := NewStandardCopier()
	err := copier.CopyDir(src, "test-copy-dir-src-not-dir-dst")
	if err == nil || !strings.Contains(err.Error(), "is not a directory") {
		t.Errorf("want 'is not a directory' error, got %v", err)
	}
}

func TestCopyDirDstAlreadyExists(t *testing.T) {
	src := "test-copy-dir-dst-already-exists-src"
	require.NoError(t, os.Mkdir(src, 0755))
	defer os.Remove(src)
	dst := "test-copy-dir-dst-already-exists-dst"
	require.NoError(t, os.Mkdir(dst, 0755))
	defer os.Remove(dst)
	copier := NewStandardCopier()
	err := copier.CopyDir(src, dst)
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrExist), "expected: ErrExist, actual: %v", err)
}

func TestCopyDirDstStatError(t *testing.T) {
	src := "test-copy-dir-dst-stat-error-src"
	require.NoError(t, os.Mkdir(src, 0755))
	defer os.Remove(src)
	dst := "test-copy-dir-dst-stat-error-dst"
	copier := NewStandardCopier()
	errDstStat := errors.New("dst Stat error")
	copier.OS.(*immuos.StandardOS).StatF = func(name string) (os.FileInfo, error) {
		if name != dst {
			return os.Stat(name)
		}
		return nil, errDstStat
	}
	err := copier.CopyDir(src, dst)
	require.Equal(t, errDstStat, err)
}

func TestCopyDirWalkError(t *testing.T) {
	src := "test-copy-dir-walk-error-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	dst := "test-copy-dir-walk-error-dst"
	defer os.RemoveAll(dst)
	copier := NewStandardCopier()
	errWalk := errors.New("walk error")
	copier.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	err := copier.CopyDir(src, dst)
	require.Equal(t, errWalk, err)
}
