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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/stretchr/testify/require"
)

func TestCopyFile(t *testing.T) {
	src := filepath.Join(t.TempDir(), "test-copy-file-source.txt")
	err := ioutil.WriteFile(src, []byte("test CopyFile file content"), 0644)
	require.NoError(t, err)

	dst := filepath.Join(t.TempDir(), "test-copy-file-destination.txt")
	copier := NewStandardCopier()
	err = copier.CopyFile(src, dst)
	require.NoError(t, err)
}

func TestCopyFileSrcNonExistent(t *testing.T) {
	copier := NewStandardCopier()
	err := copier.CopyFile(
		filepath.Join(t.TempDir(), "non-existent-copy-file-src"),
		filepath.Join(t.TempDir(), "non-existent-copy-file-dst"),
	)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCopyFileSrcIsDir(t *testing.T) {
	src := t.TempDir()
	copier := NewStandardCopier()
	err := copier.CopyFile(src, filepath.Join(t.TempDir(), "test-copy-file-source-is-dir-dst"))
	require.ErrorContains(t, err, "is a directory")
}

func TestCopyFileOpenDstError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "test-copy-file-open-dst-error-src")
	err := ioutil.WriteFile(src, []byte("test CopyFile open dst error src file content"), 0644)
	require.NoError(t, err)
	copier := NewStandardCopier()
	errOpenFile := errors.New("OpenFile error")
	copier.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	err = copier.CopyFile(src, filepath.Join(t.TempDir(), "test-copy-file-open-dst-error-dst"))
	require.ErrorIs(t, err, errOpenFile)
}

func TestCopyDir(t *testing.T) {
	srcDir := t.TempDir()

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

	dst := filepath.Join(t.TempDir(), "dst")

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
	copier := NewStandardCopier()
	err := copier.CopyDir(
		filepath.Join(t.TempDir(), "non-existent-copy-file-src"),
		filepath.Join(t.TempDir(), "non-existent-copy-file-dst"),
	)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCopyDirSrcNotDir(t *testing.T) {
	src := filepath.Join(t.TempDir(), "test-copy-dir-src-not-dir-src")
	require.NoError(t, ioutil.WriteFile(src, []byte(src), 0644))
	copier := NewStandardCopier()
	err := copier.CopyDir(src, filepath.Join(t.TempDir(), "test-copy-dir-src-not-dir-dst"))
	require.ErrorContains(t, err, "is not a directory")
}

func TestCopyDirDstAlreadyExists(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	copier := NewStandardCopier()
	err := copier.CopyDir(src, dst)
	require.ErrorIs(t, err, os.ErrExist)
}

func TestCopyDirDstStatError(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "test-copy-dir-dst-stat-error-dst")
	copier := NewStandardCopier()
	errDstStat := errors.New("dst Stat error")
	copier.OS.(*immuos.StandardOS).StatF = func(name string) (os.FileInfo, error) {
		if name != dst {
			return os.Stat(name)
		}
		return nil, errDstStat
	}
	err := copier.CopyDir(src, dst)
	require.ErrorIs(t, err, errDstStat)
}

func TestCopyDirWalkError(t *testing.T) {
	src := t.TempDir()
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))

	dst := filepath.Join(t.TempDir(), "test-copy-dir-walk-error-dst")
	copier := NewStandardCopier()

	errWalk := errors.New("walk error")
	copier.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	err := copier.CopyDir(src, dst)
	require.ErrorIs(t, err, errWalk)
}
