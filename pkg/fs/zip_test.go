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

package fs

import (
	"archive/zip"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/stretchr/testify/require"
)

func TestZipUnZip(t *testing.T) {
	src := filepath.Join(t.TempDir(), "test-zip-unzip")
	dst := src + ".zip"
	ziper := NewStandardZiper()
	testArchiveUnarchive(
		t,
		src,
		dst,
		func() error { return ziper.ZipIt(src, dst, ZipBestSpeed) },
		func() error { return ziper.UnZipIt(dst, ".") },
	)
}

func TestZipSrcNonExistent(t *testing.T) {
	nonExistentSrc := filepath.Join(t.TempDir(), "non-existent-zip-src")

	ziper := NewStandardZiper()
	err := ziper.ZipIt(nonExistentSrc, nonExistentSrc+".zip", ZipBestSpeed)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestZipDstAlreadyExists(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-zip-src")
	dst := filepath.Join(t.TempDir(), "existing-zip-dest")

	err := ioutil.WriteFile(src, []byte(src), 0644)
	require.NoError(t, err)

	err = ioutil.WriteFile(dst, []byte(dst), 0644)
	require.NoError(t, err)

	ziper := NewStandardZiper()
	err = ziper.ZipIt(src, dst, ZipBestSpeed)
	require.ErrorIs(t, err, os.ErrExist)
}

func TestZipDstCreateError(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "dst")
	ziper := NewStandardZiper()
	errCreate := errors.New("Create error")
	ziper.OS.(*immuos.StandardOS).CreateF = func(name string) (*os.File, error) {
		return nil, errCreate
	}
	err := ziper.ZipIt(src, dst, ZipBestSpeed)
	require.ErrorIs(t, err, errCreate)
}

func TestZipWalkError(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "dst")
	srcSub := filepath.Join(src, "subdir")

	err := os.MkdirAll(srcSub, 0755)
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(srcSub, "somefile.txt"), []byte("content"), 0644)
	require.NoError(t, err)

	ziper := NewStandardZiper()
	errWalk := errors.New("Walk error")
	ziper.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	err = ziper.ZipIt(src, dst, ZipBestSpeed)
	require.ErrorIs(t, err, errWalk)
}

func TestZipWalkOpenError(t *testing.T) {
	src := t.TempDir()
	dst := filepath.Join(t.TempDir(), "dst")

	srcSub := filepath.Join(src, "subdir")
	err := os.MkdirAll(srcSub, 0755)
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(srcSub, "somefile.txt"), []byte("content"), 0644)
	require.NoError(t, err)

	ziper := NewStandardZiper()
	errWalkOpen := errors.New("Walk open error")
	ziper.OS.(*immuos.StandardOS).OpenF = func(name string) (*os.File, error) {
		return nil, errWalkOpen
	}
	err = ziper.ZipIt(src, dst, ZipBestSpeed)
	require.ErrorIs(t, err, errWalkOpen)
}

func TestUnZipSrcNotZipArchive(t *testing.T) {
	src := filepath.Join(t.TempDir(), "somefile.txt")
	dst := filepath.Join(t.TempDir(), "dst")
	require.NoError(t, ioutil.WriteFile(src, []byte("content"), 0644))
	zipper := NewStandardZiper()
	err := zipper.UnZipIt(src, dst)
	require.ErrorIs(t, err, zip.ErrFormat)
}

func TestUnZipEntryMkdirAllError(t *testing.T) {
	src := t.TempDir()

	require.NoError(t, ioutil.WriteFile(filepath.Join(src, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()

	dst := filepath.Join(t.TempDir(), "some-zip.zip")
	err := ziper.ZipIt(src, dst, ZipBestSpeed)
	require.NoError(t, err)

	errMkdirAll := errors.New("MkdirAll error")
	counter := 0
	ziper.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		counter++
		if counter == 2 {
			return errMkdirAll
		}
		return os.MkdirAll(path, perm)
	}
	dstUnZip := filepath.Join(t.TempDir(), "unzip-dst")
	err = ziper.UnZipIt(dst, dstUnZip)
	require.ErrorIs(t, err, errMkdirAll)
}

func TestUnZipEntryOpenFileError(t *testing.T) {
	src := filepath.Join(t.TempDir(), "some-zip-src")
	require.NoError(t, os.Mkdir(src, 0755))
	require.NoError(t, ioutil.WriteFile(filepath.Join(src, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()
	dst := filepath.Join(t.TempDir(), "some-zip.zip")
	require.NoError(t, ziper.ZipIt(src, dst, ZipBestSpeed))
	errOpenFile := errors.New("OpenFile entry error")
	ziper.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	dstUnZip := filepath.Join(t.TempDir(), "unzip-dst")
	err := ziper.UnZipIt(dst, dstUnZip)
	require.ErrorIs(t, err, errOpenFile)
}
