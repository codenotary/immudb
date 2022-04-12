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

func TestZipUnZip(t *testing.T) {
	src := "test-zip-unzip"
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
	nonExistentSrc := "non-existent-zip-src"
	testSrcNonExistent(t, func() error {
		ziper := NewStandardZiper()
		return ziper.ZipIt(nonExistentSrc, nonExistentSrc+".zip", ZipBestSpeed)
	})
}

func TestZipDstAlreadyExists(t *testing.T) {
	src := "some-zip-src"
	dst := "existing-zip-dest"
	testDstAlreadyExists(t, src, dst, func() error {
		ziper := NewStandardZiper()
		return ziper.ZipIt(src, dst, ZipBestSpeed)
	})
}

func TestZipDstCreateError(t *testing.T) {
	src := "some-zip-src"
	require.NoError(t, os.MkdirAll(src, 0755))
	defer os.Remove(src)
	ziper := NewStandardZiper()
	errCreate := errors.New("Create error")
	ziper.OS.(*immuos.StandardOS).CreateF = func(name string) (*os.File, error) {
		return nil, errCreate
	}
	require.Equal(t, errCreate, ziper.ZipIt(src, "dst", ZipBestSpeed))
}

func TestZipWalkError(t *testing.T) {
	src := "some-zip-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	require.NoError(t, ioutil.WriteFile(filepath.Join(srcSub, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()
	errWalk := errors.New("Walk error")
	ziper.OS.(*immuos.StandardOS).WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return walkFn("", nil, errWalk)
	}
	defer os.Remove("dst")
	require.Equal(t, errWalk, ziper.ZipIt(src, "dst", ZipBestSpeed))
}

func TestZipWalkOpenError(t *testing.T) {
	src := "some-zip-src"
	srcSub := filepath.Join(src, "subdir")
	require.NoError(t, os.MkdirAll(srcSub, 0755))
	defer os.RemoveAll(src)
	require.NoError(t, ioutil.WriteFile(filepath.Join(srcSub, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()
	errWalkOpen := errors.New("Walk open error")
	ziper.OS.(*immuos.StandardOS).OpenF = func(name string) (*os.File, error) {
		return nil, errWalkOpen
	}
	defer os.Remove("dst")
	require.Equal(t, errWalkOpen, ziper.ZipIt(src, "dst", ZipBestSpeed))
}

func TestUnZipSrcNotZipArchive(t *testing.T) {
	src := "somefile.txt"
	require.NoError(t, ioutil.WriteFile(src, []byte("content"), 0644))
	defer os.Remove(src)
	zipper := NewStandardZiper()
	defer os.Remove("dst")
	require.Error(t, zipper.UnZipIt(src, "dst"))
}

func TestUnZipEntryMkdirAllError(t *testing.T) {
	src := "some-zip-src"
	require.NoError(t, os.Mkdir(src, 0755))
	defer os.RemoveAll(src)
	require.NoError(t, ioutil.WriteFile(filepath.Join(src, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()
	dst := "some-zip.zip"
	defer os.Remove(dst)
	require.NoError(t, ziper.ZipIt(src, dst, ZipBestSpeed))
	errMkdirAll := errors.New("MkdirAll error")
	counter := 0
	ziper.OS.(*immuos.StandardOS).MkdirAllF = func(path string, perm os.FileMode) error {
		counter++
		if counter == 2 {
			return errMkdirAll
		}
		return os.MkdirAll(path, perm)
	}
	dstUnZip := "unzip-dst"
	defer os.RemoveAll(dstUnZip)
	require.Equal(t, errMkdirAll, ziper.UnZipIt(dst, dstUnZip))
}

func TestUnZipEntryOpenFileError(t *testing.T) {
	src := "some-zip-src"
	require.NoError(t, os.Mkdir(src, 0755))
	defer os.RemoveAll(src)
	require.NoError(t, ioutil.WriteFile(filepath.Join(src, "somefile.txt"), []byte("content"), 0644))
	ziper := NewStandardZiper()
	dst := "some-zip.zip"
	defer os.Remove(dst)
	require.NoError(t, ziper.ZipIt(src, dst, ZipBestSpeed))
	errOpenFile := errors.New("OpenFile entry error")
	ziper.OS.(*immuos.StandardOS).OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errOpenFile
	}
	dstUnZip := "unzip-dst"
	defer os.RemoveAll(dstUnZip)
	require.Equal(t, errOpenFile, ziper.UnZipIt(dst, dstUnZip))
}
