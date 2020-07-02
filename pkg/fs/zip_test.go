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

func TestZipUnZip(t *testing.T) {
	srcDir := "test-zip-unzip"

	// TODO OGG: extract this to a function in tar_test and reuse it in both
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

	dst := srcDir + ".zip"
	require.NoError(t, ZipIt(srcDir, dst, ZipBestSpeed))
	_, err := os.Stat(dst)
	require.NoError(t, err)
	defer os.Remove(dst)
	require.NoError(t, os.RemoveAll(srcDir))

	require.NoError(t, UnZipIt(dst, "."))
	fileContent1, err := ioutil.ReadFile(file1[0])
	require.NoError(t, err)
	require.Equal(t, file1[1], string(fileContent1))
	fileContent2, err := ioutil.ReadFile(file2[0])
	require.NoError(t, err)
	require.Equal(t, file2[1], string(fileContent2))
}

func TestZipSrcNonExistent(t *testing.T) {
	nonExistentSrc := "non-existent"
	err := ZipIt(nonExistentSrc, nonExistentSrc+".zip", ZipBestSpeed)
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrNotExist), "expected: ErrNotExist, actual: %v", err)
}

func TestZipDstAlreadyExists(t *testing.T) {
	someSrc := "some-src"
	require.NoError(t, ioutil.WriteFile(someSrc, []byte(someSrc), 0644))
	defer os.Remove(someSrc)
	existingDest := "existing-dest"
	require.NoError(t, ioutil.WriteFile(existingDest, []byte(existingDest), 0644))
	defer os.Remove(existingDest)
	err := ZipIt(someSrc, existingDest, ZipBestSpeed)
	require.Error(t, err)
	require.Truef(
		t, errors.Is(err, os.ErrExist), "expected: ErrExist, actual: %v", err)
}
