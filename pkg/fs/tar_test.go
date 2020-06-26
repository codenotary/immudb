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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTarUnTar(t *testing.T) {
	srcDir := "test-tar-untar"
	require.NoError(t, os.MkdirAll(srcDir, 0755))
	defer os.RemoveAll(srcDir)
	file1 := [2]string{filepath.Join(srcDir, "file1"), "file1\ncontent1"}
	file2 := [2]string{filepath.Join(srcDir, "file2"), "file2\ncontent2"}
	require.NoError(t, ioutil.WriteFile(file1[0], []byte(file1[1]), 0644))
	require.NoError(t, ioutil.WriteFile(file2[0], []byte(file2[1]), 0644))
	dst := srcDir + ".tar.gz"
	require.NoError(t, TarIt(srcDir, dst))
	_, err := os.Stat(dst)
	require.NoError(t, err)
	defer os.Remove(dst)
	require.NoError(t, os.RemoveAll(srcDir))

	require.NoError(t, UnTarIt(dst, "."))
	fileContent1, err := ioutil.ReadFile(file1[0])
	require.NoError(t, err)
	require.Equal(t, file1[1], string(fileContent1))
	fileContent2, err := ioutil.ReadFile(file2[0])
	require.NoError(t, err)
	require.Equal(t, file2[1], string(fileContent2))
}
