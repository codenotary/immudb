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

package homedir

import (
	"os"
	"os/user"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFileToUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()
	err := hds.WriteFileToUserHomeDir(content, pathToFile)
	require.FileExists(t, filepath.Join(user.HomeDir, pathToFile))
	require.NoError(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"

	user, _ := user.Current()
	exists, err := hds.FileExistsInUserHomeDir(filepath.Join(user.HomeDir, pathToFile))
	require.False(t, exists)
	require.NoError(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)
	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	require.True(t, exists)
	require.NoError(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestReadFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()

	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	require.ErrorIs(t, err, os.ErrNotExist)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)
	defer os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))

	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.NotEmpty(t, strcontent)
}

func TestDeleteFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)

	pathToFile := "testfile"
	user, _ := user.Current()
	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	require.ErrorIs(t, err, os.ErrNotExist)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)
	defer os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))

	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(user.HomeDir, pathToFile))
}

func TestWriteDirFileToUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	err := hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)
	require.FileExists(t, pathToFile)
}

func TestDirFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	exists, err := hds.FileExistsInUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.False(t, exists)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)

	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDirFileFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	require.ErrorIs(t, err, syscall.ENOENT)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)

	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.NotEmpty(t, strcontent)
}

func TestDeleteDirFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	require.ErrorIs(t, err, syscall.ENOENT)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	require.NoError(t, err)

	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	require.NoError(t, err)
	require.NoFileExists(t, pathToFile)
}
