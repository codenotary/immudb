/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package homedir

import (
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteFileToUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()
	err := hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.FileExists(t, filepath.Join(user.HomeDir, pathToFile))
	assert.NoError(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"

	user, _ := user.Current()
	exists, err := hds.FileExistsInUserHomeDir(filepath.Join(user.HomeDir, pathToFile))
	assert.False(t, exists)
	assert.NoError(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)
	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	assert.True(t, exists)
	assert.NoError(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestReadFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()

	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.ErrorIs(t, err, os.ErrNotExist)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)
	defer os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))

	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.NotEmpty(t, strcontent)
}

func TestDeleteFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)

	pathToFile := "testfile"
	user, _ := user.Current()
	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.ErrorIs(t, err, os.ErrNotExist)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)
	defer os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))

	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.NoFileExists(t, filepath.Join(user.HomeDir, pathToFile))
}

func TestWriteDirFileToUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	err := hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)
	assert.FileExists(t, pathToFile)
}

func TestDirFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	exists, err := hds.FileExistsInUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.False(t, exists)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)

	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestDirFileFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)

	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.NotEmpty(t, strcontent)
}

func TestDeleteDirFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := filepath.Join(t.TempDir(), "testfile")

	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)

	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.NoError(t, err)

	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.NoError(t, err)
	assert.NoFileExists(t, pathToFile)
}
