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
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"

	user, _ := user.Current()
	exists, err := hds.FileExistsInUserHomeDir(filepath.Join(user.HomeDir, pathToFile))
	assert.False(t, exists)
	assert.Nil(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.Nil(t, err)
	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	assert.True(t, exists)
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestReadFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()
	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.NotEmpty(t, strcontent)
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(user.HomeDir, pathToFile))
}

func TestDeleteFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "testfile"
	user, _ := user.Current()
	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.Nil(t, err)
	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, filepath.Join(user.HomeDir, pathToFile))
}

func TestWriteDirFileToUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "./testfile"
	err := hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.FileExists(t, pathToFile)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDirFileExistsInUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "./testfile"
	exists, err := hds.FileExistsInUserHomeDir(pathToFile)
	assert.False(t, exists)
	assert.Nil(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.Nil(t, err)
	exists, err = hds.FileExistsInUserHomeDir(pathToFile)
	assert.True(t, exists)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDirFileFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "./testfile"
	_, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.Nil(t, err)
	strcontent, err := hds.ReadFileFromUserHomeDir(pathToFile)
	assert.NotEmpty(t, strcontent)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDeleteDirFileFromUserHomeDir(t *testing.T) {
	hds := NewHomedirService()
	content := []byte(`t`)
	pathToFile := "./testfile"
	err := hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = hds.WriteFileToUserHomeDir(content, pathToFile)
	assert.Nil(t, err)
	err = hds.DeleteFileFromUserHomeDir(pathToFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, pathToFile)
}
