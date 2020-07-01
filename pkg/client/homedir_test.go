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

package client

import (
	"github.com/mitchellh/go-homedir"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileToUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "testfile"
	h, _ := homedir.Dir()
	err := WriteFileToUserHomeDir(content, pathToFile)
	assert.FileExists(t, filepath.Join(h, pathToFile))
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(h, pathToFile))
}

func TestFileExistsInUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "testfile"
	h, _ := homedir.Dir()
	exists, err := FileExistsInUserHomeDir(filepath.Join(h, pathToFile))
	assert.False(t, exists)
	assert.Nil(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	exists, err = FileExistsInUserHomeDir(pathToFile)
	assert.True(t, exists)
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(h, pathToFile))
}

func TestReadFileFromUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "testfile"
	h, _ := homedir.Dir()
	_, err := ReadFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	strcontent, err := ReadFileFromUserHomeDir(pathToFile)
	assert.NotEmpty(t, strcontent)
	assert.Nil(t, err)
	os.RemoveAll(filepath.Join(h, pathToFile))
}

func TestDeleteFileFromUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "testfile"
	h, _ := homedir.Dir()
	err := DeleteFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	err = DeleteFileFromUserHomeDir(pathToFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, filepath.Join(h, pathToFile))
}

func TestWriteDirFileToUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "./testfile"
	err := WriteFileToUserHomeDir(content, pathToFile)
	assert.FileExists(t, pathToFile)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDirFileExistsInUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "./testfile"
	exists, err := FileExistsInUserHomeDir(pathToFile)
	assert.False(t, exists)
	assert.Nil(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	exists, err = FileExistsInUserHomeDir(pathToFile)
	assert.True(t, exists)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDirFileFileFromUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "./testfile"
	_, err := ReadFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	strcontent, err := ReadFileFromUserHomeDir(pathToFile)
	assert.NotEmpty(t, strcontent)
	assert.Nil(t, err)
	os.RemoveAll(pathToFile)
}

func TestDeleteDirFileFromUserHomeDir(t *testing.T) {
	content := []byte(`t`)
	pathToFile := "./testfile"
	err := DeleteFileFromUserHomeDir(pathToFile)
	assert.Error(t, err)
	err = WriteFileToUserHomeDir(content, pathToFile)
	err = DeleteFileFromUserHomeDir(pathToFile)
	assert.Nil(t, err)
	assert.NoFileExists(t, pathToFile)
}
