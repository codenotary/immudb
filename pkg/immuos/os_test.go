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

package immuos

import (
	"errors"
	"io/ioutil"
	"math"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	stdos "os"

	"github.com/stretchr/testify/require"
)

func TestStandardOS(t *testing.T) {
	os := NewStandardOS()

	// Create
	filename := "os_test_file"
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer stdos.Remove(filename)
	require.NotNil(t, f)
	createFOK := os.CreateF
	errCreate := errors.New("Create error")
	os.CreateF = func(name string) (*stdos.File, error) {
		return nil, errCreate
	}
	_, err = os.Create(filename)
	require.Equal(t, errCreate, err)
	os.CreateF = createFOK

	// Getwd
	ws, err := os.Getwd()
	require.NoError(t, err)
	require.NotEmpty(t, ws)
	getwdFOK := os.GetwdF
	errGetwd := errors.New("Getwd error")
	os.GetwdF = func() (string, error) {
		return "", errGetwd
	}
	_, err = os.Getwd()
	require.Equal(t, errGetwd, err)
	os.GetwdF = getwdFOK

	// Mkdir
	dirname := "os_test_dir"
	require.NoError(t, os.Mkdir(dirname, 0755))
	defer stdos.Remove(dirname)
	mkdirFOK := os.MkdirF
	errMkdir := errors.New("Mkdir error")
	os.MkdirF = func(name string, perm stdos.FileMode) error {
		return errMkdir
	}
	require.Equal(t, errMkdir, os.Mkdir(dirname, 0755))
	os.MkdirF = mkdirFOK

	// MkdirAll
	dirname2 := "os_test_dir2"
	require.NoError(t, os.MkdirAll(filepath.Join(dirname2, "os_test_subdir"), 0755))
	defer stdos.RemoveAll(dirname2)
	mkdirAllFOK := os.MkdirAllF
	errMkdirAll := errors.New("MkdirAll error")
	os.MkdirAllF = func(path string, perm stdos.FileMode) error {
		return errMkdirAll
	}
	require.Equal(t, errMkdirAll, os.MkdirAll(dirname2, 0755))
	os.MkdirAllF = mkdirAllFOK

	// Rename
	filename2 := filename + "_renamed"
	require.NoError(t, os.Rename(filename, filename2))
	defer stdos.Remove(filename2)
	renameFOK := os.RenameF
	errRename := errors.New("Rename error")
	os.RenameF = func(oldpath, newpath string) error {
		return errRename
	}
	require.Equal(t, errRename, os.Rename(filename, filename2))
	os.RenameF = renameFOK

	// Stat
	fi, err := os.Stat(filename2)
	require.NoError(t, err)
	require.NotNil(t, fi)
	statFOK := os.StatF
	errStat := errors.New("Stat error")
	os.StatF = func(name string) (stdos.FileInfo, error) {
		return nil, errStat
	}
	_, err = os.Stat(filename2)
	require.Equal(t, errStat, err)
	os.StatF = statFOK

	// Remove
	require.NoError(t, os.Remove(filename2))
	removeFOK := os.RemoveF
	errRemove := errors.New("Remove error")
	os.RemoveF = func(name string) error {
		return errRemove
	}
	require.Equal(t, errRemove, os.Remove(filename2))
	os.RemoveF = removeFOK

	// RemoveAll
	require.NoError(t, os.RemoveAll(dirname2))
	removeAllFOK := os.RemoveAllF
	errRemoveAll := errors.New("RemoveAll error")
	os.RemoveAllF = func(path string) error {
		return errRemoveAll
	}
	require.Equal(t, errRemoveAll, os.RemoveAll(filename2))
	os.RemoveAllF = removeAllFOK

	// Chown
	chownFOK := os.ChownF
	errChown := errors.New("Chown error")
	os.ChownF = func(name string, uid, gid int) error {
		return errChown
	}
	require.Equal(t, errChown, os.Chown("name", 1, 2))
	os.ChownF = chownFOK

	// Chmod
	chmodFOK := os.ChmodF
	errChmod := errors.New("Chmod error")
	os.ChmodF = func(name string, mode stdos.FileMode) error {
		return errChmod
	}
	require.Equal(t, errChmod, os.Chmod("name", 0644))
	os.ChmodF = chmodFOK

	// IsNotExist
	isNotExistFOK := os.IsNotExistF
	os.IsNotExistF = func(err error) bool {
		return true
	}
	require.True(t, os.IsNotExist(nil))
	os.IsNotExistF = isNotExistFOK

	// Open
	openFOK := os.OpenF
	errOpen := errors.New("Open error")
	os.OpenF = func(name string) (*stdos.File, error) {
		return nil, errOpen
	}
	_, err = os.Open("name")
	require.Equal(t, errOpen, err)
	os.OpenF = openFOK

	// OpenFile
	openFileFOK := os.OpenFileF
	errOpenFile := errors.New("OpenFile error")
	os.OpenFileF = func(name string, flag int, perm stdos.FileMode) (*stdos.File, error) {
		return nil, errOpenFile
	}
	_, err = os.OpenFile("name", 1, 0644)
	require.Equal(t, errOpenFile, err)
	os.OpenFileF = openFileFOK

	// Executable
	executableFOK := os.ExecutableF
	errExecutable := errors.New("Executable error")
	os.ExecutableF = func() (string, error) {
		return "", errExecutable
	}
	_, err = os.Executable()
	require.Equal(t, errExecutable, err)
	os.ExecutableF = executableFOK

	// Getpid
	getpidFOK := os.GetpidF
	os.GetpidF = func() int {
		return math.MinInt32
	}
	require.Equal(t, math.MinInt32, os.Getpid())
	os.GetpidF = getpidFOK
}

func TestStandardOSFilepathEmbedded(t *testing.T) {
	os := NewStandardOS()

	// Abs
	relPath := "some-path"
	absPath, err := os.Abs(relPath)
	require.NoError(t, err)
	require.Contains(t, absPath, relPath)
	require.Greater(t, len(absPath), len(relPath))
	absFOK := os.AbsF
	errAbs := errors.New("Abs error")
	os.AbsF = func(path string) (string, error) {
		return "", errAbs
	}
	_, err = os.Abs(relPath)
	require.Equal(t, errAbs, err)
	os.AbsF = absFOK
}

func TestStandardOSUserEmbedded(t *testing.T) {
	os := NewStandardOS()

	// Lookup ...
	lookupFOK := os.LookupF
	errLookup := errors.New("Lookup error")
	os.LookupF = func(username string) (*user.User, error) {
		return nil, errLookup
	}
	_, err := os.Lookup("username")
	require.Equal(t, errLookup, err)
	os.LookupF = lookupFOK
}

func TestStandardOSIoutilEmbedded(t *testing.T) {
	os := NewStandardOS()

	// ReadFile ...
	filename := "test-standard-os-ioutil-embedded-readfile"
	content := strings.ReplaceAll(filename, "-", " ")
	require.NoError(t, ioutil.WriteFile(filename, []byte(content), 0644))
	defer os.Remove(filename)
	readBytes, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}
