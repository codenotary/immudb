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

package immuos

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandardFilepath(t *testing.T) {
	fp := NewStandardFilepath()

	// Abs
	relPath := "some-path"
	absPath, err := fp.Abs(relPath)
	require.NoError(t, err)
	require.Contains(t, absPath, relPath)
	require.Greater(t, len(absPath), len(relPath))
	absFOK := fp.AbsF
	errAbs := errors.New("Abs error")
	fp.AbsF = func(path string) (string, error) {
		return "", errAbs
	}
	_, err = fp.Abs(relPath)
	require.ErrorIs(t, err, errAbs)
	fp.AbsF = absFOK

	// Base
	path := filepath.Join("some", "file", "path")
	require.Equal(t, "path", fp.Base(path))
	baseFOK := fp.BaseF
	otherBase := "other"
	fp.BaseF = func(path string) string {
		return otherBase
	}
	require.Equal(t, otherBase, fp.Base(path))
	fp.BaseF = baseFOK

	// Ext
	pathWithExt := filepath.Join("some", "file.ext")
	require.Equal(t, ".ext", fp.Ext(pathWithExt))
	extFOK := fp.ExtF
	otherExt := ".otherExt"
	fp.ExtF = func(path string) string {
		return otherExt
	}
	require.Equal(t, otherExt, fp.Ext(pathWithExt))
	fp.ExtF = extFOK

	// Dir
	pathToDir := filepath.Join("dir", "subdir")
	pathToFile := filepath.Join(pathToDir, "file.txt")
	require.Equal(t, pathToDir, fp.Dir(pathToFile))
	dirFOK := fp.DirF
	otherPathToDir := "other-path-to-dir"
	fp.DirF = func(path string) string {
		return otherPathToDir
	}
	require.Equal(t, otherPathToDir, fp.Dir(pathToFile))
	fp.DirF = dirFOK

	// Walk
	walkFOK := fp.WalkF
	errWalk := errors.New("Walk error")
	fp.WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return errWalk
	}
	err = fp.Walk("root", func(path string, info os.FileInfo, err error) error { return nil })
	require.ErrorIs(t, err, errWalk)
	fp.WalkF = walkFOK

	// FromSlash ...
	fromSlashFOK := fp.FromSlashF
	fp.FromSlashF = func(path string) string {
		return "fromslash"
	}
	require.Equal(t, "fromslash", fp.FromSlash("slash"))
	fp.FromSlashF = fromSlashFOK

	// Join ...
	joinFOK := fp.JoinF
	fp.JoinF = func(elem ...string) string {
		return "joined"
	}
	require.Equal(t, "joined", fp.Join("pie", "ces"))
	fp.JoinF = joinFOK

	// Clean ...
	cleanFOK := fp.CleanF
	fp.CleanF = func(path string) string {
		return "path"
	}
	require.Equal(t, "path", fp.Clean("/../path"))
	fp.CleanF = cleanFOK

	// Split ...
	splitFOK := fp.SplitF
	fp.SplitF = func(path string) (dir, file string) {
		dir = "someDir"
		file = "someFile.txt"
		return
	}
	splitDir, splitFile := fp.Split("a/b.txt")
	require.Equal(t, "someDir", splitDir)
	require.Equal(t, "someFile.txt", splitFile)
	fp.SplitF = splitFOK
}
