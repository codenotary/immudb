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

package fileutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()

	err := SyncDir(dir)
	require.NoError(t, err)
}

func TestSyncDirMultiple(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	err := SyncDir(dir1, dir2)
	require.NoError(t, err)
}

func TestSyncDirNonExistent(t *testing.T) {
	err := SyncDir("/nonexistent/path")
	require.Error(t, err)
}

func TestSyncDirEmpty(t *testing.T) {
	err := SyncDir()
	require.NoError(t, err)
}

func TestFdatasync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.dat")

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Write([]byte("test data"))
	require.NoError(t, err)

	err = Fdatasync(f)
	require.NoError(t, err)
}
