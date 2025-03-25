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

package streamutils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestStreamUtilsFiles(t *testing.T) {
	tmpdir := t.TempDir()

	// stat will fail
	_, err := GetKeyValuesFromFiles(filepath.Join(tmpdir, "non-existant"))
	require.ErrorIs(t, err, syscall.ENOENT)

	unreadable := filepath.Join(tmpdir, "dir")
	os.Mkdir(unreadable, 200)
	// open will fail
	_, err = GetKeyValuesFromFiles(unreadable)
	require.ErrorIs(t, err, unix.EACCES)

	valid := filepath.Join(tmpdir, "data")
	err = ioutil.WriteFile(valid, []byte("content"), 0644)
	require.NoError(t, err)
	kvs, err := GetKeyValuesFromFiles(valid)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
}
