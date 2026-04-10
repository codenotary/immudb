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

package fs

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeExtractPath(t *testing.T) {
	dst := "/tmp/extract"

	t.Run("valid relative path", func(t *testing.T) {
		target, err := sanitizeExtractPath(dst, "subdir/file.txt")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(dst, "subdir/file.txt"), target)
	})

	t.Run("valid simple filename", func(t *testing.T) {
		target, err := sanitizeExtractPath(dst, "file.txt")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(dst, "file.txt"), target)
	})

	t.Run("reject absolute path", func(t *testing.T) {
		_, err := sanitizeExtractPath(dst, "/etc/passwd")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "illegal absolute path")
	})

	t.Run("reject dotdot traversal", func(t *testing.T) {
		_, err := sanitizeExtractPath(dst, "../escape")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "illegal path traversal")
	})

	t.Run("reject nested dotdot traversal", func(t *testing.T) {
		_, err := sanitizeExtractPath(dst, "foo/../../escape")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "illegal path traversal")
	})

	t.Run("reject dotdot only", func(t *testing.T) {
		_, err := sanitizeExtractPath(dst, "..")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "illegal path traversal")
	})

	t.Run("reject deep traversal", func(t *testing.T) {
		_, err := sanitizeExtractPath(dst, "../../../etc/shadow")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "illegal path traversal")
	})

	t.Run("valid path with dot segment", func(t *testing.T) {
		target, err := sanitizeExtractPath(dst, "subdir/./file.txt")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(dst, "subdir/file.txt"), target)
	})
}
