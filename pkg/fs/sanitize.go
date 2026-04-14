/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"fmt"
	"path/filepath"
	"strings"
)

// sanitizeExtractPath validates that an archive member name does not escape
// the destination directory via path traversal (e.g. "../" or absolute paths).
func sanitizeExtractPath(dst string, name string) (string, error) {
	cleanName := filepath.Clean(name)

	if filepath.IsAbs(cleanName) {
		return "", fmt.Errorf("illegal absolute path in archive: %s", name)
	}

	if cleanName == ".." || strings.HasPrefix(cleanName, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("illegal path traversal in archive: %s", name)
	}

	target := filepath.Join(dst, cleanName)

	// Final defense: resolved target must be within dst
	cleanDst := filepath.Clean(dst) + string(filepath.Separator)
	if !strings.HasPrefix(filepath.Clean(target)+string(filepath.Separator), cleanDst) {
		return "", fmt.Errorf("path escapes destination: %s", name)
	}

	return target, nil
}
