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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandardIoutil(t *testing.T) {
	sio := NewStandardIoutil()
	filename := filepath.Join(t.TempDir(), "test-standard-ioutil")
	content := strings.ReplaceAll(filename, "-", " ")
	require.NoError(t, sio.WriteFile(filename, []byte(content), 0644))
	readBytes, err := sio.ReadFile(filename)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}
