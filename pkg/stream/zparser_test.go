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

package stream

import (
	"bytes"
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestParseZEntry(t *testing.T) {
	z, err := ParseZEntry([]byte(`set`), []byte(`key`), 87.4, 1, bytes.NewBuffer([]byte(`reader`)), 4096)
	require.NoError(t, err)
	require.IsType(t, &schema.ZEntry{}, z)
}

func TestParseZEntryErr(t *testing.T) {
	z, err := ParseZEntry([]byte(`set`), []byte(`key`), 87.4, 1, bytes.NewBuffer([]byte{}), 4096)
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, z)
}
