/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package multiapp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	for _, d := range []struct {
		n    string
		opts *Options
	}{
		{"nil", nil},
		{"empty", &Options{}},
		{"FileSize", DefaultOptions().WithFileSize(0)},
		{"FileSize", DefaultOptions().WithFileSize(0)},
		{"MaxOpenedFiles", DefaultOptions().WithMaxOpenedFiles(0)},
		{"FileExt", DefaultOptions().WithFileExt("")},
		{"ReadBufferSize", DefaultOptions().WithReadBufferSize(0)},
		{"WriteBufferSize", DefaultOptions().WithReadOnly(false).WithWriteBufferSize(0)},
	} {
		t.Run(d.n, func(t *testing.T) {
			require.ErrorIs(t, d.opts.Validate(), ErrInvalidOptions)
		})
	}
}

func TestDefaultOptions(t *testing.T) {
	require.NoError(t, DefaultOptions().Validate())
}

func TestValidOptions(t *testing.T) {
	opts := &Options{}

	require.Equal(t, "aof", opts.WithFileExt("aof").fileExt)
	require.Equal(t, "aof", opts.WithFileExt("aof").GetFileExt())
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).GetFileMode())
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).fileSize)
	require.Equal(t, DefaultMaxOpenedFiles, opts.WithMaxOpenedFiles(DefaultMaxOpenedFiles).maxOpenedFiles)
	require.Equal(t, []byte{}, opts.WithMetadata([]byte{}).metadata)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).compressionFormat)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).compressionLevel)

	require.True(t, opts.WithRetryableSync(true).retryableSync)
	require.True(t, opts.WithAutoSync(true).autoSync)

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, DefaultReadBufferSize+1, opts.WithReadBufferSize(DefaultReadBufferSize+1).GetReadBufferSize())
	require.NoError(t, opts.Validate())

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, DefaultWriteBufferSize+2, opts.WithWriteBufferSize(DefaultWriteBufferSize+2).GetWriteBufferSize())
	require.NoError(t, opts.Validate())

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.NoError(t, opts.Validate())
}
