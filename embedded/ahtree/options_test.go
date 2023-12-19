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

package ahtree

import (
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
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
		{"DataCacheSlots", DefaultOptions().WithDataCacheSlots(0)},
		{"DigestsCacheSlots", DefaultOptions().WithDigestsCacheSlots(0)},
		{"ReadBufferSize", DefaultOptions().WithReadBufferSize(0)},
		{"SyncThld", DefaultOptions().WithReadOnly(false).WithSyncThld(0)},
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

	dummyAppFactory := func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
		return nil, nil
	}

	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).fileSize)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).compressionFormat)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).compressionLevel)
	require.Equal(t, DefaultDataCacheSlots, opts.WithDataCacheSlots(DefaultDataCacheSlots).dataCacheSlots)
	require.Equal(t, DefaultDigestsCacheSlots, opts.WithDigestsCacheSlots(DefaultDigestsCacheSlots).digestsCacheSlots)
	require.NotNil(t, opts.WithAppFactory(dummyAppFactory).appFactory)

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.Equal(t, multiapp.DefaultReadBufferSize, opts.WithReadBufferSize(multiapp.DefaultReadBufferSize).readBufferSize)
	require.Equal(t, 0, opts.WithWriteBufferSize(0).writeBufferSize)
	require.Equal(t, 0, opts.WithSyncThld(0).syncThld)
	require.NoError(t, opts.Validate())

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.Equal(t, multiapp.DefaultWriteBufferSize, opts.WithWriteBufferSize(multiapp.DefaultWriteBufferSize).writeBufferSize)
	require.True(t, opts.WithRetryableSync(true).retryableSync)
	require.True(t, opts.WithAutoSync(true).autoSync)
	require.Equal(t, DefaultSyncThld, opts.WithSyncThld(DefaultSyncThld).syncThld)
	require.NoError(t, opts.Validate())

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.NoError(t, opts.Validate())
}
