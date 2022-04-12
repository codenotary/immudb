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
package ahtree

import (
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	require.False(t, validOptions(nil))
	require.False(t, validOptions(&Options{}))
}

func TestDefaultOptions(t *testing.T) {
	require.True(t, validOptions(DefaultOptions()))
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
	require.True(t, opts.WithSynced(true).synced)
	require.NotNil(t, opts.WithAppFactory(dummyAppFactory).appFactory)

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.True(t, validOptions(opts))

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.True(t, validOptions(opts))
}
