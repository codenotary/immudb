/*
Copyright 2019-2020 vChain, Inc.

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
package tbtree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	require.False(t, validOptions(nil))
	require.False(t, validOptions(&Options{}))
}

func TestValidOptions(t *testing.T) {
	opts := &Options{}

	require.Equal(t, DefaultCacheSize, opts.WithCacheSize(DefaultCacheSize).cacheSize)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).fileSize)
	require.Equal(t, DefaultFlushThld, opts.WithFlushThld(DefaultFlushThld).flushThld)
	require.Equal(t, DefaultKeyHistorySpace, opts.WithKeyHistorySpace(DefaultKeyHistorySpace).keyHistorySpace)
	require.Equal(t, DefaultMaxActiveSnapshots, opts.WithMaxActiveSnapshots(DefaultMaxActiveSnapshots).maxActiveSnapshots)
	require.Equal(t, DefaultMaxNodeSize, opts.WithMaxNodeSize(DefaultMaxNodeSize).maxNodeSize)
	require.Equal(t, DefaultRenewSnapRootAfter, opts.WithRenewSnapRootAfter(DefaultRenewSnapRootAfter).renewSnapRootAfter)
	require.True(t, opts.WithSynced(true).synced)

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.True(t, validOptions(opts))

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.True(t, validOptions(opts))
}
