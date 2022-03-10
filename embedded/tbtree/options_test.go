/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"time"

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

	require.Equal(t, DefaultCacheSize, opts.WithCacheSize(DefaultCacheSize).cacheSize)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).fileSize)
	require.Equal(t, DefaultFlushThld, opts.WithFlushThld(DefaultFlushThld).flushThld)
	require.Equal(t, DefaultSyncThld, opts.WithSyncThld(DefaultSyncThld).syncThld)
	require.Equal(t, DefaultFlushBufferSize, opts.WithFlushBufferSize(DefaultFlushBufferSize).flushBufferSize)
	require.Equal(t, DefaultCleanUpPercentage+1, opts.WithCleanupPercentage(DefaultCleanUpPercentage+1).cleanupPercentage)

	require.Equal(t, DefaultMaxActiveSnapshots, opts.WithMaxActiveSnapshots(DefaultMaxActiveSnapshots).maxActiveSnapshots)
	require.Equal(t, DefaultMaxNodeSize, opts.WithMaxNodeSize(DefaultMaxNodeSize).maxNodeSize)
	require.Equal(t, DefaultRenewSnapRootAfter, opts.WithRenewSnapRootAfter(DefaultRenewSnapRootAfter).renewSnapRootAfter)
	require.Equal(t, 256, opts.WithMaxKeyLen(256).maxKeyLen)
	require.Equal(t, 1, opts.WithCompactionThld(1).compactionThld)
	require.Equal(t, time.Duration(1)*time.Millisecond, opts.WithDelayDuringCompaction(time.Duration(1)*time.Millisecond).delayDuringCompaction)
	require.False(t, opts.WithReadOnly(false).readOnly)
	require.NotNil(t, opts.WithLog(DefaultOptions().log))

	require.Equal(t, 2, opts.WithNodesLogMaxOpenedFiles(2).nodesLogMaxOpenedFiles)
	require.Equal(t, 3, opts.WithHistoryLogMaxOpenedFiles(3).historyLogMaxOpenedFiles)
	require.Equal(t, 1, opts.WithCommitLogMaxOpenedFiles(1).commitLogMaxOpenedFiles)

	require.True(t, validOptions(opts))

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.True(t, validOptions(opts))
	require.Nil(t, opts.WithAppFactory(nil).appFactory)
	require.True(t, validOptions(opts))

	appFactoryCalled := false
	appFactory := func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
		appFactoryCalled = true
		return nil, nil
	}

	require.NotNil(t, opts.WithAppFactory(appFactory).appFactory)
	require.True(t, validOptions(opts))

	opts.appFactory("", "", nil)
	require.True(t, appFactoryCalled)

}
