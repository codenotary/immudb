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

package tbtree

import (
	"testing"
	"time"

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
		{"logger", DefaultOptions().WithLogger(nil)},
		{"FileSize", DefaultOptions().WithFileSize(0)},
		{"FlushThld", DefaultOptions().WithFlushThld(0)},
		{"WithSyncThld", DefaultOptions().WithSyncThld(0)},
		{"FlushThld>WithSyncThld", DefaultOptions().WithFlushThld(10).WithSyncThld(1)},
		{"FlushBufferSize", DefaultOptions().WithFlushBufferSize(0)},
		{"CleanupPercentage<0", DefaultOptions().WithCleanupPercentage(-1)},
		{"CleanupPercentage>100", DefaultOptions().WithCleanupPercentage(101)},
		{"MaxActiveSnapshots", DefaultOptions().WithMaxActiveSnapshots(0)},
		{"RenewSnapRootAfter", DefaultOptions().WithRenewSnapRootAfter(-1)},
		{"CacheSize", DefaultOptions().WithCacheSize(0)},
		{"CompactionThld", DefaultOptions().WithCompactionThld(-1)},
		{"MaxKeySize", DefaultOptions().WithMaxKeySize(0)},
		{"MaxValueSize", DefaultOptions().WithMaxValueSize(0)},
		{"MaxNodeSize", DefaultOptions().WithMaxNodeSize(requiredNodeSize(DefaultMaxKeySize, DefaultMaxValueSize) - 1)},
		{"NodesLogMaxOpenedFiles", DefaultOptions().WithNodesLogMaxOpenedFiles(0)},
		{"HistoryLogMaxOpenedFiles", DefaultOptions().WithHistoryLogMaxOpenedFiles(0)},
		{"CommitLogMaxOpenedFiles", DefaultOptions().WithCommitLogMaxOpenedFiles(0)},
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

	require.Equal(t, 256, opts.WithMaxKeySize(256).maxKeySize)
	require.Equal(t, 256, opts.WithMaxValueSize(256).maxValueSize)

	require.Equal(t, 1, opts.WithCompactionThld(1).compactionThld)
	require.Equal(t, time.Duration(1)*time.Millisecond, opts.WithDelayDuringCompaction(time.Duration(1)*time.Millisecond).delayDuringCompaction)
	require.False(t, opts.WithReadOnly(false).readOnly)
	require.NotNil(t, opts.WithLogger(DefaultOptions().logger))

	require.Equal(t, 2, opts.WithNodesLogMaxOpenedFiles(2).nodesLogMaxOpenedFiles)
	require.Equal(t, 3, opts.WithHistoryLogMaxOpenedFiles(3).historyLogMaxOpenedFiles)
	require.Equal(t, 1, opts.WithCommitLogMaxOpenedFiles(1).commitLogMaxOpenedFiles)

	require.NoError(t, opts.Validate())

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.NoError(t, opts.Validate())
	require.Nil(t, opts.WithAppFactory(nil).appFactory)
	require.NoError(t, opts.Validate())

	appFactoryCalled := false
	appFactory := func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
		appFactoryCalled = true
		return nil, nil
	}

	require.NotNil(t, opts.WithAppFactory(appFactory).appFactory)
	require.NoError(t, opts.Validate())

	opts.appFactory("", "", nil)
	require.True(t, appFactoryCalled)

}
