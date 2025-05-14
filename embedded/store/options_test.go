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

package store

import (
	"testing"
	"time"

	"github.com/codenotary/immudb/v2/embedded/appendable"
	"github.com/codenotary/immudb/v2/embedded/appendable/multiapp"
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
		{"MaxConcurrency", DefaultOptions().WithMaxConcurrency(0)},
		{"WriteBufferSize", DefaultOptions().WithWriteBufferSize(0)},
		{"SyncFrequency", DefaultOptions().WithSyncFrequency(-1)},
		{"MaxActiveTransactions", DefaultOptions().WithMaxActiveTransactions(0)},
		{"MVCCReadSetLimit", DefaultOptions().WithMVCCReadSetLimit(0)},
		{"MaxIOConcurrency", DefaultOptions().WithMaxIOConcurrency(0)},
		{"MaxIOConcurrency-max", DefaultOptions().WithMaxIOConcurrency(MaxParallelIO + 1)},
		{"TxLogCacheSize", DefaultOptions().WithTxLogCacheSize(-1)},
		{"VLogCacheSize", DefaultOptions().WithVLogCacheSize(-1)},
		{"VLogMaxOpenedFiles", DefaultOptions().WithVLogMaxOpenedFiles(0)},
		{"TxLogMaxOpenedFiles", DefaultOptions().WithTxLogMaxOpenedFiles(0)},
		{"CommitLogMaxOpenedFiles", DefaultOptions().WithCommitLogMaxOpenedFiles(0)},
		{"WriteTxHeaderVersion", DefaultOptions().WithWriteTxHeaderVersion(-1)},
		{"WriteTxHeaderVersion-max", DefaultOptions().WithWriteTxHeaderVersion(MaxTxHeaderVersion + 1)},
		{"MaxWaitees", DefaultOptions().WithMaxWaitees(-1)},
		{"TimeFunc", DefaultOptions().WithTimeFunc(nil)},
		{"MaxTxEntries", DefaultOptions().WithMaxTxEntries(0)},
		{"MaxKeyLen", DefaultOptions().WithMaxKeyLen(0)},
		{"MaxKeyLen-max", DefaultOptions().WithMaxKeyLen(MaxKeyLen + 1)},
		{"MaxValueLen", DefaultOptions().WithMaxValueLen(0)},
		{"FileSize", DefaultOptions().WithFileSize(0)},
		{"FileSize-max", DefaultOptions().WithFileSize(MaxFileSize)},
	} {
		t.Run(d.n, func(t *testing.T) {
			require.ErrorIs(t, d.opts.Validate(), ErrInvalidOptions)
		})
	}
}

func TestInvalidIndexOptions(t *testing.T) {
	for _, d := range []struct {
		n    string
		opts *IndexOptions
	}{
		{"nil", nil},
		{"empty", &IndexOptions{}},
		{"SyncThld", DefaultIndexOptions().WithSyncThld(0)},
		{"FlushBufferSize", DefaultIndexOptions().WithFlushBufferSize(0)},
		{"CleanupPercentage", DefaultIndexOptions().WithCleanupPercentage(-1)},
		{"CleanupPercentage", DefaultIndexOptions().WithCleanupPercentage(101)},
		{"MaxActiveSnapshots", DefaultIndexOptions().WithMaxActiveSnapshots(0)},
		{"RenewSnapRootAfter", DefaultIndexOptions().WithRenewSnapRootAfter(-1)},
		{"CompactionThld", DefaultIndexOptions().WithCompactionThld(0)},
		{"DelayDuringCompaction", DefaultIndexOptions().WithDelayDuringCompaction(-1)},
		{"NodesLogMaxOpenedFiles", DefaultIndexOptions().WithNodesLogMaxOpenedFiles(0)},
		{"HistoryLogMaxOpenedFiles", DefaultIndexOptions().WithHistoryLogMaxOpenedFiles(0)},
	} {
		t.Run(d.n, func(t *testing.T) {
			require.ErrorIs(t, d.opts.Validate(), ErrInvalidOptions)
		})
	}
}

func TestInvalidAHTOptions(t *testing.T) {
	for _, d := range []struct {
		n    string
		opts *AHTOptions
	}{
		{"nil", nil},
		{"empty", &AHTOptions{}},
		{"WriteBufferSize", DefaultAHTOptions().WithWriteBufferSize(0)},
		{"SyncThld", DefaultAHTOptions().WithSyncThld(0)},
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

	require.Equal(t, 1, opts.WithCommitLogMaxOpenedFiles(1).CommitLogMaxOpenedFiles)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).CompressionLevel)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).CompressionFormat)
	require.Equal(t, DefaultMaxConcurrency, opts.WithMaxConcurrency(DefaultMaxConcurrency).MaxConcurrency)
	require.Equal(t, 1<<20, opts.WithWriteBufferSize(1<<20).WriteBufferSize)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).FileMode)
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).FileSize)
	require.Equal(t, DefaultSyncFrequency, opts.WithSyncFrequency(DefaultSyncFrequency).SyncFrequency)
	require.Equal(t, DefaultMaxActiveTransactions, opts.WithMaxActiveTransactions(DefaultMaxActiveTransactions).MaxActiveTransactions)
	require.Equal(t, DefaultMVCCReadSetLimit, opts.WithMVCCReadSetLimit(DefaultMVCCReadSetLimit).MVCCReadSetLimit)
	require.Equal(t, DefaultMaxIOConcurrency, opts.WithMaxIOConcurrency(DefaultMaxIOConcurrency).MaxIOConcurrency)
	require.Equal(t, DefaultMaxKeyLen, opts.WithMaxKeyLen(DefaultMaxKeyLen).MaxKeyLen)
	require.Equal(t, DefaultMaxTxEntries, opts.WithMaxTxEntries(DefaultMaxTxEntries).MaxTxEntries)
	require.Equal(t, DefaultMaxValueLen, opts.WithMaxValueLen(DefaultMaxValueLen).MaxValueLen)
	require.Equal(t, DefaultTxLogCacheSize, opts.WithTxLogCacheSize(DefaultOptions().TxLogCacheSize).TxLogCacheSize)
	require.Equal(t, DefaultVLogCacheSize, opts.WithVLogCacheSize(DefaultOptions().VLogCacheSize).VLogCacheSize)
	require.Equal(t, 2, opts.WithTxLogMaxOpenedFiles(2).TxLogMaxOpenedFiles)
	require.Equal(t, 3, opts.WithVLogMaxOpenedFiles(3).VLogMaxOpenedFiles)
	require.Equal(t, DefaultMaxWaitees, opts.WithMaxWaitees(DefaultMaxWaitees).MaxWaitees)
	require.Equal(t, DefaultEmbeddedValues, opts.WithEmbeddedValues(DefaultEmbeddedValues).EmbeddedValues)

	timeFun := func() time.Time {
		return time.Now()
	}
	require.NotNil(t, opts.WithTimeFunc(timeFun).TimeFunc)

	require.True(t, opts.WithSynced(true).Synced)

	require.NotNil(t, opts.WithIndexOptions(DefaultIndexOptions()).IndexOpts)

	require.NotNil(t, opts.WithAHTOptions(DefaultAHTOptions()).AHTOpts)

	require.False(t, opts.WithReadOnly(false).ReadOnly)

	require.NotNil(t, opts.WithLogger(DefaultOptions().logger))

	require.NoError(t, opts.Validate())

	require.True(t, opts.WithReadOnly(true).ReadOnly)
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

	require.Nil(t, opts.WithIndexOptions(nil).IndexOpts)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	indexOpts := &IndexOptions{}
	opts.WithIndexOptions(indexOpts)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, 10_000, indexOpts.WithSyncThld(10_000).SyncThld)
	require.Equal(t, 10, indexOpts.WithMaxActiveSnapshots(10).MaxActiveSnapshots)
	require.Equal(t, time.Duration(1000)*time.Millisecond,
		indexOpts.WithRenewSnapRootAfter(time.Duration(1000)*time.Millisecond).RenewSnapRootAfter)
	require.Equal(t, 10, indexOpts.WithNodesLogMaxOpenedFiles(10).NodesLogMaxOpenedFiles)
	require.Equal(t, 11, indexOpts.WithHistoryLogMaxOpenedFiles(11).HistoryLogMaxOpenedFiles)
	require.Equal(t, 0.7, indexOpts.WithCompactionThld(0.7).CompactionThld)
	require.Equal(t, 1*time.Millisecond, indexOpts.WithDelayDuringCompaction(1*time.Millisecond).DelayDuringCompaction)
	require.Equal(t, 4096*2, indexOpts.WithFlushBufferSize(4096*2).FlushBufferSize)
	require.Equal(t, float32(10), indexOpts.WithCleanupPercentage(10).CleanupPercentage)
	require.Equal(t, 10*1024*1024, indexOpts.WithSharedWriteBufferSize(10*1024*1024).SharedWriteBufferSize)
	require.Equal(t, 1024*1024, indexOpts.WithSharedWriteBufferChunkSize(1024*1024).SharedWriteBufferChunkSize)
	require.Equal(t, 1024*1024, indexOpts.WithMinWriteBufferSize(1024*1024).MinWriteBufferSize)
	require.Equal(t, 8*1024*1024, indexOpts.WithMaxWriteBufferSize(8*1024*1024).MaxWriteBufferSize)

	require.Equal(t, 8*time.Millisecond, indexOpts.WithBackPressureMinDelay(8*time.Millisecond).BackpressureMinDelay)
	require.Equal(t, 10*time.Millisecond, indexOpts.WithBackPressureMaxDelay(10*time.Millisecond).BackpressureMaxDelay)

	require.NoError(t, opts.Validate())

	require.Nil(t, opts.WithAHTOptions(nil).AHTOpts)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	ahtOpts := &AHTOptions{}
	opts.WithAHTOptions(ahtOpts)
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	require.Equal(t, 1<<20, ahtOpts.WithWriteBufferSize(1<<20).WriteBufferSize)
	require.Equal(t, 10_000, ahtOpts.WithSyncThld(10_000).SyncThld)

	require.NoError(t, opts.Validate())
}
