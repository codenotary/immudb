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
package store

import (
	"testing"
	"time"

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

	require.Equal(t, 1, opts.WithCommitLogMaxOpenedFiles(1).CommitLogMaxOpenedFiles)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).CompressionLevel)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).CompressionFormat)
	require.Equal(t, DefaultMaxConcurrency, opts.WithMaxConcurrency(DefaultMaxConcurrency).MaxConcurrency)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).FileMode)
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).FileSize)
	require.Equal(t, DefaultMaxIOConcurrency, opts.WithMaxIOConcurrency(DefaultMaxIOConcurrency).MaxIOConcurrency)
	require.Equal(t, DefaultMaxKeyLen, opts.WithMaxKeyLen(DefaultMaxKeyLen).MaxKeyLen)
	require.Equal(t, DefaultMaxLinearProofLen, opts.WithMaxLinearProofLen(DefaultMaxLinearProofLen).MaxLinearProofLen)
	require.Equal(t, DefaultMaxTxEntries, opts.WithMaxTxEntries(DefaultMaxTxEntries).MaxTxEntries)
	require.Equal(t, DefaultMaxValueLen, opts.WithMaxValueLen(DefaultMaxValueLen).MaxValueLen)
	require.Equal(t, DefaultTxLogCacheSize, opts.WithTxLogCacheSize(DefaultOptions().TxLogCacheSize).TxLogCacheSize)
	require.Equal(t, 2, opts.WithTxLogMaxOpenedFiles(2).TxLogMaxOpenedFiles)
	require.Equal(t, 3, opts.WithVLogMaxOpenedFiles(3).VLogMaxOpenedFiles)

	require.True(t, opts.WithSynced(true).Synced)

	require.NotNil(t, opts.WithIndexOptions(DefaultIndexOptions()).IndexOpts)

	require.False(t, opts.WithReadOnly(false).ReadOnly)
	require.True(t, validOptions(opts))

	require.True(t, opts.WithReadOnly(true).ReadOnly)
	require.True(t, validOptions(opts))

	require.Nil(t, opts.WithIndexOptions(nil).IndexOpts)
	require.False(t, validOptions(opts))

	indexOpts := &IndexOptions{}
	opts.WithIndexOptions(indexOpts)
	require.False(t, validOptions(opts))

	require.Equal(t, 100, indexOpts.WithCacheSize(100).CacheSize)
	require.Equal(t, 1000, indexOpts.WithFlushThld(1000).FlushThld)
	require.Equal(t, 10, indexOpts.WithMaxActiveSnapshots(10).MaxActiveSnapshots)
	require.Equal(t, 4096, indexOpts.WithMaxNodeSize(4096).MaxNodeSize)
	require.Equal(t, time.Duration(1000)*time.Millisecond,
		indexOpts.WithRenewSnapRootAfter(time.Duration(1000)*time.Millisecond).RenewSnapRootAfter)
	require.True(t, validOptions(opts))
}
