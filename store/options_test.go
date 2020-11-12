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

	require.Equal(t, 1, opts.WithCommitLogMaxOpenedFiles(1).commitLogMaxOpenedFiles)
	require.Equal(t, DefaultCompressionLevel, opts.WithCompresionLevel(DefaultCompressionLevel).compressionLevel)
	require.Equal(t, DefaultCompressionFormat, opts.WithCompressionFormat(DefaultCompressionFormat).compressionFormat)
	require.Equal(t, DefaultMaxConcurrency, opts.WithMaxConcurrency(DefaultMaxConcurrency).maxConcurrency)
	require.Equal(t, DefaultFileMode, opts.WithFileMode(DefaultFileMode).fileMode)
	require.Equal(t, DefaultFileSize, opts.WithFileSize(DefaultFileSize).fileSize)
	require.Equal(t, DefaultMaxIOConcurrency, opts.WithMaxIOConcurrency(DefaultMaxIOConcurrency).maxIOConcurrency)
	require.Equal(t, DefaultMaxKeyLen, opts.WithMaxKeyLen(DefaultMaxKeyLen).maxKeyLen)
	require.Equal(t, DefaultMaxLinearProofLen, opts.WithMaxLinearProofLen(DefaultMaxLinearProofLen).maxLinearProofLen)
	require.Equal(t, DefaultMaxTxEntries, opts.WithMaxTxEntries(DefaultMaxTxEntries).maxTxEntries)
	require.Equal(t, DefaultMaxValueLen, opts.WithMaxValueLen(DefaultMaxValueLen).maxValueLen)
	require.Equal(t, 2, opts.WithTxLogMaxOpenedFiles(2).txLogMaxOpenedFiles)
	require.Equal(t, 3, opts.WithVLogMaxOpenedFiles(3).vLogMaxOpenedFiles)

	require.True(t, opts.WithSynced(true).synced)

	require.NotNil(t, opts.WithIndexOptions(DefaultIndexOptions()).indexOpts)

	require.False(t, opts.WithReadOnly(false).readOnly)
	require.True(t, validOptions(opts))

	require.True(t, opts.WithReadOnly(true).readOnly)
	require.True(t, validOptions(opts))

	require.Nil(t, opts.WithIndexOptions(nil).indexOpts)
	require.False(t, validOptions(opts))

	indexOpts := &IndexOptions{}
	opts.WithIndexOptions(indexOpts)
	require.False(t, validOptions(opts))

	require.Equal(t, 100, indexOpts.WithCacheSize(100).cacheSize)
	require.Equal(t, 1000, indexOpts.WithFlushThld(1000).flushThld)
	require.Equal(t, 10, indexOpts.WithMaxActiveSnapshots(10).maxActiveSnapshots)
	require.Equal(t, 4096, indexOpts.WithMaxNodeSize(4096).maxNodeSize)
	require.Equal(t, time.Duration(1000)*time.Millisecond,
		indexOpts.WithRenewSnapRootAfter(time.Duration(1000)*time.Millisecond).renewSnapRootAfter)
	require.True(t, validOptions(opts))
}
