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
package store

import (
	"fmt"
	"os"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/logger"
)

const DefaultMaxConcurrency = 30
const DefaultMaxIOConcurrency = 1
const DefaultMaxTxEntries = 1 << 10 // 1024
const DefaultMaxKeyLen = 1024
const DefaultMaxValueLen = 4096 // 4Kb
const DefaultFileMode = os.FileMode(0755)
const DefaultMaxLinearProofLen = 1 << 10
const DefaultFileSize = multiapp.DefaultFileSize
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel
const DefaultTxLogCacheSize = 1000
const DefaultMaxWaitees = 1000
const DefaultVLogMaxOpenedFiles = 10
const DefaultTxLogMaxOpenedFiles = 10
const DefaultCommitLogMaxOpenedFiles = 10
const DefaultWriteTxHeaderVersion = MaxTxHeaderVersion

const MaxFileSize = (1 << 31) - 1 // 2Gb

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type TimeFunc func() time.Time

type Options struct {
	ReadOnly bool
	Synced   bool
	FileMode os.FileMode
	logger   logger.Logger

	appFactory         AppFactoryFunc
	CompactionDisabled bool

	MaxConcurrency    int
	MaxIOConcurrency  int
	MaxLinearProofLen int

	TxLogCacheSize int

	VLogMaxOpenedFiles      int
	TxLogMaxOpenedFiles     int
	CommitLogMaxOpenedFiles int
	WriteTxHeaderVersion    int

	MaxWaitees int

	TimeFunc TimeFunc

	// options below are only set during initialization and stored as metadata
	MaxTxEntries      int
	MaxKeyLen         int
	MaxValueLen       int
	FileSize          int
	CompressionFormat int
	CompressionLevel  int

	// options below affect indexing
	IndexOpts *IndexOptions
}

type IndexOptions struct {
	CacheSize                int
	FlushThld                int
	SyncThld                 int
	FlushBufferSize          int
	CleanupPercentage        float32
	MaxActiveSnapshots       int
	MaxNodeSize              int
	RenewSnapRootAfter       time.Duration
	CompactionThld           int
	DelayDuringCompaction    time.Duration
	NodesLogMaxOpenedFiles   int
	HistoryLogMaxOpenedFiles int
	CommitLogMaxOpenedFiles  int
}

func DefaultOptions() *Options {
	return &Options{
		ReadOnly: false,
		Synced:   true,
		FileMode: DefaultFileMode,
		logger:   logger.NewSimpleLogger("immudb ", os.Stderr),

		MaxConcurrency:    DefaultMaxConcurrency,
		MaxIOConcurrency:  DefaultMaxIOConcurrency,
		MaxLinearProofLen: DefaultMaxLinearProofLen,

		TxLogCacheSize: DefaultTxLogCacheSize,

		VLogMaxOpenedFiles:      DefaultVLogMaxOpenedFiles,
		TxLogMaxOpenedFiles:     DefaultTxLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles: DefaultCommitLogMaxOpenedFiles,

		MaxWaitees: DefaultMaxWaitees,

		TimeFunc: func() time.Time {
			return time.Now()
		},

		WriteTxHeaderVersion: DefaultWriteTxHeaderVersion,

		// options below are only set during initialization and stored as metadata
		MaxTxEntries:      DefaultMaxTxEntries,
		MaxKeyLen:         DefaultMaxKeyLen,
		MaxValueLen:       DefaultMaxValueLen,
		FileSize:          DefaultFileSize,
		CompressionFormat: DefaultCompressionFormat,
		CompressionLevel:  DefaultCompressionLevel,

		IndexOpts: DefaultIndexOptions(),
	}
}

func DefaultIndexOptions() *IndexOptions {
	return &IndexOptions{
		CacheSize:                tbtree.DefaultCacheSize,
		FlushThld:                tbtree.DefaultFlushThld,
		SyncThld:                 tbtree.DefaultSyncThld,
		FlushBufferSize:          tbtree.DefaultFlushBufferSize,
		CleanupPercentage:        tbtree.DefaultCleanUpPercentage,
		MaxActiveSnapshots:       tbtree.DefaultMaxActiveSnapshots,
		MaxNodeSize:              tbtree.DefaultMaxNodeSize,
		RenewSnapRootAfter:       tbtree.DefaultRenewSnapRootAfter,
		CompactionThld:           tbtree.DefaultCompactionThld,
		DelayDuringCompaction:    0,
		NodesLogMaxOpenedFiles:   tbtree.DefaultNodesLogMaxOpenedFiles,
		HistoryLogMaxOpenedFiles: tbtree.DefaultHistoryLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles:  tbtree.DefaultCommitLogMaxOpenedFiles,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: invalid MaxConcurrency", ErrInvalidOptions)
	}

	if opts.MaxIOConcurrency <= 0 || opts.MaxIOConcurrency > MaxParallelIO {
		return fmt.Errorf("%w: invalid MaxIOConcurrency", ErrInvalidOptions)
	}
	if opts.MaxLinearProofLen < 0 {
		return fmt.Errorf("%w: invalid MaxLinearProofLen", ErrInvalidOptions)
	}

	if opts.VLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid VLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.TxLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid TxLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.CommitLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid CommitLogMaxOpenedFiles", ErrInvalidOptions)
	}

	if opts.TxLogCacheSize < 0 {
		return fmt.Errorf("%w: invalid TxLogCacheSize", ErrInvalidOptions)
	}

	if opts.MaxWaitees < 0 {
		return fmt.Errorf("%w: invalid MaxWaitees", ErrInvalidOptions)
	}

	if opts.TimeFunc == nil {
		return fmt.Errorf("%w: invalid TimeFunc", ErrInvalidOptions)
	}

	if opts.WriteTxHeaderVersion < 0 {
		return fmt.Errorf("%w: invalid WriteTxHeaderVersion", ErrInvalidOptions)
	}
	if opts.WriteTxHeaderVersion > MaxTxHeaderVersion {
		return fmt.Errorf("%w: invalid WriteTxHeaderVersion", ErrInvalidOptions)
	}

	// options below are only set during initialization and stored as metadata
	if opts.MaxTxEntries <= 0 {
		return fmt.Errorf("%w: invalid MaxTxEntries", ErrInvalidOptions)
	}
	if opts.MaxKeyLen <= 0 || opts.MaxKeyLen > MaxKeyLen {
		return fmt.Errorf("%w: invalid MaxKeyLen", ErrInvalidOptions)
	}
	if opts.MaxValueLen <= 0 {
		return fmt.Errorf("%w: invalid MaxValueLen", ErrInvalidOptions)
	}
	if opts.FileSize <= 0 || opts.FileSize >= MaxFileSize {
		return fmt.Errorf("%w: invalid FileSize", ErrInvalidOptions)
	}
	if opts.logger == nil {
		return fmt.Errorf("%w: invalid log", ErrInvalidOptions)
	}

	return opts.IndexOpts.Validate()
}

func (opts *IndexOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil index options ", ErrInvalidOptions)
	}
	if opts.CacheSize <= 0 {
		return fmt.Errorf("%w: invalid index option CacheSize", ErrInvalidOptions)
	}
	if opts.FlushThld <= 0 {
		return fmt.Errorf("%w: invalid index option FlushThld", ErrInvalidOptions)
	}
	if opts.SyncThld <= 0 {
		return fmt.Errorf("%w: invalid index option SyncThld", ErrInvalidOptions)
	}
	if opts.FlushBufferSize <= 0 {
		return fmt.Errorf("%w: invalid index option FlushBufferSize", ErrInvalidOptions)
	}
	if opts.CleanupPercentage < 0 || opts.CleanupPercentage > 100 {
		return fmt.Errorf("%w: invalid index option CleanupPercentage", ErrInvalidOptions)
	}
	if opts.MaxActiveSnapshots <= 0 {
		return fmt.Errorf("%w: invalid index option MaxActiveSnapshots", ErrInvalidOptions)
	}
	if opts.MaxNodeSize <= 0 {
		return fmt.Errorf("%w: invalid index option MaxNodeSize", ErrInvalidOptions)
	}
	if opts.CompactionThld <= 0 {
		return fmt.Errorf("%w: invalid index option CompactionThld", ErrInvalidOptions)
	}
	if opts.DelayDuringCompaction < 0 {
		return fmt.Errorf("%w: invalid index option DelayDuringCompaction", ErrInvalidOptions)
	}
	if opts.RenewSnapRootAfter < 0 {
		return fmt.Errorf("%w: invalid index option RenewSnapRootAfter", ErrInvalidOptions)
	}
	if opts.NodesLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option NodesLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.HistoryLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option HistoryLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.CommitLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option CommitLogMaxOpenedFiles", ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.ReadOnly = readOnly
	return opts
}

func (opts *Options) WithSynced(synced bool) *Options {
	opts.Synced = synced
	return opts
}

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.FileMode = fileMode
	return opts
}

func (opts *Options) WithLogger(logger logger.Logger) *Options {
	opts.logger = logger
	return opts
}

func (opts *Options) WithAppFactory(appFactory AppFactoryFunc) *Options {
	opts.appFactory = appFactory
	return opts
}

func (opts *Options) WithCompactionDisabled(disabled bool) *Options {
	opts.CompactionDisabled = disabled
	return opts
}

func (opts *Options) WithMaxConcurrency(maxConcurrency int) *Options {
	opts.MaxConcurrency = maxConcurrency
	return opts
}

func (opts *Options) WithMaxIOConcurrency(maxIOConcurrency int) *Options {
	opts.MaxIOConcurrency = maxIOConcurrency
	return opts
}

func (opts *Options) WithMaxTxEntries(maxTxEntries int) *Options {
	opts.MaxTxEntries = maxTxEntries
	return opts
}

func (opts *Options) WithMaxKeyLen(maxKeyLen int) *Options {
	opts.MaxKeyLen = maxKeyLen
	return opts
}

func (opts *Options) WithMaxValueLen(maxValueLen int) *Options {
	opts.MaxValueLen = maxValueLen
	return opts
}

func (opts *Options) WithMaxLinearProofLen(maxLinearProofLen int) *Options {
	opts.MaxLinearProofLen = maxLinearProofLen
	return opts
}

func (opts *Options) WithTxLogCacheSize(txLogCacheSize int) *Options {
	opts.TxLogCacheSize = txLogCacheSize
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.FileSize = fileSize
	return opts
}

func (opts *Options) WithVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opts.VLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opts.TxLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opts.CommitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithMaxWaitees(maxWaitees int) *Options {
	opts.MaxWaitees = maxWaitees
	return opts
}

func (opts *Options) WithTimeFunc(timeFunc TimeFunc) *Options {
	opts.TimeFunc = timeFunc
	return opts
}

func (opts *Options) WithWriteTxHeaderVersion(version int) *Options {
	opts.WriteTxHeaderVersion = version
	return opts
}

func (opts *Options) WithCompressionFormat(compressionFormat int) *Options {
	opts.CompressionFormat = compressionFormat
	return opts
}

func (opts *Options) WithCompresionLevel(compressionLevel int) *Options {
	opts.CompressionLevel = compressionLevel
	return opts
}

func (opts *Options) WithIndexOptions(indexOptions *IndexOptions) *Options {
	opts.IndexOpts = indexOptions
	return opts
}

// IndexOptions

func (opts *IndexOptions) WithCacheSize(cacheSize int) *IndexOptions {
	opts.CacheSize = cacheSize
	return opts
}

func (opts *IndexOptions) WithFlushThld(flushThld int) *IndexOptions {
	opts.FlushThld = flushThld
	return opts
}

func (opts *IndexOptions) WithSyncThld(syncThld int) *IndexOptions {
	opts.SyncThld = syncThld
	return opts
}

func (opts *IndexOptions) WithFlushBufferSize(flushBufferSize int) *IndexOptions {
	opts.FlushBufferSize = flushBufferSize
	return opts
}

func (opts *IndexOptions) WithCleanupPercentage(cleanupPercentage float32) *IndexOptions {
	opts.CleanupPercentage = cleanupPercentage
	return opts
}

func (opts *IndexOptions) WithMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opts.MaxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *IndexOptions) WithMaxNodeSize(maxNodeSize int) *IndexOptions {
	opts.MaxNodeSize = maxNodeSize
	return opts
}

func (opts *IndexOptions) WithRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opts.RenewSnapRootAfter = renewSnapRootAfter
	return opts
}

func (opts *IndexOptions) WithCompactionThld(compactionThld int) *IndexOptions {
	opts.CompactionThld = compactionThld
	return opts
}

func (opts *IndexOptions) WithDelayDuringCompaction(delayDuringCompaction time.Duration) *IndexOptions {
	opts.DelayDuringCompaction = delayDuringCompaction
	return opts
}

func (opts *IndexOptions) WithNodesLogMaxOpenedFiles(nodesLogMaxOpenedFiles int) *IndexOptions {
	opts.NodesLogMaxOpenedFiles = nodesLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithHistoryLogMaxOpenedFiles(historyLogMaxOpenedFiles int) *IndexOptions {
	opts.HistoryLogMaxOpenedFiles = historyLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *IndexOptions {
	opts.CommitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}
