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
	"fmt"
	"os"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/logger"
)

const DefaultMaxNodeSize = 4096
const DefaultFlushThld = 100_000
const DefaultSyncThld = 1_000_000
const DefaultFlushBufferSize = 4096
const DefaultCleanUpPercentage float32 = 0
const DefaultMaxActiveSnapshots = 100
const DefaultRenewSnapRootAfter = time.Duration(1000) * time.Millisecond
const DefaultCacheSize = 100_000
const DefaultFileMode = os.FileMode(0755)
const DefaultFileSize = 1 << 26 // 64Mb
const DefaultMaxKeySize = 1024
const DefaultMaxValueSize = 512
const DefaultCompactionThld = 2
const DefaultDelayDuringCompaction = time.Duration(10) * time.Millisecond

const DefaultNodesLogMaxOpenedFiles = 10
const DefaultHistoryLogMaxOpenedFiles = 1
const DefaultCommitLogMaxOpenedFiles = 1

const MinCacheSize = 1

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type AppRemoveFunc func(rootPath, subPath string) error

type Options struct {
	logger logger.Logger

	flushThld          int
	syncThld           int
	flushBufferSize    int
	cleanupPercentage  float32
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	cacheSize          int
	readOnly           bool
	fileMode           os.FileMode

	nodesLogMaxOpenedFiles   int
	historyLogMaxOpenedFiles int
	commitLogMaxOpenedFiles  int

	compactionThld        int
	delayDuringCompaction time.Duration

	// options below are only set during initialization and stored as metadata
	maxNodeSize  int
	maxKeySize   int
	maxValueSize int
	fileSize     int

	appFactory AppFactoryFunc
	appRemove  AppRemoveFunc
}

func DefaultOptions() *Options {
	return &Options{
		logger:                logger.NewSimpleLogger("immudb ", os.Stderr),
		flushThld:             DefaultFlushThld,
		syncThld:              DefaultSyncThld,
		flushBufferSize:       DefaultFlushBufferSize,
		cleanupPercentage:     DefaultCleanUpPercentage,
		maxActiveSnapshots:    DefaultMaxActiveSnapshots,
		renewSnapRootAfter:    DefaultRenewSnapRootAfter,
		cacheSize:             DefaultCacheSize,
		readOnly:              false,
		fileMode:              DefaultFileMode,
		compactionThld:        DefaultCompactionThld,
		delayDuringCompaction: DefaultDelayDuringCompaction,

		nodesLogMaxOpenedFiles:   DefaultNodesLogMaxOpenedFiles,
		historyLogMaxOpenedFiles: DefaultHistoryLogMaxOpenedFiles,
		commitLogMaxOpenedFiles:  DefaultCommitLogMaxOpenedFiles,

		// options below are only set during initialization and stored as metadata
		maxNodeSize:  DefaultMaxNodeSize,
		maxKeySize:   DefaultMaxKeySize,
		maxValueSize: DefaultMaxValueSize,
		fileSize:     DefaultFileSize,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.fileSize <= 0 {
		return fmt.Errorf("%w: invalid FileSize", ErrInvalidOptions)
	}

	if opts.maxKeySize <= 0 {
		return fmt.Errorf("%w: invalid MaxKeySize", ErrInvalidOptions)
	}

	if opts.maxValueSize <= 0 {
		return fmt.Errorf("%w: invalid MaxValueSize", ErrInvalidOptions)
	}

	if opts.maxNodeSize < requiredNodeSize(opts.maxKeySize, opts.maxValueSize) {
		return fmt.Errorf("%w: invalid MaxNodeSize", ErrInvalidOptions)
	}

	if opts.flushThld <= 0 {
		return fmt.Errorf("%w: invalid FlushThld", ErrInvalidOptions)
	}

	if opts.syncThld <= 0 {
		return fmt.Errorf("%w: invalid SyncThld", ErrInvalidOptions)
	}

	if opts.flushThld > opts.syncThld {
		return fmt.Errorf("%w: FlushThld must be lower or equal to SyncThld", ErrInvalidOptions)
	}

	if opts.flushBufferSize <= 0 {
		return fmt.Errorf("%w: invalid FlushBufferSize", ErrInvalidOptions)
	}

	if opts.cleanupPercentage < 0 || opts.cleanupPercentage > 100 {
		return fmt.Errorf("%w: invalid CleanupPercentage", ErrInvalidOptions)
	}

	if opts.nodesLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid NodesLogMaxOpenedFiles", ErrInvalidOptions)
	}

	if opts.historyLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid HistoryLogMaxOpenedFiles", ErrInvalidOptions)
	}

	if opts.commitLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid CommitLogMaxOpenedFiles", ErrInvalidOptions)
	}

	if opts.maxActiveSnapshots <= 0 {
		return fmt.Errorf("%w: invalid MaxActiveSnapshots", ErrInvalidOptions)
	}

	if opts.renewSnapRootAfter < 0 {
		return fmt.Errorf("%w: invalid RenewSnapRootAfter", ErrInvalidOptions)
	}

	if opts.cacheSize < MinCacheSize {
		return fmt.Errorf("%w: invalid CacheSize", ErrInvalidOptions)
	}

	if opts.compactionThld <= 0 {
		return fmt.Errorf("%w: invalid CompactionThld", ErrInvalidOptions)
	}

	if opts.logger == nil {
		return fmt.Errorf("%w: invalid Logger", ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithLogger(logger logger.Logger) *Options {
	opts.logger = logger
	return opts
}

func (opts *Options) WithAppFactory(appFactory AppFactoryFunc) *Options {
	opts.appFactory = appFactory
	return opts
}

func (opts *Options) WithAppRemoveFunc(AppRemove AppRemoveFunc) *Options {
	opts.appRemove = AppRemove
	return opts
}

func (opts *Options) WithFlushThld(flushThld int) *Options {
	opts.flushThld = flushThld
	return opts
}

func (opts *Options) WithSyncThld(syncThld int) *Options {
	opts.syncThld = syncThld
	return opts
}

func (opts *Options) WithFlushBufferSize(size int) *Options {
	opts.flushBufferSize = size
	return opts
}

func (opts *Options) WithCleanupPercentage(cleanupPercentage float32) *Options {
	opts.cleanupPercentage = cleanupPercentage
	return opts
}

func (opts *Options) WithMaxActiveSnapshots(maxActiveSnapshots int) *Options {
	opts.maxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *Options) WithRenewSnapRootAfter(renewSnapRootAfter time.Duration) *Options {
	opts.renewSnapRootAfter = renewSnapRootAfter
	return opts
}

func (opts *Options) WithCacheSize(cacheSize int) *Options {
	opts.cacheSize = cacheSize
	return opts
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
	return opts
}

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.fileMode = fileMode
	return opts
}

func (opts *Options) WithNodesLogMaxOpenedFiles(nodesLogMaxOpenedFiles int) *Options {
	opts.nodesLogMaxOpenedFiles = nodesLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithHistoryLogMaxOpenedFiles(historyLogMaxOpenedFiles int) *Options {
	opts.historyLogMaxOpenedFiles = historyLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opts.commitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithMaxKeySize(maxKeySize int) *Options {
	opts.maxKeySize = maxKeySize
	return opts
}

func (opts *Options) WithMaxValueSize(maxValueSize int) *Options {
	opts.maxValueSize = maxValueSize
	return opts
}

func (opts *Options) WithMaxNodeSize(maxNodeSize int) *Options {
	opts.maxNodeSize = maxNodeSize
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
	return opts
}

func (opts *Options) WithCompactionThld(compactionThld int) *Options {
	opts.compactionThld = compactionThld
	return opts
}

func (opts *Options) WithDelayDuringCompaction(delay time.Duration) *Options {
	opts.delayDuringCompaction = delay
	return opts
}
