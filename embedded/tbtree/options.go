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

package tbtree

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/v2/embedded/appendable"
	"github.com/codenotary/immudb/v2/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/v2/embedded/logger"
)

const (
	DefaultSyncThld                  = 1024 * 1024
	DefaultFlushBufferSize           = 4096
	DefaultCompactionThld            = 0.5
	DefaultCleanUpPercentage float32 = 0

	DefaultFileSize = 1 << 26 // 64Mb
	DefaultFileMode = os.FileMode(0755)

	DefaultMaxActiveSnapshots    = 100
	DefaultSnapshotRenewalPeriod = time.Duration(1000) * time.Millisecond

	DefaultAppendableWriteBufferSize = 4096

	DefaultNodesLogMaxOpenedFiles   = 10
	DefaultHistoryLogMaxOpenedFiles = 1
	DefaultCommitLogMaxOpenedFiles  = 1
)

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type Options struct {
	logger logger.Logger

	id    TreeID
	wb    *WriteBuffer
	pgBuf *PageCache

	syncThld       int
	compactionThld float32

	cleanupPercentage  float32
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	readOnly           bool
	fileMode           os.FileMode

	appWriteBufferSize int

	treeLogMaxOpenedFiles    int
	historyLogMaxOpenedFiles int
	commitLogMaxOpenedFiles  int

	snapshotRenewalPeriod time.Duration

	// options below are only set during initialization and stored as metadata
	fileSize int

	appFactory AppFactoryFunc
	appRemove  AppRemoveFunc
	readDir    ReadDirFunc
}

func DefaultOptions() *Options {
	return &Options{
		logger:                   logger.NewMemoryLogger(),
		maxActiveSnapshots:       DefaultMaxActiveSnapshots,
		renewSnapRootAfter:       DefaultSnapshotRenewalPeriod,
		fileMode:                 DefaultFileMode,
		readOnly:                 false,
		syncThld:                 DefaultSyncThld,
		compactionThld:           DefaultCompactionThld,
		appWriteBufferSize:       DefaultAppendableWriteBufferSize,
		treeLogMaxOpenedFiles:    DefaultNodesLogMaxOpenedFiles,
		historyLogMaxOpenedFiles: DefaultHistoryLogMaxOpenedFiles,
		commitLogMaxOpenedFiles:  DefaultCommitLogMaxOpenedFiles,
		snapshotRenewalPeriod:    DefaultSnapshotRenewalPeriod,
		fileSize:                 DefaultFileSize,
		appFactory:               defaultAppFactory,
		appRemove:                defaultAppRemove,
		readDir:                  os.ReadDir,
	}
}

func defaultAppFactory(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
	fullPath := filepath.Join(rootPath, subPath)
	return multiapp.Open(fullPath, opts)
}

func defaultAppRemove(rootPath, subPath string) error {
	fullPath := filepath.Join(rootPath, subPath)
	return os.RemoveAll(fullPath)
}

func (opts *Options) Validate() error {
	if opts.logger == nil {
		return fmt.Errorf("%w: invalid Logger", ErrInvalidOptions)
	}

	if opts.wb == nil {
		return fmt.Errorf("%w: missing write buffer", ErrInvalidOptions)
	}

	if opts.pgBuf == nil {
		return fmt.Errorf("%w: missing page buffer", ErrInvalidOptions)
	}

	if opts.cleanupPercentage < 0 || opts.cleanupPercentage > 100 {
		return fmt.Errorf("%w: invalid CleanupPercentage", ErrInvalidOptions)
	}

	if opts.appWriteBufferSize <= 0 {
		return fmt.Errorf("%w: invalid appendable write buffer size", ErrInvalidOptions)
	}

	if opts.treeLogMaxOpenedFiles <= 0 {
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

	if opts.appFactory == nil {
		return fmt.Errorf("%w: missing appendable factory", ErrInvalidOptions)
	}
	return nil
}

func (opts *Options) WithTreeID(id TreeID) *Options {
	opts.id = id
	return opts
}

func (opts *Options) WithLogger(logger logger.Logger) *Options {
	opts.logger = logger
	return opts
}

func (opts *Options) WithWriteBuffer(wb *WriteBuffer) *Options {
	opts.wb = wb
	return opts
}

func (opts *Options) WithPageBuffer(pgBuf *PageCache) *Options {
	opts.pgBuf = pgBuf
	return opts
}

func (opts *Options) WithAppRemove(appRemove AppRemoveFunc) *Options {
	opts.appRemove = appRemove
	return opts
}

func (opts *Options) WithAppFactory(appFactory AppFactoryFunc) *Options {
	opts.appFactory = appFactory
	return opts
}

func (opts *Options) WithReadDirFunc(readDir ReadDirFunc) *Options {
	opts.readDir = readDir
	return opts
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
	return opts
}

func (opts *Options) WithFileSize(size int) *Options {
	opts.fileSize = size
	return opts
}

func (opts *Options) WithFileMode(mode os.FileMode) *Options {
	opts.fileMode = mode
	return opts
}

func (opts *Options) WithTreeLogMaxOpenedFiles(n int) *Options {
	opts.treeLogMaxOpenedFiles = n
	return opts
}

func (opts *Options) WithHistoryLogMaxOpenedFiles(n int) *Options {
	opts.historyLogMaxOpenedFiles = n
	return opts
}

func (opts *Options) WithAppendableWriteBufferSize(size int) *Options {
	opts.appWriteBufferSize = size
	return opts
}

func (opts *Options) WithSyncThld(thld int) *Options {
	opts.syncThld = thld
	return opts
}

func (opts *Options) WithCompactionThld(thld float32) *Options {
	opts.compactionThld = thld
	return opts
}

func (opts *Options) WithMaxActiveSnapshots(n int) *Options {
	opts.maxActiveSnapshots = n
	return opts
}
