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
	"os"
	"time"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/tbtree"
)

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	maxConcurrency    int
	maxIOConcurrency  int
	maxLinearProofLen int

	vLogMaxOpenedFiles      int
	txLogMaxOpenedFiles     int
	commitLogMaxOpenedFiles int

	// options below are only set during initialization and stored as metadata
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	fileSize          int
	compressionFormat int
	compressionLevel  int

	// options below affect indexing
	indexOpts *IndexOptions
}

type IndexOptions struct {
	cacheSize          int
	flushThld          int
	maxActiveSnapshots int
	maxNodeSize        int
	renewSnapRootAfter time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		readOnly: false,
		synced:   true,
		fileMode: DefaultFileMode,

		maxConcurrency:    DefaultMaxConcurrency,
		maxIOConcurrency:  DefaultMaxIOConcurrency,
		maxLinearProofLen: DefaultMaxLinearProofLen,

		vLogMaxOpenedFiles:      10,
		txLogMaxOpenedFiles:     10,
		commitLogMaxOpenedFiles: 1,

		// options below are only set during initialization and stored as metadata
		maxTxEntries:      DefaultMaxTxEntries,
		maxKeyLen:         DefaultMaxKeyLen,
		maxValueLen:       DefaultMaxValueLen,
		fileSize:          multiapp.DefaultFileSize,
		compressionFormat: appendable.DefaultCompressionFormat,
		compressionLevel:  appendable.DefaultCompressionLevel,

		indexOpts: DefaultIndexOptions(),
	}
}

func DefaultIndexOptions() *IndexOptions {
	return &IndexOptions{
		cacheSize:          tbtree.DefaultCacheSize,
		flushThld:          tbtree.DefaultFlushThld,
		maxActiveSnapshots: tbtree.DefaultMaxActiveSnapshots,
		maxNodeSize:        tbtree.DefaultMaxNodeSize,
		renewSnapRootAfter: time.Duration(1000) * time.Millisecond,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.maxKeyLen <= MaxKeyLen &&
		opts.maxConcurrency > 0 &&
		opts.maxIOConcurrency > 0 &&
		opts.maxIOConcurrency <= MaxParallelIO &&
		opts.maxLinearProofLen >= 0
}

func (opts *Options) SetReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
	return opts
}

func (opts *Options) SetSynced(synced bool) *Options {
	opts.synced = synced
	return opts
}

func (opts *Options) SetFileMode(fileMode os.FileMode) *Options {
	opts.fileMode = fileMode
	return opts
}

func (opts *Options) SetConcurrency(maxConcurrency int) *Options {
	opts.maxConcurrency = maxConcurrency
	return opts
}

func (opts *Options) SetIOConcurrency(maxIOConcurrency int) *Options {
	opts.maxIOConcurrency = maxIOConcurrency
	return opts
}

func (opts *Options) SetMaxTxEntries(maxTxEntries int) *Options {
	opts.maxTxEntries = maxTxEntries
	return opts
}

func (opts *Options) SetMaxKeyLen(maxKeyLen int) *Options {
	opts.maxKeyLen = maxKeyLen
	return opts
}

func (opts *Options) SetMaxValueLen(maxValueLen int) *Options {
	opts.maxValueLen = maxValueLen
	return opts
}

func (opts *Options) SetMaxLinearProofLen(maxLinearProofLen int) *Options {
	opts.maxLinearProofLen = maxLinearProofLen
	return opts
}

func (opts *Options) SetFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
	return opts
}

func (opts *Options) SetVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opts.vLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opts
}

func (opts *Options) SetTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opts.txLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opts
}

func (opts *Options) SetCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opts.commitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *Options) SetCompressionFormat(compressionFormat int) *Options {
	opts.compressionFormat = compressionFormat
	return opts
}

func (opts *Options) SetCompresionLevel(compressionLevel int) *Options {
	opts.compressionLevel = compressionLevel
	return opts
}

// IndexOptions

func (opts *IndexOptions) SetCacheSize(cacheSize int) *IndexOptions {
	opts.cacheSize = cacheSize
	return opts
}

func (opts *IndexOptions) SetFlushThld(flushThld int) *IndexOptions {
	opts.flushThld = flushThld
	return opts
}

func (opts *IndexOptions) SetMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opts.maxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *IndexOptions) SetMaxNodeSize(maxNodeSize int) *IndexOptions {
	opts.maxNodeSize = maxNodeSize
	return opts
}

func (opts *IndexOptions) SetRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opts.renewSnapRootAfter = renewSnapRootAfter
	return opts
}
