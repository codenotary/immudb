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
		opts.maxIOConcurrency <= MaxParallelIO
}

func (opt *Options) SetReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) SetSynced(synced bool) *Options {
	opt.synced = synced
	return opt
}

func (opt *Options) SetFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

func (opt *Options) SetConcurrency(maxConcurrency int) *Options {
	opt.maxConcurrency = maxConcurrency
	return opt
}

func (opt *Options) SetIOConcurrency(maxIOConcurrency int) *Options {
	opt.maxIOConcurrency = maxIOConcurrency
	return opt
}

func (opt *Options) SetMaxTxEntries(maxTxEntries int) *Options {
	opt.maxTxEntries = maxTxEntries
	return opt
}

func (opt *Options) SetMaxKeyLen(maxKeyLen int) *Options {
	opt.maxKeyLen = maxKeyLen
	return opt
}

func (opt *Options) SetMaxValueLen(maxValueLen int) *Options {
	opt.maxValueLen = maxValueLen
	return opt
}

func (opt *Options) SetMaxLinearProofLen(maxLinearProofLen int) *Options {
	opt.maxLinearProofLen = maxLinearProofLen
	return opt
}

func (opt *Options) SetFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

func (opt *Options) SetVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opt.vLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opt.txLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opt.commitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetCompressionFormat(compressionFormat int) *Options {
	opt.compressionFormat = compressionFormat
	return opt
}

func (opt *Options) SetCompresionLevel(compressionLevel int) *Options {
	opt.compressionLevel = compressionLevel
	return opt
}

// IndexOptions

func (opt *IndexOptions) SetCacheSize(cacheSize int) *IndexOptions {
	opt.cacheSize = cacheSize
	return opt
}

func (opt *IndexOptions) SetFlushThld(flushThld int) *IndexOptions {
	opt.flushThld = flushThld
	return opt
}

func (opt *IndexOptions) SetMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opt.maxActiveSnapshots = maxActiveSnapshots
	return opt
}

func (opt *IndexOptions) SetMaxNodeSize(maxNodeSize int) *IndexOptions {
	opt.maxNodeSize = maxNodeSize
	return opt
}

func (opt *IndexOptions) SetRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opt.renewSnapRootAfter = renewSnapRootAfter
	return opt
}
