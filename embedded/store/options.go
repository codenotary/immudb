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

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/tbtree"
)

const DefaultMaxConcurrency = 30
const DefaultMaxIOConcurrency = 1
const DefaultMaxTxEntries = 1 << 10 // 1024
const DefaultMaxKeyLen = 256
const DefaultMaxValueLen = 4096 // 4Kb
const DefaultFileMode = os.FileMode(0755)
const DefaultMaxLinearProofLen = 1 << 10
const DefaultFileSize = multiapp.DefaultFileSize
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel

const MaxFileSize = 1 << 50 // 1 Pb

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
		fileSize:          DefaultFileSize,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,

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
		opts.maxConcurrency > 0 &&
		opts.maxIOConcurrency > 0 &&
		opts.maxIOConcurrency <= MaxParallelIO &&
		opts.maxLinearProofLen >= 0 &&

		opts.vLogMaxOpenedFiles > 0 &&
		opts.txLogMaxOpenedFiles > 0 &&
		opts.commitLogMaxOpenedFiles > 0 &&

		// options below are only set during initialization and stored as metadata
		opts.maxTxEntries > 0 &&
		opts.maxKeyLen > 0 &&
		opts.maxKeyLen <= MaxKeyLen &&
		opts.maxValueLen > 0 &&
		opts.fileSize > 0 &&
		opts.fileSize < MaxFileSize &&
		validIndexOptions(opts.indexOpts)
}

func validIndexOptions(opts *IndexOptions) bool {
	return opts != nil &&
		opts.cacheSize > 0 &&
		opts.flushThld > 0 &&
		opts.maxActiveSnapshots > 0 &&
		opts.maxNodeSize > 0 &&
		opts.renewSnapRootAfter > 0
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
	return opts
}

func (opts *Options) WithSynced(synced bool) *Options {
	opts.synced = synced
	return opts
}

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.fileMode = fileMode
	return opts
}

func (opts *Options) WithMaxConcurrency(maxConcurrency int) *Options {
	opts.maxConcurrency = maxConcurrency
	return opts
}

func (opts *Options) WithMaxIOConcurrency(maxIOConcurrency int) *Options {
	opts.maxIOConcurrency = maxIOConcurrency
	return opts
}

func (opts *Options) WithMaxTxEntries(maxTxEntries int) *Options {
	opts.maxTxEntries = maxTxEntries
	return opts
}

func (opts *Options) WithMaxKeyLen(maxKeyLen int) *Options {
	opts.maxKeyLen = maxKeyLen
	return opts
}

func (opts *Options) WithMaxValueLen(maxValueLen int) *Options {
	opts.maxValueLen = maxValueLen
	return opts
}

func (opts *Options) WithMaxLinearProofLen(maxLinearProofLen int) *Options {
	opts.maxLinearProofLen = maxLinearProofLen
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
	return opts
}

func (opts *Options) WithVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opts.vLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opts.txLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opts.commitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithCompressionFormat(compressionFormat int) *Options {
	opts.compressionFormat = compressionFormat
	return opts
}

func (opts *Options) WithCompresionLevel(compressionLevel int) *Options {
	opts.compressionLevel = compressionLevel
	return opts
}

func (opts *Options) WithIndexOptions(indexOptions *IndexOptions) *Options {
	opts.indexOpts = indexOptions
	return opts
}

// IndexOptions

func (opts *IndexOptions) WithCacheSize(cacheSize int) *IndexOptions {
	opts.cacheSize = cacheSize
	return opts
}

func (opts *IndexOptions) WithFlushThld(flushThld int) *IndexOptions {
	opts.flushThld = flushThld
	return opts
}

func (opts *IndexOptions) WithMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opts.maxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *IndexOptions) WithMaxNodeSize(maxNodeSize int) *IndexOptions {
	opts.maxNodeSize = maxNodeSize
	return opts
}

func (opts *IndexOptions) WithRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opts.renewSnapRootAfter = renewSnapRootAfter
	return opts
}
