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
const DefaultMaxKeyLen = 1024
const DefaultMaxValueLen = 4096 // 4Kb
const DefaultFileMode = os.FileMode(0755)
const DefaultMaxLinearProofLen = 1 << 10
const DefaultFileSize = multiapp.DefaultFileSize
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel

const MaxFileSize = 1 << 50 // 1 Pb

type Options struct {
	ReadOnly bool
	Synced   bool
	FileMode os.FileMode

	MaxConcurrency    int
	MaxIOConcurrency  int
	MaxLinearProofLen int

	VLogMaxOpenedFiles      int
	TxLogMaxOpenedFiles     int
	CommitLogMaxOpenedFiles int

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
	CacheSize          int
	FlushThld          int
	MaxActiveSnapshots int
	MaxNodeSize        int
	RenewSnapRootAfter time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		ReadOnly: false,
		Synced:   true,
		FileMode: DefaultFileMode,

		MaxConcurrency:    DefaultMaxConcurrency,
		MaxIOConcurrency:  DefaultMaxIOConcurrency,
		MaxLinearProofLen: DefaultMaxLinearProofLen,

		VLogMaxOpenedFiles:      10,
		TxLogMaxOpenedFiles:     10,
		CommitLogMaxOpenedFiles: 1,

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
		CacheSize:          tbtree.DefaultCacheSize,
		FlushThld:          tbtree.DefaultFlushThld,
		MaxActiveSnapshots: tbtree.DefaultMaxActiveSnapshots,
		MaxNodeSize:        tbtree.DefaultMaxNodeSize,
		RenewSnapRootAfter: time.Duration(1000) * time.Millisecond,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.MaxConcurrency > 0 &&
		opts.MaxIOConcurrency > 0 &&
		opts.MaxIOConcurrency <= MaxParallelIO &&
		opts.MaxLinearProofLen >= 0 &&

		opts.VLogMaxOpenedFiles > 0 &&
		opts.TxLogMaxOpenedFiles > 0 &&
		opts.CommitLogMaxOpenedFiles > 0 &&

		// options below are only set during initialization and stored as metadata
		opts.MaxTxEntries > 0 &&
		opts.MaxKeyLen > 0 &&
		opts.MaxKeyLen <= MaxKeyLen &&
		opts.MaxValueLen > 0 &&
		opts.FileSize > 0 &&
		opts.FileSize < MaxFileSize &&
		validIndexOptions(opts.IndexOpts)
}

func validIndexOptions(opts *IndexOptions) bool {
	return opts != nil &&
		opts.CacheSize > 0 &&
		opts.FlushThld > 0 &&
		opts.MaxActiveSnapshots > 0 &&
		opts.MaxNodeSize > 0 &&
		opts.RenewSnapRootAfter >= 0
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
