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
package tbtree

import (
	"os"
	"time"
)

const DefaultMaxNodeSize = 4096
const DefaultFlushThld = 100_000
const DefaultMaxActiveSnapshots = 100
const DefaultRenewSnapRootAfter = time.Duration(1000) * time.Millisecond
const DefaultCacheSize = 100_000
const DefaultFileMode = os.FileMode(0755)
const DefaultFileSize = 1 << 26 // 64Mb
const DefaultMaxKeyLen = 1024
const DefaultCompactionThld = 3

const MinNodeSize = 128
const MinCacheSize = 1

type Options struct {
	flushThld          int
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	cacheSize          int
	readOnly           bool
	synced             bool
	fileMode           os.FileMode

	maxKeyLen int

	compactionThld        int
	delayDuringCompaction time.Duration

	// options below are only set during initialization and stored as metadata
	maxNodeSize int
	fileSize    int
}

func DefaultOptions() *Options {
	return &Options{
		flushThld:             DefaultFlushThld,
		maxActiveSnapshots:    DefaultMaxActiveSnapshots,
		renewSnapRootAfter:    DefaultRenewSnapRootAfter,
		cacheSize:             DefaultCacheSize,
		readOnly:              false,
		synced:                false,
		fileMode:              DefaultFileMode,
		maxKeyLen:             DefaultMaxKeyLen,
		compactionThld:        DefaultCompactionThld,
		delayDuringCompaction: 0,

		// options below are only set during initialization and stored as metadata
		maxNodeSize: DefaultMaxNodeSize,
		fileSize:    DefaultFileSize,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.maxNodeSize >= MinNodeSize &&
		opts.flushThld > 0 &&
		opts.maxActiveSnapshots > 0 &&
		opts.renewSnapRootAfter >= 0 &&
		opts.cacheSize >= MinCacheSize &&
		opts.maxKeyLen > 0 &&
		opts.compactionThld >= 0
}

func (opts *Options) WithFlushThld(flushThld int) *Options {
	opts.flushThld = flushThld
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

func (opts *Options) WithSynced(synced bool) *Options {
	opts.synced = synced
	return opts
}

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.fileMode = fileMode
	return opts
}

func (opts *Options) WithMaxKeyLen(maxKeyLen int) *Options {
	opts.maxKeyLen = maxKeyLen
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
