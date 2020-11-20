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
package tbtree

import (
	"os"
	"time"
)

const DefaultMaxNodeSize = 4096
const DefaultFlushThld = 100_000
const DefaultMaxActiveSnapshots = 100
const DefaultRenewSnapRootAfter = time.Duration(1000) * time.Millisecond
const DefaultCacheSize = 10000
const DefaultFileMode = os.FileMode(0755)
const DefaultFileSize = 1 << 26 // 64Mb

const DefaultKeyHistorySpace = 32 // ts trace len per key, number of key updates traced within a same key and leaf node

const MinNodeSize = 96
const MinCacheSize = 1

type Options struct {
	flushThld          int
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	cacheSize          int
	readOnly           bool
	synced             bool
	fileMode           os.FileMode

	// options below are only set during initialization and stored as metadata
	maxNodeSize     int
	keyHistorySpace int
	fileSize        int
}

func DefaultOptions() *Options {
	return &Options{
		flushThld:          DefaultFlushThld,
		maxActiveSnapshots: DefaultMaxActiveSnapshots,
		renewSnapRootAfter: DefaultRenewSnapRootAfter,
		cacheSize:          DefaultCacheSize,
		readOnly:           false,
		synced:             false,
		fileMode:           DefaultFileMode,

		// options below are only set during initialization and stored as metadata
		maxNodeSize:     DefaultMaxNodeSize,
		keyHistorySpace: DefaultKeyHistorySpace,
		fileSize:        DefaultFileSize,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.maxNodeSize >= MinNodeSize &&
		opts.keyHistorySpace >= 0 &&
		opts.flushThld > 0 &&
		opts.maxActiveSnapshots > 0 &&
		opts.renewSnapRootAfter > 0 &&
		opts.cacheSize >= MinCacheSize
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

func (opts *Options) WithMaxNodeSize(maxNodeSize int) *Options {
	opts.maxNodeSize = maxNodeSize
	return opts
}

func (opts *Options) WithKeyHistorySpace(keyHistorySpace int) *Options {
	opts.keyHistorySpace = keyHistorySpace
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
	return opts
}
