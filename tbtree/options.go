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
const DefaultFileMode = 0755
const DefaultFileSize = 1 << 26 // 64Mb

const DefaultKeyHistorySpace = 32 // ts trace len per key, number of key updates traced within a same key and leaf node

type Options struct {
	flushThld          int
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	cacheSize          int
	readOnly           bool
	synced             bool
	fileMode           os.FileMode

	// optsions below are only set during initialization and stored as metadata
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

		// optsions below are only set during initialization and stored as metadata
		maxNodeSize:     DefaultMaxNodeSize,
		keyHistorySpace: DefaultKeyHistorySpace,
		fileSize:        DefaultFileSize,
	}
}

func (opts *Options) SetFlushThld(flushThld int) *Options {
	opts.flushThld = flushThld
	return opts
}

func (opts *Options) SetMaxActiveSnapshots(maxActiveSnapshots int) *Options {
	opts.maxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *Options) SetRenewSnapRootAfter(renewSnapRootAfter time.Duration) *Options {
	opts.renewSnapRootAfter = renewSnapRootAfter
	return opts
}

func (opts *Options) SetCacheSize(cacheSize int) *Options {
	opts.cacheSize = cacheSize
	return opts
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

func (opts *Options) SetMaxNodeSize(maxNodeSize int) *Options {
	opts.maxNodeSize = maxNodeSize
	return opts
}

func (opts *Options) SetKeyHistorySpace(keyHistorySpace int) *Options {
	opts.keyHistorySpace = keyHistorySpace
	return opts
}

func (opts *Options) SetFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
	return opts
}
