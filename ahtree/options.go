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
package ahtree

import (
	"os"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
)

const DefaultFileMode = 0755
const DefaultDataCacheSlots = 1_000
const DefaultDigestsCacheSlots = 100_000

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	dataCacheSlots    int
	digestsCacheSlots int

	// Options below are only set during initialization and stored as metadata
	fileSize          int
	compressionFormat int
	compressionLevel  int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            false,
		fileMode:          DefaultFileMode,
		dataCacheSlots:    DefaultDataCacheSlots,
		digestsCacheSlots: DefaultDigestsCacheSlots,

		// Options below are only set during initialization and stored as metadata
		fileSize:          multiapp.DefaultFileSize,
		compressionFormat: appendable.DefaultCompressionFormat,
		compressionLevel:  appendable.DefaultCompressionLevel,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.fileSize > 0 &&
		opts.dataCacheSlots > 0 &&
		opts.digestsCacheSlots > 0
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

func (opts *Options) SetDataCacheSlots(cacheSlots int) *Options {
	opts.dataCacheSlots = cacheSlots
	return opts
}

func (opts *Options) SetDigestsCacheSlots(cacheSlots int) *Options {
	opts.digestsCacheSlots = cacheSlots
	return opts
}

func (opts *Options) SetFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
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
