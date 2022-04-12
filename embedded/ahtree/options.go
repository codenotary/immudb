/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
)

const DefaultFileSize = multiapp.DefaultFileSize
const DefaultFileMode = os.FileMode(0755)
const DefaultDataCacheSlots = 1_000
const DefaultDigestsCacheSlots = 100_000
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	appFactory AppFactoryFunc

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
		fileSize:          DefaultFileSize,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.fileSize > 0 &&
		opts.dataCacheSlots > 0 &&
		opts.digestsCacheSlots > 0
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

func (opts *Options) WithDataCacheSlots(cacheSlots int) *Options {
	opts.dataCacheSlots = cacheSlots
	return opts
}

func (opts *Options) WithDigestsCacheSlots(cacheSlots int) *Options {
	opts.digestsCacheSlots = cacheSlots
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.fileSize = fileSize
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

func (opts *Options) WithAppFactory(appFactory AppFactoryFunc) *Options {
	opts.appFactory = appFactory
	return opts
}
