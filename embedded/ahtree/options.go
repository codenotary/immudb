/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package ahtree

import (
	"fmt"
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
const DefaultSyncThld = 100_000
const DefaultWriteBufferSize = 1 << 24 //16Mb

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type Options struct {
	readOnly       bool
	readBufferSize int

	writeBufferSize int
	retryableSync   bool // if retryableSync is enabled, buffer space is released only after a successful sync
	autoSync        bool // if autoSync is enabled, sync is called when the buffer is full
	syncThld        int  // sync after appending the specified amount of values

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
		readOnly:       false,
		readBufferSize: multiapp.DefaultReadBufferSize,

		writeBufferSize: multiapp.DefaultWriteBufferSize,
		retryableSync:   true,
		autoSync:        true,
		syncThld:        DefaultSyncThld,

		fileMode:          DefaultFileMode,
		dataCacheSlots:    DefaultDataCacheSlots,
		digestsCacheSlots: DefaultDigestsCacheSlots,

		// Options below are only set during initialization and stored as metadata
		fileSize:          DefaultFileSize,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.fileSize <= 0 {
		return fmt.Errorf("%w: invalid fileSize", ErrInvalidOptions)
	}

	if opts.dataCacheSlots <= 0 {
		return fmt.Errorf("%w: invalid dataCacheSlots", ErrInvalidOptions)
	}

	if opts.digestsCacheSlots <= 0 {
		return fmt.Errorf("%w: invalid digestsCacheSlots", ErrInvalidOptions)
	}

	if opts.readBufferSize <= 0 {
		return fmt.Errorf("%w: invalid readBufferSize", ErrInvalidOptions)
	}

	if !opts.readOnly && opts.writeBufferSize <= 0 {
		return fmt.Errorf("%w: invalid writeBufferSize", ErrInvalidOptions)
	}

	if !opts.readOnly && opts.syncThld <= 0 {
		return fmt.Errorf("%w: invalid syncThld", ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
	return opts
}

func (opts *Options) WithReadBufferSize(size int) *Options {
	opts.readBufferSize = size
	return opts
}

func (opts *Options) WithWriteBufferSize(size int) *Options {
	opts.writeBufferSize = size
	return opts
}

func (opts *Options) WithRetryableSync(retryableSync bool) *Options {
	opts.retryableSync = retryableSync
	return opts
}

func (opts *Options) WithAutoSync(autoSync bool) *Options {
	opts.autoSync = autoSync
	return opts
}

func (opts *Options) WithSyncThld(syncThld int) *Options {
	opts.syncThld = syncThld
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
