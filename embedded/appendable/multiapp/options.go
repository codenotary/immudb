/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package multiapp

import (
	"fmt"
	"os"

	"github.com/codenotary/immudb/embedded/appendable"
)

const DefaultFileSize = 1 << 26 // 64Mb
const DefaultMaxOpenedFiles = 10
const DefaultFileMode = os.FileMode(0755)
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel
const DefaultReadBufferSize = 4096
const DefaultWriteBufferSize = 4096

type Options struct {
	readOnly       bool
	readBufferSize int

	writeBufferSize int
	retryableSync   bool // if retryableSync is enabled, buffer space is released only after a successful sync
	autoSync        bool // if autoSync is enabled, sync is called when the buffer is full

	fileMode          os.FileMode
	fileSize          int
	prealloc          bool
	fileExt           string
	metadata          []byte
	maxOpenedFiles    int
	compressionFormat int
	compressionLevel  int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		retryableSync:     true,
		autoSync:          true,
		fileMode:          DefaultFileMode,
		fileSize:          DefaultFileSize,
		fileExt:           "aof",
		maxOpenedFiles:    DefaultMaxOpenedFiles,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
		readBufferSize:    DefaultReadBufferSize,
		writeBufferSize:   DefaultWriteBufferSize,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.fileSize <= 0 {
		return fmt.Errorf("%w: invalid fileSize", ErrInvalidOptions)
	}

	if opts.maxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid maxOpenedFiles", ErrInvalidOptions)
	}

	if opts.fileExt == "" {
		return fmt.Errorf("%w: invalid fileExt", ErrInvalidOptions)
	}

	if opts.readBufferSize <= 0 {
		return fmt.Errorf("%w: invalid readBufferSize", ErrInvalidOptions)
	}

	if !opts.readOnly && opts.writeBufferSize <= 0 {
		return fmt.Errorf("%w: invalid writeBufferSize", ErrInvalidOptions)
	}

	return nil
}

func (opt *Options) WithReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) WithRetryableSync(retryableSync bool) *Options {
	opt.retryableSync = retryableSync
	return opt
}

func (opt *Options) WithAutoSync(autoSync bool) *Options {
	opt.autoSync = autoSync
	return opt
}

func (opt *Options) WithFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

func (opt *Options) WithMetadata(metadata []byte) *Options {
	opt.metadata = metadata
	return opt
}

func (opt *Options) WithFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

func (opt *Options) WithFileExt(fileExt string) *Options {
	opt.fileExt = fileExt
	return opt
}

func (opt *Options) WithMaxOpenedFiles(maxOpenedFiles int) *Options {
	opt.maxOpenedFiles = maxOpenedFiles
	return opt
}

func (opt *Options) WithCompressionFormat(compressionFormat int) *Options {
	opt.compressionFormat = compressionFormat
	return opt
}

func (opt *Options) WithCompresionLevel(compressionLevel int) *Options {
	opt.compressionLevel = compressionLevel
	return opt
}

func (opts *Options) WithReadBufferSize(size int) *Options {
	opts.readBufferSize = size
	return opts
}

func (opts *Options) WithWriteBufferSize(size int) *Options {
	opts.writeBufferSize = size
	return opts
}

func (opts *Options) WithPrealloc(prealloc bool) *Options {
	opts.prealloc = prealloc
	return opts
}

func (opt *Options) GetFileExt() string {
	return opt.fileExt
}

func (opt *Options) GetFileMode() os.FileMode {
	return opt.fileMode
}

func (opts *Options) GetReadBufferSize() int {
	return opts.readBufferSize
}

func (opts *Options) GetWriteBufferSize() int {
	return opts.writeBufferSize
}

func (opts *Options) GetPrealloc() bool {
	return opts.prealloc
}
