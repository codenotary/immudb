/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package singleapp

import (
	"fmt"
	"os"

	"github.com/codenotary/immudb/embedded/appendable"
)

const DefaultFileMode = os.FileMode(0644)
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel
const DefaultReadBufferSize = 4096
const DefaultWriteBufferSize = 4096

type Options struct {
	readOnly       bool
	readBufferSize int

	writeBuffer   []byte
	retryableSync bool // if retryableSync is enabled, buffer space is released only after a successful sync
	autoSync      bool // if autoSync is enabled, sync is called when the buffer is full

	fileMode os.FileMode

	compressionFormat int
	compressionLevel  int

	preallocSize      int
	createIfNotExists bool

	metadata []byte
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		retryableSync:     true,
		autoSync:          true,
		createIfNotExists: true,
		fileMode:          DefaultFileMode,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
		readBufferSize:    DefaultReadBufferSize,
		writeBuffer:       make([]byte, DefaultWriteBufferSize),
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.readBufferSize <= 0 {
		return fmt.Errorf("%w: invalid readBufferSize", ErrInvalidOptions)
	}

	if !opts.readOnly && len(opts.writeBuffer) == 0 {
		return fmt.Errorf("%w: invalid writeBuffer", ErrInvalidOptions)
	}

	if opts.preallocSize < 0 {
		return fmt.Errorf("%w: invalid preallocSize", ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.readOnly = readOnly
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

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.fileMode = fileMode
	return opts
}

func (opts *Options) WithCompressionFormat(compressionFormat int) *Options {
	opts.compressionFormat = compressionFormat
	return opts
}

func (opts *Options) WithPreallocSize(preallocSize int) *Options {
	opts.preallocSize = preallocSize
	return opts
}

func (opts *Options) WithCreateIfNotExists(createIfNotExists bool) *Options {
	opts.createIfNotExists = createIfNotExists
	return opts
}

func (opts *Options) GetCompressionFormat() int {
	return opts.compressionFormat
}

func (opts *Options) GetCompressionLevel() int {
	return opts.compressionLevel
}

func (opts *Options) GetReadBufferSize() int {
	return opts.readBufferSize
}

func (opts *Options) GetPreallocSize() int {
	return opts.preallocSize
}

func (opts *Options) GetWriteBuffer() []byte {
	return opts.writeBuffer
}

func (opts *Options) WithCompresionLevel(compressionLevel int) *Options {
	opts.compressionLevel = compressionLevel
	return opts
}

func (opts *Options) WithMetadata(metadata []byte) *Options {
	opts.metadata = metadata
	return opts
}

func (opts *Options) WithReadBufferSize(size int) *Options {
	opts.readBufferSize = size
	return opts
}

func (opts *Options) WithWriteBuffer(b []byte) *Options {
	opts.writeBuffer = b
	return opts
}
