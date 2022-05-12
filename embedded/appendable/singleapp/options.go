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
package singleapp

import (
	"os"

	"github.com/codenotary/immudb/embedded/appendable"
)

const DefaultFileMode = os.FileMode(0644)
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel
const DefaultReadBufferSize = 4096
const DefaultWriteBufferSize = 4096

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	compressionFormat int
	compressionLevel  int

	readBufferSize  int
	writeBufferSize int

	metadata []byte
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
		fileMode:          DefaultFileMode,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
		readBufferSize:    DefaultReadBufferSize,
		writeBufferSize:   DefaultWriteBufferSize,
	}
}

func (opts *Options) Valid() bool {
	return opts != nil &&
		opts.readBufferSize > 0 &&
		opts.writeBufferSize > 0
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

func (opts *Options) WithCompressionFormat(compressionFormat int) *Options {
	opts.compressionFormat = compressionFormat
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

func (opts *Options) GetWriteBufferSize() int {
	return opts.writeBufferSize
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

func (opts *Options) WithWriteBufferSize(size int) *Options {
	opts.writeBufferSize = size
	return opts
}
