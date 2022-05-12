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
package multiapp

import (
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
	readOnly          bool
	synced            bool
	fileMode          os.FileMode
	fileSize          int
	fileExt           string
	metadata          []byte
	maxOpenedFiles    int
	compressionFormat int
	compressionLevel  int
	readBufferSize    int
	writeBufferSize   int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
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

func (opts *Options) Valid() bool {
	return opts != nil &&
		opts.fileSize > 0 &&
		opts.maxOpenedFiles > 0 &&
		opts.fileExt != "" &&
		opts.readBufferSize > 0 &&
		opts.writeBufferSize > 0
}

func (opt *Options) WithReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) WithSynced(synced bool) *Options {
	opt.synced = synced
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
