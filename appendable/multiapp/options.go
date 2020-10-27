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
package multiapp

import (
	"os"

	"codenotary.io/immudb-v2/appendable"
)

const DefaultFileSize = 1 << 26 // 64Mb
const DefaultMaxOpenedFiles = 10
const DefaultFileMode = 0755

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
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
		fileMode:          DefaultFileMode,
		fileSize:          DefaultFileSize,
		fileExt:           "aof",
		maxOpenedFiles:    DefaultMaxOpenedFiles,
		compressionFormat: appendable.DefaultCompressionFormat,
		compressionLevel:  appendable.DefaultCompressionLevel,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.fileSize > 0 &&
		opts.maxOpenedFiles > 0 &&
		opts.fileExt != ""
}

func (opt *Options) SetReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) SetSynced(synced bool) *Options {
	opt.synced = synced
	return opt
}

func (opt *Options) SetFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

func (opt *Options) SetMetadata(metadata []byte) *Options {
	opt.metadata = metadata
	return opt
}

func (opt *Options) SetFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

func (opt *Options) SetFileExt(fileExt string) *Options {
	opt.fileExt = fileExt
	return opt
}

func (opt *Options) SetMaxOpenedFiles(maxOpenedFiles int) *Options {
	opt.maxOpenedFiles = maxOpenedFiles
	return opt
}

func (opt *Options) SetCompressionFormat(compressionFormat int) *Options {
	opt.compressionFormat = compressionFormat
	return opt
}

func (opt *Options) SetCompresionLevel(compressionLevel int) *Options {
	opt.compressionLevel = compressionLevel
	return opt
}
