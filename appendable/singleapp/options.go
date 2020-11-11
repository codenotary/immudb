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
package singleapp

import (
	"os"

	"codenotary.io/immudb-v2/appendable"
)

const DefaultFileMode = os.FileMode(0644)
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	compressionFormat int
	compressionLevel  int

	metadata []byte
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
		fileMode:          DefaultFileMode,
		compressionFormat: DefaultCompressionFormat,
		compressionLevel:  DefaultCompressionLevel,
	}
}

func validOptions(opts *Options) bool {
	return opts != nil
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

func (opts *Options) WithCompresionLevel(compressionLevel int) *Options {
	opts.compressionLevel = compressionLevel
	return opts
}

func (opts *Options) WithMetadata(metadata []byte) *Options {
	opts.metadata = metadata
	return opts
}
