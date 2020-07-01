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

package store

import (
	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"

	"runtime"
)

// Options ...
type Options struct {
	log logger.Logger
}

// DefaultOptions ...
func DefaultOptions(path string, log logger.Logger) (Options, badger.Options) {
	badgerOptions := badger.DefaultOptions(path).
		WithLogger(log.CloneWithLevel(logger.LogWarn)).
		WithSyncWrites(false).
		WithEventLogging(false)
	// set Truncate to true according to https://github.com/dgraph-io/badger/issues/476#issuecomment-388122680
	if runtime.GOOS == "windows" {
		badgerOptions.Truncate = true
	}
	return Options{log}, badgerOptions
}

// WriteOptions ...
type WriteOptions struct {
	asyncCommit bool
}

func makeWriteOptions(opts ...WriteOption) *WriteOptions {
	wo := &WriteOptions{}
	for _, f := range opts {
		f(wo)
	}
	return wo
}

// WriteOption ...
type WriteOption func(*WriteOptions)

// WithAsyncCommit ...
func WithAsyncCommit(async bool) WriteOption {
	return func(opts *WriteOptions) {
		opts.asyncCommit = async
	}
}
