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

package sql

import (
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
)

var defaultDistinctLimit = 1 << 20 // ~ 1mi rows

type Options struct {
	prefix        []byte
	distinctLimit int
	autocommit    bool
}

func DefaultOptions() *Options {
	return &Options{
		distinctLimit: defaultDistinctLimit,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", store.ErrInvalidOptions)
	}

	if opts.distinctLimit <= 0 {
		return fmt.Errorf("%w: invalid DistinctLimit value", store.ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithPrefix(prefix []byte) *Options {
	opts.prefix = prefix
	return opts
}

func (opts *Options) WithDistinctLimit(distinctLimit int) *Options {
	opts.distinctLimit = distinctLimit
	return opts
}

func (opts *Options) WithAutocommit(autocommit bool) *Options {
	opts.autocommit = autocommit
	return opts
}

func (opts *Options) GetPrefix() []byte {
	return opts.prefix
}

func (opts *Options) GetDistinctLimit() int {
	return opts.distinctLimit
}

func (opts *Options) GetAutoCommit() bool {
	return opts.autocommit
}
