/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package sql

import (
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
)

const (
	defaultDistinctLimit  = 1 << 20 // ~ 1mi rows
	defaultSortBufferSize = 1024
)

type Options struct {
	prefix                        []byte
	sortBufferSize                int
	distinctLimit                 int
	maxWindowRows                 int // 0 = unlimited
	maxKeyLen                     int // 0 = leave package default in place
	autocommit                    bool
	lazyIndexConstraintValidation bool
	parseTxMetadata               func([]byte) (map[string]interface{}, error)

	multidbHandler MultiDBHandler
	tableResolvers []TableResolver
}

func DefaultOptions() *Options {
	return &Options{
		sortBufferSize: defaultSortBufferSize,
		distinctLimit:  defaultDistinctLimit,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", store.ErrInvalidOptions)
	}

	if opts.distinctLimit <= 0 {
		return fmt.Errorf("%w: invalid DistinctLimit value", store.ErrInvalidOptions)
	}

	if opts.sortBufferSize <= 0 {
		return fmt.Errorf("%w: invalid SortBufferSize value", store.ErrInvalidOptions)
	}

	// 0 means "leave the package default" (no override). Anything explicit
	// must fit a uint16 length-prefix (the on-disk encoding ceiling) and
	// be at least wide enough for the small system PKs.
	if opts.maxKeyLen != 0 && (opts.maxKeyLen < 64 || opts.maxKeyLen > 65535) {
		return fmt.Errorf("%w: MaxKeyLen must be in [64, 65535]", store.ErrInvalidOptions)
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

func (opts *Options) WithLazyIndexConstraintValidation(lazyIndexConstraintValidation bool) *Options {
	opts.lazyIndexConstraintValidation = lazyIndexConstraintValidation
	return opts
}

func (opts *Options) WithMultiDBHandler(multidbHandler MultiDBHandler) *Options {
	opts.multidbHandler = multidbHandler
	return opts
}

// WithSortBufferSize specifies the size of the buffer used to sort rows in-memory
// when executing queries containing an ORDER BY clause. The default value is 1024.
// Increasing this value improves sorting speed at the expense of higher memory usage.
func (opts *Options) WithSortBufferSize(size int) *Options {
	opts.sortBufferSize = size
	return opts
}

func (opts *Options) WithMaxWindowRows(maxRows int) *Options {
	opts.maxWindowRows = maxRows
	return opts
}

func (opts *Options) WithParseTxMetadataFunc(parseFunc func([]byte) (map[string]interface{}, error)) *Options {
	opts.parseTxMetadata = parseFunc
	return opts
}

func (opts *Options) WithTableResolvers(resolvers ...TableResolver) *Options {
	opts.tableResolvers = append(opts.tableResolvers, resolvers...)
	return opts
}

// WithMaxKeyLen overrides the engine-side maximum length (in bytes) for
// indexed VARCHAR columns. The value is clamped to [64, 65535] by Validate.
// When set, NewEngine assigns it to the package-level MaxKeyLen so that
// EncodeRawValueAsKey (and every other site that consults MaxKeyLen) sees
// the new ceiling. Note that the underlying store layer still enforces
// its own composite-key limit (embedded/store.MaxKeyLen, default 1024 B),
// which is the practical ceiling at insert time. Pass 0 (or omit the
// call) to leave the package default in place — the value is unchanged.
func (opts *Options) WithMaxKeyLen(maxKeyLen int) *Options {
	opts.maxKeyLen = maxKeyLen
	return opts
}
