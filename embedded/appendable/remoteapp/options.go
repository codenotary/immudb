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

package remoteapp

import (
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/appendable/multiapp"
)

var ErrInvalidOptions = fmt.Errorf("%w: invalid remoteapp options", ErrIllegalArguments)

type Options struct {
	multiapp.Options
	parallelUploads int

	retryMinDelay    time.Duration
	retryMaxDelay    time.Duration
	retryDelayExp    float64
	retryDelayJitter float64
}

func DefaultOptions() *Options {
	return &Options{
		Options:          *multiapp.DefaultOptions(),
		parallelUploads:  10,
		retryMinDelay:    time.Second,
		retryMaxDelay:    2 * time.Minute,
		retryDelayExp:    2,
		retryDelayJitter: 0.1,
	}
}

// Validate returns nil if opts is a usable configuration, or a descriptive
// error wrapping ErrInvalidOptions otherwise.
//
// Compression is not yet supported by the remote appendable (see
// OpenAppendable in remote_app.go which rejects non-default
// CompressionFormat); callers must leave compression at the default.
func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}
	if err := opts.Options.Validate(); err != nil {
		return fmt.Errorf("%w: embedded multiapp options: %v", ErrInvalidOptions, err)
	}
	if opts.parallelUploads <= 0 {
		return fmt.Errorf("%w: parallelUploads must be > 0", ErrInvalidOptions)
	}
	if opts.parallelUploads >= 100000 {
		return fmt.Errorf("%w: parallelUploads must be < 100000", ErrInvalidOptions)
	}
	if opts.retryMinDelay <= 0 {
		return fmt.Errorf("%w: retryMinDelay must be > 0", ErrInvalidOptions)
	}
	if opts.retryMaxDelay <= 0 {
		return fmt.Errorf("%w: retryMaxDelay must be > 0", ErrInvalidOptions)
	}
	if opts.retryMaxDelay < opts.retryMinDelay {
		return fmt.Errorf("%w: retryMaxDelay must be >= retryMinDelay", ErrInvalidOptions)
	}
	if opts.retryDelayExp <= 1 {
		return fmt.Errorf("%w: retryDelayExp must be > 1", ErrInvalidOptions)
	}
	if opts.retryDelayJitter < 0 || opts.retryDelayJitter > 1 {
		return fmt.Errorf("%w: retryDelayJitter must be in [0, 1]", ErrInvalidOptions)
	}
	return nil
}

// Valid is a back-compat wrapper around Validate for existing callers.
func (opts *Options) Valid() bool {
	return opts.Validate() == nil
}

func (opts *Options) WithParallelUploads(parallelUploads int) *Options {
	opts.parallelUploads = parallelUploads
	return opts
}

func (opts *Options) WithRetryMinDelay(retryMinDelay time.Duration) *Options {
	opts.retryMinDelay = retryMinDelay
	return opts
}

func (opts *Options) WithRetryMaxDelay(retryMaxDelay time.Duration) *Options {
	opts.retryMaxDelay = retryMaxDelay
	return opts
}

func (opts *Options) WithRetryDelayExp(retryDelayExp float64) *Options {
	opts.retryDelayExp = retryDelayExp
	return opts
}

func (opts *Options) WithRetryDelayJitter(retryDelayJitter float64) *Options {
	opts.retryDelayJitter = retryDelayJitter
	return opts
}
