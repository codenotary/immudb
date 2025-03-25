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

package remoteapp

import (
	"time"

	"github.com/codenotary/immudb/embedded/appendable/multiapp"
)

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

func (opts *Options) Valid() bool {
	// TODO: implement signature `Validate() error``
	// TODO: Compression is not supported ATM, this must be disabled
	return opts != nil &&
		opts.Options.Validate() == nil &&
		opts.parallelUploads > 0 &&
		opts.parallelUploads < 100000 &&
		opts.retryMinDelay > 0 &&
		opts.retryMaxDelay > 0 &&
		opts.retryDelayExp > 1
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
