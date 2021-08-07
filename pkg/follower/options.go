/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package follower

import "time"

type Options struct {
	srcDatabase string
	srcAddress  string
	srcPort     int
	followerUsr string
	followerPwd string

	streamChunkSize int

	retryMinDelay    time.Duration
	retryMaxDelay    time.Duration
	retryDelayExp    float64
	retryDelayJitter float64
}

func DefaultOptions() *Options {
	return &Options{
		retryMinDelay:    time.Second,
		retryMaxDelay:    2 * time.Minute,
		retryDelayExp:    2,
		retryDelayJitter: 0.1,
	}
}

func (opts *Options) Valid() bool {
	return opts != nil &&
		opts.streamChunkSize > 0 &&
		opts.retryMinDelay > 0 &&
		opts.retryMaxDelay > 0 &&
		opts.retryDelayExp > 1
}

// WithSrcDatabase sets the source database name
func (o *Options) WithSrcDatabase(srcDatabase string) *Options {
	o.srcDatabase = srcDatabase
	return o
}

// WithSrcAddress sets the source database address
func (o *Options) WithSrcAddress(srcAddress string) *Options {
	o.srcAddress = srcAddress
	return o
}

// WithSrcPort sets the source database port
func (o *Options) WithSrcPort(srcPort int) *Options {
	o.srcPort = srcPort
	return o
}

// WithFollowerUsr sets follower username
func (o *Options) WithFollowerUsr(followerUsr string) *Options {
	o.followerUsr = followerUsr
	return o
}

// WithFollowerPwd sets follower password
func (o *Options) WithFollowerPwd(followerPwd string) *Options {
	o.followerPwd = followerPwd
	return o
}

// WithStreamChunkSize sets streaming chunk size
func (o *Options) WithStreamChunkSize(streamChunkSize int) *Options {
	o.streamChunkSize = streamChunkSize
	return o
}
