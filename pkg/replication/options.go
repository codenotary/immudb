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

package replication

import "time"

const DefaultChunkSize int = 64 * 1024 // 64 * 1024 64 KiB

type Options struct {
	srcDatabase string
	srcAddress  string
	srcPort     int
	followerUsr string
	followerPwd string

	streamChunkSize int

	delayer Delayer
}

func DefaultOptions() *Options {
	delayer := &expBackoff{
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Minute,
		retryDelayExp: 2,
		retryJitter:   0.1,
	}

	return &Options{
		delayer:         delayer,
		streamChunkSize: DefaultChunkSize,
	}
}

func (opts *Options) Valid() bool {
	return opts != nil &&
		opts.streamChunkSize > 0 &&
		opts.delayer != nil
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

// WithDelayer sets delayer used to pause re-attempts
func (o *Options) WithDelayer(delayer Delayer) *Options {
	o.delayer = delayer
	return o
}
