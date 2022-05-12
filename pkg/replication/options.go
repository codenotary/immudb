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

package replication

import "time"

const DefaultChunkSize int = 64 * 1024 // 64 * 1024 64 KiB

type Options struct {
	masterDatabase   string
	masterAddress    string
	masterPort       int
	followerUsername string
	followerPassword string

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

// WithMasterDatabase sets the source database name
func (o *Options) WithMasterDatabase(masterDatabase string) *Options {
	o.masterDatabase = masterDatabase
	return o
}

// WithMasterAddress sets the source database address
func (o *Options) WithMasterAddress(masterAddress string) *Options {
	o.masterAddress = masterAddress
	return o
}

// WithMasterPort sets the source database port
func (o *Options) WithMasterPort(masterPort int) *Options {
	o.masterPort = masterPort
	return o
}

// WithFollowerUsername sets username used for replication
func (o *Options) WithFollowerUsername(followerUsername string) *Options {
	o.followerUsername = followerUsername
	return o
}

// WithFollowerPassword sets password used for replication
func (o *Options) WithFollowerPassword(followerPassword string) *Options {
	o.followerPassword = followerPassword
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
