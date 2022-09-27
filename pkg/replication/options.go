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

package replication

import "time"

const DefaultChunkSize int = 64 * 1024 // 64 * 1024 64 KiB
const DefaultPrefetchTxBufferSize int = 100
const DefaultReplicationCommitConcurrency int = 10
const DefaultAllowTxDiscarding = false

type Options struct {
	masterDatabase   string
	masterAddress    string
	masterPort       int
	followerUsername string
	followerPassword string

	streamChunkSize int

	prefetchTxBufferSize         int
	replicationCommitConcurrency int

	allowTxDiscarding bool

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
		delayer:                      delayer,
		streamChunkSize:              DefaultChunkSize,
		prefetchTxBufferSize:         DefaultPrefetchTxBufferSize,
		replicationCommitConcurrency: DefaultReplicationCommitConcurrency,
		allowTxDiscarding:            DefaultAllowTxDiscarding,
	}
}

func (opts *Options) Valid() bool {
	return opts != nil &&
		opts.streamChunkSize > 0 &&
		opts.prefetchTxBufferSize > 0 &&
		opts.replicationCommitConcurrency > 0 &&
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

// WithPrefetchTxBufferSize sets tx buffer size
func (o *Options) WithPrefetchTxBufferSize(prefetchTxBufferSize int) *Options {
	o.prefetchTxBufferSize = prefetchTxBufferSize
	return o
}

// WithReplicationCommitConcurrency sets the number of goroutines doing replication
func (o *Options) WithReplicationCommitConcurrency(replicationCommitConcurrency int) *Options {
	o.replicationCommitConcurrency = replicationCommitConcurrency
	return o
}

// WithAllowTxDiscarding enable auto discarding of precommitted transactions
func (o *Options) WithAllowTxDiscarding(allowTxDiscarding bool) *Options {
	o.allowTxDiscarding = allowTxDiscarding
	return o
}

// WithDelayer sets delayer used to pause re-attempts
func (o *Options) WithDelayer(delayer Delayer) *Options {
	o.delayer = delayer
	return o
}
