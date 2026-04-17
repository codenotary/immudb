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

package replication

import (
	"fmt"
	"time"

	"github.com/codenotary/immudb/pkg/client"
)

const (
	DefaultChunkSize                    int = 64 * 1024 // 64 * 1024 64 KiB
	DefaultPrefetchTxBufferSize         int = 100
	DefaultReplicationCommitConcurrency int = 10
	DefaultAllowTxDiscarding                = false
	DefaultSkipIntegrityCheck               = false
	DefaultWaitForIndexing                  = false
)

type ClientFactory func(string, int) client.ImmuClient

type Options struct {
	primaryDatabase string
	primaryHost     string
	primaryPort     int
	primaryUsername string
	primaryPassword string

	streamChunkSize int

	prefetchTxBufferSize         int
	replicationCommitConcurrency int

	// fetchPipelineDepth (A4) controls how many ExportTx requests can be
	// in flight on the export stream concurrently. Default 1 = legacy
	// strictly-serial fetch loop; > 1 enables pipelined fetch.
	//
	// IMPORTANT: pipelined fetch (depth > 1) is currently scaffolding-only
	// — the option is plumbed through but the replicator's serial
	// fetchNextTx loop has not yet been refactored into a producer/
	// consumer split. Wiring the actual pipeline requires non-trivial
	// changes to:
	//   - sync-replication state machinery (each ExportTxRequest carries
	//     a ReplicaState snapshot; pipelined requests need coherent
	//     ordering of those snapshots),
	//   - error handling (a mid-pipeline failure must drop pending
	//     responses without leaking the gRPC stream),
	//   - back-pressure (a slow replica must not let the primary buffer
	//     unbounded export traffic).
	// These are the risks called out in immudb-improvements.md A4.
	// Setting this > 1 today is a no-op other than emitting a one-time
	// warning at startup.
	fetchPipelineDepth int

	allowTxDiscarding  bool
	skipIntegrityCheck bool
	waitForIndexing    bool

	delayer       Delayer
	clientFactory ClientFactory
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
		fetchPipelineDepth:           1, // strictly-serial fetch (A4 not yet wired)
		allowTxDiscarding:            DefaultAllowTxDiscarding,
		skipIntegrityCheck:           DefaultSkipIntegrityCheck,
		waitForIndexing:              DefaultWaitForIndexing,
		clientFactory:                newClient,
	}
}

// WithFetchPipelineDepth (A4) reserves the option for future pipelined
// fetch from the primary. See Options.fetchPipelineDepth doc for the
// current scaffolding limitation — depth > 1 is accepted but not yet
// honoured by the replicator loop.
func (o *Options) WithFetchPipelineDepth(depth int) *Options {
	if depth < 1 {
		depth = 1
	}
	o.fetchPipelineDepth = depth
	return o
}

func newClient(host string, port int) client.ImmuClient {
	opts := client.DefaultOptions().
		WithAddress(host).
		WithPort(port).
		WithDisableIdentityCheck(true)

	return client.NewClient().WithOptions(opts)
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.streamChunkSize <= 0 {
		return fmt.Errorf("%w: invalid StreamChunkSize", ErrInvalidOptions)
	}

	if opts.prefetchTxBufferSize <= 0 {
		return fmt.Errorf("%w: invalid PrefetchTxBufferSize", ErrInvalidOptions)
	}

	if opts.replicationCommitConcurrency <= 0 {
		return fmt.Errorf("%w: invalid ReplicationCommitConcurrency", ErrInvalidOptions)
	}

	if opts.delayer == nil {
		return fmt.Errorf("%w: invalid Delayer", ErrInvalidOptions)
	}

	return nil
}

// WithPrimaryDatabase sets the source database name
func (o *Options) WithPrimaryDatabase(primaryDatabase string) *Options {
	o.primaryDatabase = primaryDatabase
	return o
}

// WithPrimaryHost sets the source database address
func (o *Options) WithPrimaryHost(primaryHost string) *Options {
	o.primaryHost = primaryHost
	return o
}

// WithPrimaryPort sets the source database port
func (o *Options) WithPrimaryPort(primaryPort int) *Options {
	o.primaryPort = primaryPort
	return o
}

// WithPrimaryUsername sets username used for replication
func (o *Options) WithPrimaryUsername(primaryUsername string) *Options {
	o.primaryUsername = primaryUsername
	return o
}

// WithPrimaryPassword sets password used for replication
func (o *Options) WithPrimaryPassword(primaryPassword string) *Options {
	o.primaryPassword = primaryPassword
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

// WithSkipIntegrityCheck disable integrity checks when reading data during replication
func (o *Options) WithSkipIntegrityCheck(skipIntegrityCheck bool) *Options {
	o.skipIntegrityCheck = skipIntegrityCheck
	return o
}

// WithWaitForIndexing wait for indexing to be up to date during replication
func (o *Options) WithWaitForIndexing(waitForIndexing bool) *Options {
	o.waitForIndexing = waitForIndexing
	return o
}

// WithDelayer sets delayer used to pause re-attempts
func (o *Options) WithDelayer(delayer Delayer) *Options {
	o.delayer = delayer
	return o
}

// WithClientFactoryFunc specifies a function to instantiate a new client
func (o *Options) WithClientFactoryFunc(clientFactory func(string, int) client.ImmuClient) *Options {
	o.clientFactory = clientFactory
	return o
}
