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

package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := &Options{}
	require.ErrorIs(t, opts.Validate(), ErrInvalidOptions)

	delayer := &expBackoff{
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Minute,
		retryDelayExp: 2,
		retryJitter:   0.1,
	}

	opts.WithPrimaryDatabase("defaultdb").
		WithPrimaryHost("127.0.0.1").
		WithPrimaryPort(3322).
		WithPrimaryUsername("immudbUsr").
		WithPrimaryPassword("immdubPwd").
		WithStreamChunkSize(DefaultChunkSize).
		WithPrefetchTxBufferSize(DefaultPrefetchTxBufferSize).
		WithReplicationCommitConcurrency(DefaultReplicationCommitConcurrency).
		WithAllowTxDiscarding(true).
		WithSkipIntegrityCheck(true).
		WithWaitForIndexing(true).
		WithDelayer(delayer)

	require.Equal(t, "defaultdb", opts.primaryDatabase)
	require.Equal(t, "127.0.0.1", opts.primaryHost)
	require.Equal(t, 3322, opts.primaryPort)
	require.Equal(t, "immudbUsr", opts.primaryUsername)
	require.Equal(t, "immdubPwd", opts.primaryPassword)
	require.Equal(t, DefaultChunkSize, opts.streamChunkSize)
	require.Equal(t, DefaultPrefetchTxBufferSize, opts.prefetchTxBufferSize)
	require.Equal(t, DefaultReplicationCommitConcurrency, opts.replicationCommitConcurrency)
	require.True(t, opts.allowTxDiscarding)
	require.True(t, opts.skipIntegrityCheck)
	require.True(t, opts.waitForIndexing)
	require.Equal(t, delayer, opts.delayer)

	require.NoError(t, opts.Validate())

	defaultOpts := DefaultOptions()
	require.NotNil(t, defaultOpts)
	require.NoError(t, defaultOpts.Validate())
}
