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

package server

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/replication"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1")

	require.NoError(t, opts.Validate())

	opts.ReadTxPoolSize = 0
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)
}

func TestReplicaOptions(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1")

	opts.Replica = true

	opts.PrefetchTxBufferSize = replication.DefaultPrefetchTxBufferSize
	opts.ReplicationCommitConcurrency = replication.DefaultReplicationCommitConcurrency
	opts.SyncAcks = 0

	require.NoError(t, opts.Validate())

	opts.SyncAcks = 1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.ReplicationCommitConcurrency = 0
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrefetchTxBufferSize = 0
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)
}

func TestPrimaryOptions(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1")

	opts.Replica = false

	require.NoError(t, opts.Validate())

	opts.SyncReplication = false
	opts.SyncAcks = 1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.SyncReplication = true
	opts.SyncAcks = 0
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.AllowTxDiscarding = true
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.ReplicationCommitConcurrency = 1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrefetchTxBufferSize = 100
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrimaryPassword = "primary-pwd"
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrimaryUsername = "primary-username"
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrimaryPort = 3323
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrimaryHost = "localhost"
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.PrimaryDatabase = "primarydb"
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.SyncAcks = -1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)
}
