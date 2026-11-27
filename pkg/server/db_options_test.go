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

package server

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/replication"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	dir := t.TempDir()

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1", "user")

	require.NoError(t, opts.Validate())

	opts.ReadTxPoolSize = 0
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)
}

func TestReplicaOptions(t *testing.T) {
	dir := t.TempDir()

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1", "user")

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
	dir := t.TempDir()

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1", "user")

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

	opts.TruncationFrequency = -1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)

	opts.RetentionPeriod = -1
	require.ErrorIs(t, opts.Validate(), ErrIllegalArguments)
}

func TestDatabaseNullableSettingsOmitsPrimaryPassword(t *testing.T) {
	dir := t.TempDir()

	s, closer := testServer(DefaultOptions().WithDir(dir))
	defer closer()

	opts := s.defaultDBOptions("db1", "user")
	opts.Replica = true
	opts.PrimaryUsername = "primary-username"
	opts.PrimaryPassword = "primary-pwd"

	rs := opts.databaseNullableSettings().ReplicationSettings
	require.Equal(t, "primary-username", rs.PrimaryUsername.GetValue())
	require.Nil(t, rs.PrimaryPassword)
}

func TestOverwriteWithExcludeCommitTime(t *testing.T) {
	s, closer := testServer(DefaultOptions().WithDir(t.TempDir()))
	defer closer()

	tests := []struct {
		name     string
		current  bool
		settings *schema.NullableBool
		wantErr  bool
	}{
		{"flip to true", false, &schema.NullableBool{Value: true}, true},
		{"flip to false", true, &schema.NullableBool{Value: false}, true},
		{"unchanged value", true, &schema.NullableBool{Value: true}, false},
		{"not provided", false, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := s.defaultDBOptions("db1", "user")
			opts.ExcludeCommitTime = tt.current

			err := s.overwriteWith(opts, &schema.DatabaseNullableSettings{ExcludeCommitTime: tt.settings}, true)

			if tt.wantErr {
				require.ErrorIs(t, err, ErrIllegalArguments)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.current, opts.ExcludeCommitTime)
		})
	}
}
