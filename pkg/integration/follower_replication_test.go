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

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	//init primary server
	primaryDir := t.TempDir()

	primaryServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(primaryDir)

	primaryServer := server.DefaultServer().WithOptions(primaryServerOpts).(*server.ImmuServer)

	err := primaryServer.Initialize()
	require.NoError(t, err)

	//init replica server
	replicaDir := t.TempDir()

	replicaServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(replicaDir)

	replicaServer := server.DefaultServer().WithOptions(replicaServerOpts).(*server.ImmuServer)

	err = replicaServer.Initialize()
	require.NoError(t, err)

	go func() {
		primaryServer.Start()
	}()

	go func() {
		replicaServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer func() {
		primaryServer.Stop()

		time.Sleep(1 * time.Second)

		replicaServer.Stop()
	}()

	// init primary client
	primaryPort := primaryServer.Listener.Addr().(*net.TCPAddr).Port

	primaryOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(primaryPort)

	primaryClient := ic.NewClient().WithOptions(primaryOpts)
	require.NoError(t, err)

	err = primaryClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	// create database as primarydb in primary server
	_, err = primaryClient.CreateDatabaseV2(context.Background(), "primarydb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: 1},
		},
	})
	require.NoError(t, err)

	err = primaryClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = primaryClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "primarydb")
	require.NoError(t, err)

	defer primaryClient.CloseSession(context.Background())

	err = primaryClient.CreateUser(context.Background(), []byte("replicator"), []byte("replicator1Pwd!"), auth.PermissionAdmin, "primarydb")
	require.NoError(t, err)

	err = primaryClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{Active: true, Username: "replicator"})
	require.NoError(t, err)

	// init replica client
	replicaPort := replicaServer.Listener.Addr().(*net.TCPAddr).Port

	replicaOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(replicaPort)

	replicaClient := ic.NewClient().WithOptions(replicaOpts)
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	// create database as replica in replica server
	_, err = replicaClient.CreateDatabaseV2(context.Background(), "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: true},
			SyncReplication: &schema.NullableBool{Value: true},
			PrimaryDatabase: &schema.NullableString{Value: "primarydb"},
			PrimaryHost:     &schema.NullableString{Value: "127.0.0.1"},
			PrimaryPort:     &schema.NullableUint32{Value: uint32(primaryPort)},
			PrimaryUsername: &schema.NullableString{Value: "replicator"},
			PrimaryPassword: &schema.NullableString{Value: "wrongPassword"},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	_, err = replicaClient.UpdateDatabaseV2(context.Background(), "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			PrimaryPassword: &schema.NullableString{Value: "replicator1Pwd!"},
		},
	})
	require.NoError(t, err)

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "replicadb")
	require.NoError(t, err)

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = primaryClient.Set(context.Background(), []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	_, err = primaryClient.Set(context.Background(), []byte("key2"), []byte("value2"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicadb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
	})

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	t.Run("key1 should not exist in defaultdb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.Contains(t, err.Error(), "key not found")
	})

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)
}

func TestSystemDBAndDefaultDBReplication(t *testing.T) {
	// init primary server
	primaryDir := t.TempDir()

	primaryServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(primaryDir)

	primaryServer := server.DefaultServer().WithOptions(primaryServerOpts).(*server.ImmuServer)

	err := primaryServer.Initialize()
	require.NoError(t, err)

	go func() {
		primaryServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer primaryServer.Stop()

	// init primary client
	primaryPort := primaryServer.Listener.Addr().(*net.TCPAddr).Port

	primaryOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(primaryPort)

	primaryClient := ic.NewClient().WithOptions(primaryOpts)
	require.NoError(t, err)

	err = primaryClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	defer primaryClient.CloseSession(context.Background())

	// init replica server
	replicaDir := t.TempDir()

	replicationOpts := &server.ReplicationOptions{
		IsReplica:                    true,
		PrimaryHost:                  "127.0.0.1",
		PrimaryPort:                  primaryPort,
		PrimaryUsername:              "immudb",
		PrimaryPassword:              "immudb",
		PrefetchTxBufferSize:         100,
		ReplicationCommitConcurrency: 1,
	}

	replicaServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(replicaDir).
		WithReplicationOptions(replicationOpts)

	replicaServer := server.DefaultServer().WithOptions(replicaServerOpts).(*server.ImmuServer)

	err = replicaServer.Initialize()
	require.NoError(t, err)

	go func() {
		replicaServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer replicaServer.Stop()

	// init replica client
	replicaPort := replicaServer.Listener.Addr().(*net.TCPAddr).Port

	replicaOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(replicaPort)

	replicaClient := ic.NewClient().WithOptions(replicaOpts)
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	defer replicaClient.CloseSession(context.Background())

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = primaryClient.Set(context.Background(), []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicateddb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
	})

	_, err = replicaClient.Set(context.Background(), []byte("key2"), []byte("value2"))
	require.Contains(t, err.Error(), "database is read-only because it's a replica")
}
