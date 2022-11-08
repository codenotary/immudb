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
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
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
	primaryClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(primaryPort))
	require.NoError(t, err)
	require.NotNil(t, primaryClient)

	mlr, err := primaryClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	pctx := metadata.NewOutgoingContext(context.Background(), mmd)

	// create database as primarydb in primary server
	_, err = primaryClient.CreateDatabaseV2(pctx, "primarydb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: 1},
		},
	})
	require.NoError(t, err)

	mdb, err := primaryClient.UseDatabase(pctx, &schema.Database{DatabaseName: "primarydb"})
	require.NoError(t, err)
	require.NotNil(t, mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	pctx = metadata.NewOutgoingContext(context.Background(), mmd)

	err = primaryClient.CreateUser(pctx, []byte("replicator"), []byte("replicator1Pwd!"), auth.PermissionAdmin, "primarydb")
	require.NoError(t, err)

	err = primaryClient.SetActiveUser(pctx, &schema.SetActiveUserRequest{Active: true, Username: "replicator"})
	require.NoError(t, err)

	// init replica client
	replicaPort := replicaServer.Listener.Addr().(*net.TCPAddr).Port
	replicaClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(replicaPort))
	require.NoError(t, err)
	require.NotNil(t, replicaClient)

	flr, err := replicaClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	fmd := metadata.Pairs("authorization", flr.Token)
	rctx := metadata.NewOutgoingContext(context.Background(), fmd)

	// create database as replica in replica server
	_, err = replicaClient.CreateDatabaseV2(rctx, "replicadb", &schema.DatabaseNullableSettings{
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

	_, err = replicaClient.UpdateDatabaseV2(rctx, "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			PrimaryPassword: &schema.NullableString{Value: "replicator1Pwd!"},
		},
	})
	require.NoError(t, err)

	fdb, err := replicaClient.UseDatabase(rctx, &schema.Database{DatabaseName: "replicadb"})
	require.NoError(t, err)
	require.NotNil(t, fdb)

	fmd = metadata.Pairs("authorization", fdb.Token)
	rctx = metadata.NewOutgoingContext(context.Background(), fmd)

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = replicaClient.Get(rctx, []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = primaryClient.Set(pctx, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	_, err = primaryClient.Set(pctx, []byte("key2"), []byte("value2"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicadb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(rctx, []byte("key1"))
		require.NoError(t, err)
	})

	fdb, err = replicaClient.UseDatabase(rctx, &schema.Database{DatabaseName: "defaultdb"})
	require.NoError(t, err)
	require.NotNil(t, fdb)

	fmd = metadata.Pairs("authorization", fdb.Token)
	rctx = metadata.NewOutgoingContext(context.Background(), fmd)

	t.Run("key1 should not exist in defaultdb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(rctx, []byte("key1"))
		require.Contains(t, err.Error(), "key not found")
	})
}

func TestSystemDBAndDefaultDBReplication(t *testing.T) {
	// init primary server
	primaryDir, err := ioutil.TempDir("", "primary-data")
	require.NoError(t, err)
	defer os.RemoveAll(primaryDir)

	primaryServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(primaryDir)

	primaryServer := server.DefaultServer().WithOptions(primaryServerOpts).(*server.ImmuServer)

	err = primaryServer.Initialize()
	require.NoError(t, err)

	go func() {
		primaryServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer primaryServer.Stop()

	// init primary client
	primaryPort := primaryServer.Listener.Addr().(*net.TCPAddr).Port
	primaryClient := ic.NewClient().WithOptions(ic.DefaultOptions().WithPort(primaryPort))
	require.NotNil(t, primaryClient)

	err = primaryClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	defer primaryClient.CloseSession(context.Background())

	// init replica server
	replicaDir, err := ioutil.TempDir("", "replica-data")
	require.NoError(t, err)
	defer os.RemoveAll(replicaDir)

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
	replicaClient := ic.NewClient().WithOptions(ic.DefaultOptions().WithPort(replicaPort))
	require.NotNil(t, replicaClient)

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
