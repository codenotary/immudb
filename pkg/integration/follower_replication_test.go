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
	//init master server
	masterDir, err := ioutil.TempDir("", "master-data")
	require.NoError(t, err)
	defer os.RemoveAll(masterDir)

	masterServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(masterDir)

	masterServer := server.DefaultServer().WithOptions(masterServerOpts).(*server.ImmuServer)

	err = masterServer.Initialize()
	require.NoError(t, err)

	//init follower server
	followerDir, err := ioutil.TempDir("", "follower-data")
	require.NoError(t, err)
	defer os.RemoveAll(followerDir)

	followerServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(followerDir)

	followerServer := server.DefaultServer().WithOptions(followerServerOpts).(*server.ImmuServer)

	err = followerServer.Initialize()
	require.NoError(t, err)

	go func() {
		masterServer.Start()
	}()

	go func() {
		followerServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer func() {
		masterServer.Stop()

		time.Sleep(1 * time.Second)

		followerServer.Stop()
	}()

	// init master client
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).Port
	masterClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(masterPort))
	require.NoError(t, err)
	require.NotNil(t, masterClient)

	mlr, err := masterClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	// create database as masterdb in master server
	_, err = masterClient.CreateDatabaseV2(mctx, "masterdb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: 1},
		},
	})
	require.NoError(t, err)

	mdb, err := masterClient.UseDatabase(mctx, &schema.Database{DatabaseName: "masterdb"})
	require.NoError(t, err)
	require.NotNil(t, mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	mctx = metadata.NewOutgoingContext(context.Background(), mmd)

	err = masterClient.CreateUser(mctx, []byte("follower"), []byte("follower1Pwd!"), auth.PermissionAdmin, "masterdb")
	require.NoError(t, err)

	err = masterClient.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: "follower"})
	require.NoError(t, err)

	// init follower client
	followerPort := followerServer.Listener.Addr().(*net.TCPAddr).Port
	followerClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(followerPort))
	require.NoError(t, err)
	require.NotNil(t, followerClient)

	flr, err := followerClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	fmd := metadata.Pairs("authorization", flr.Token)
	fctx := metadata.NewOutgoingContext(context.Background(), fmd)

	// create database as replica in follower server
	_, err = followerClient.CreateDatabaseV2(fctx, "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: true},
			SyncReplication:  &schema.NullableBool{Value: true},
			MasterDatabase:   &schema.NullableString{Value: "masterdb"},
			MasterAddress:    &schema.NullableString{Value: "127.0.0.1"},
			MasterPort:       &schema.NullableUint32{Value: uint32(masterPort)},
			FollowerUsername: &schema.NullableString{Value: "follower"},
			FollowerPassword: &schema.NullableString{Value: "wrongPassword"},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	_, err = followerClient.UpdateDatabaseV2(fctx, "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			FollowerPassword: &schema.NullableString{Value: "follower1Pwd!"},
		},
	})
	require.NoError(t, err)

	fdb, err := followerClient.UseDatabase(fctx, &schema.Database{DatabaseName: "replicadb"})
	require.NoError(t, err)
	require.NotNil(t, fdb)

	fmd = metadata.Pairs("authorization", fdb.Token)
	fctx = metadata.NewOutgoingContext(context.Background(), fmd)

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = followerClient.Get(fctx, []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = masterClient.Set(mctx, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	_, err = masterClient.Set(mctx, []byte("key2"), []byte("value2"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicadb@follower", func(t *testing.T) {
		_, err = followerClient.Get(fctx, []byte("key1"))
		require.NoError(t, err)
	})

	fdb, err = followerClient.UseDatabase(fctx, &schema.Database{DatabaseName: "defaultdb"})
	require.NoError(t, err)
	require.NotNil(t, fdb)

	fmd = metadata.Pairs("authorization", fdb.Token)
	fctx = metadata.NewOutgoingContext(context.Background(), fmd)

	t.Run("key1 should not exist in defaultdb@follower", func(t *testing.T) {
		_, err = followerClient.Get(fctx, []byte("key1"))
		require.Contains(t, err.Error(), "key not found")
	})
}

func TestSystemDBAndDefaultDBReplication(t *testing.T) {
	//init master server
	masterDir, err := ioutil.TempDir("", "master-data")
	require.NoError(t, err)
	defer os.RemoveAll(masterDir)

	masterServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(masterDir)

	masterServer := server.DefaultServer().WithOptions(masterServerOpts).(*server.ImmuServer)

	err = masterServer.Initialize()
	require.NoError(t, err)

	go func() {
		masterServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer masterServer.Stop()

	// init master client
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).Port
	masterClient := ic.NewClient().WithOptions(ic.DefaultOptions().WithPort(masterPort))
	require.NotNil(t, masterClient)

	err = masterClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	defer masterClient.CloseSession(context.Background())

	//init follower server
	followerDir, err := ioutil.TempDir("", "follower-data")
	require.NoError(t, err)
	defer os.RemoveAll(followerDir)

	replicationOpts := &server.ReplicationOptions{
		IsReplica:                    true,
		MasterAddress:                "127.0.0.1",
		MasterPort:                   masterPort,
		FollowerUsername:             "immudb",
		FollowerPassword:             "immudb",
		PrefetchTxBufferSize:         100,
		ReplicationCommitConcurrency: 1,
	}
	followerServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(followerDir).
		WithReplicationOptions(replicationOpts)

	followerServer := server.DefaultServer().WithOptions(followerServerOpts).(*server.ImmuServer)

	err = followerServer.Initialize()
	require.NoError(t, err)

	go func() {
		followerServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer followerServer.Stop()

	// init follower client
	followerPort := followerServer.Listener.Addr().(*net.TCPAddr).Port
	followerClient := ic.NewClient().WithOptions(ic.DefaultOptions().WithPort(followerPort))
	require.NotNil(t, followerClient)

	err = followerClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	defer followerClient.CloseSession(context.Background())

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = followerClient.Get(context.Background(), []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = masterClient.Set(context.Background(), []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicateddb@follower", func(t *testing.T) {
		_, err = followerClient.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
	})

	_, err = followerClient.Set(context.Background(), []byte("key2"), []byte("value2"))
	require.Contains(t, err.Error(), "database is read-only because it's a replica")
}
