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

package integration

import (
	"context"
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
	masterServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir("master-data")

	masterServer := server.DefaultServer().WithOptions(masterServerOpts).(*server.ImmuServer)
	defer os.RemoveAll(masterServerOpts.Dir)

	err := masterServer.Initialize()
	require.NoError(t, err)

	//init follower server
	followerServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir("follower-data")

	followerServer := server.DefaultServer().WithOptions(followerServerOpts).(*server.ImmuServer)
	defer os.RemoveAll(followerServerOpts.Dir)

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

	err = masterClient.CreateUser(mctx, []byte("follower"), []byte("follower1Pwd!"), auth.PermissionAdmin, "defaultdb")
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
	err = followerClient.CreateDatabase(fctx, &schema.DatabaseSettings{
		DatabaseName:     "replicateddb",
		Replica:          true,
		MasterDatabase:   "defaultdb",
		MasterAddress:    "127.0.0.1",
		MasterPort:       uint32(masterPort),
		FollowerUsername: "follower",
		FollowerPassword: "wrongPassword",
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = followerClient.UpdateDatabase(fctx, &schema.DatabaseSettings{
		DatabaseName:     "replicateddb",
		Replica:          true,
		MasterDatabase:   "defaultdb",
		MasterAddress:    "127.0.0.1",
		MasterPort:       uint32(masterPort),
		FollowerUsername: "follower",
		FollowerPassword: "follower1Pwd!",
	})
	require.NoError(t, err)

	fdb, err := followerClient.UseDatabase(fctx, &schema.Database{DatabaseName: "replicateddb"})
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

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicateddb@follower", func(t *testing.T) {
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
	masterServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir("master-data")

	masterServer := server.DefaultServer().WithOptions(masterServerOpts).(*server.ImmuServer)
	defer os.RemoveAll(masterServerOpts.Dir)

	err := masterServer.Initialize()
	require.NoError(t, err)

	go func() {
		masterServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer masterServer.Stop()

	// init master client
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).Port
	masterClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(masterPort))
	require.NoError(t, err)
	require.NotNil(t, masterClient)

	mlr, err := masterClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	//init follower server
	replicationOpts := &server.ReplicationOptions{
		MasterAddress:    "127.0.0.1",
		MasterPort:       masterPort,
		FollowerUsername: "immudb",
		FollowerPassword: "immudb",
	}
	followerServerOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir("follower-data").
		WithReplicationOptions(replicationOpts)

	followerServer := server.DefaultServer().WithOptions(followerServerOpts).(*server.ImmuServer)
	defer os.RemoveAll(followerServerOpts.Dir)

	err = followerServer.Initialize()
	require.NoError(t, err)

	go func() {
		followerServer.Start()
	}()

	time.Sleep(1 * time.Second)

	defer followerServer.Stop()

	// init follower client
	followerPort := followerServer.Listener.Addr().(*net.TCPAddr).Port
	followerClient, err := ic.NewImmuClient(ic.DefaultOptions().WithPort(followerPort))
	require.NoError(t, err)
	require.NotNil(t, followerClient)

	flr, err := followerClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	fmd := metadata.Pairs("authorization", flr.Token)
	fctx := metadata.NewOutgoingContext(context.Background(), fmd)

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = followerClient.Get(fctx, []byte("key1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "key not found")
	})

	_, err = masterClient.Set(mctx, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should exist in replicateddb@follower", func(t *testing.T) {
		_, err = followerClient.Get(fctx, []byte("key1"))
		require.NoError(t, err)
	})

	_, err = followerClient.Set(mctx, []byte("key2"), []byte("value2"))
	require.Contains(t, err.Error(), "database is read-only because it's a replica")
}
