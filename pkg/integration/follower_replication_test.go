/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/replication"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/stream"
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

	_, err = primaryClient.ExportTx(context.Background(), &schema.ExportTxRequest{
		Tx:                 uint64(1),
		AllowPreCommitted:  false,
		SkipIntegrityCheck: true,
		ReplicaState:       &schema.ReplicaState{},
	})
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
		require.ErrorContains(t, err, embedded.ErrKeyNotFound.Error())
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
		require.ErrorContains(t, err, embedded.ErrKeyNotFound.Error())
	})

	for i := 0; i < 100; i++ {
		_, err = primaryClient.Set(context.Background(), []byte("key1"), make([]byte, 150))
		require.NoError(t, err)
	}

	// create database as replica in replica server
	_, err = replicaClient.CreateDatabaseV2(context.Background(), "replicadb2", &schema.DatabaseNullableSettings{
		MaxValueLen: &schema.NullableUint32{Value: 100},
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: true},
			SyncReplication: &schema.NullableBool{Value: false},
			PrimaryDatabase: &schema.NullableString{Value: "primarydb"},
			PrimaryHost:     &schema.NullableString{Value: "127.0.0.1"},
			PrimaryPort:     &schema.NullableUint32{Value: uint32(primaryPort)},
			PrimaryUsername: &schema.NullableString{Value: "replicator"},
			PrimaryPassword: &schema.NullableString{Value: "replicator1Pwd!"},
		},
	})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	primaryServer.Stop()

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	replicaServer.Stop()
}

func TestAsyncReplication(t *testing.T) {
	//init primary server
	primaryDir := t.TempDir()

	primaryServerOpts := server.DefaultOptions().
		WithMetricsServer(true).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithPProf(true).
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
			SyncReplication: &schema.NullableBool{Value: false},
			SyncAcks:        &schema.NullableUint32{Value: 0},
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
			SyncReplication: &schema.NullableBool{Value: false},
			PrimaryDatabase: &schema.NullableString{Value: "primarydb"},
			PrimaryHost:     &schema.NullableString{Value: "127.0.0.1"},
			PrimaryPort:     &schema.NullableUint32{Value: uint32(primaryPort)},
			PrimaryUsername: &schema.NullableString{Value: "replicator"},
			PrimaryPassword: &schema.NullableString{Value: "replicator1Pwd!"},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "replicadb")
	require.NoError(t, err)

	keyCount := 100

	for i := 0; i < keyCount; i++ {
		ei := keyCount + i

		_, err = primaryClient.Set(context.Background(), []byte(fmt.Sprintf("key%d", ei)), []byte(fmt.Sprintf("value%d", ei)))
		require.NoError(t, err)
	}

	err = primaryClient.CloseSession(context.Background())
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	// keys should exist in replicadb@replica"
	for i := 0; i < keyCount; i++ {
		ei := keyCount + i

		_, err = replicaClient.Get(context.Background(), []byte(fmt.Sprintf("key%d", ei)))
		require.NoError(t, err)
	}

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	replicaServer.Stop()
	primaryServer.Stop()
}

func TestReplicationTxDiscarding(t *testing.T) {
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

	_, err = replicaClient.CreateDatabaseV2(context.Background(), "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: false},
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: 1},
		},
	})
	require.NoError(t, err)

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "replicadb")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err = replicaClient.Set(ctx, []byte("key1"), []byte("value1"))
	require.Error(t, err)

	_, err = replicaClient.UpdateDatabaseV2(context.Background(), "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:           &schema.NullableBool{Value: true},
			SyncReplication:   &schema.NullableBool{Value: true},
			AllowTxDiscarding: &schema.NullableBool{Value: false},
			PrimaryDatabase:   &schema.NullableString{Value: "primarydb"},
			PrimaryHost:       &schema.NullableString{Value: "127.0.0.1"},
			PrimaryPort:       &schema.NullableUint32{Value: uint32(primaryPort)},
			PrimaryUsername:   &schema.NullableString{Value: "replicator"},
			PrimaryPassword:   &schema.NullableString{Value: "replicator1Pwd!"},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	_, err = replicaClient.UpdateDatabaseV2(context.Background(), "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			AllowTxDiscarding: &schema.NullableBool{Value: true},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.ErrorContains(t, err, embedded.ErrKeyNotFound.Error())
	})

	_, err = primaryClient.Set(context.Background(), []byte("key11"), []byte("value11"))
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	t.Run("key1 should exist in replicadb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key11"))
		require.NoError(t, err)
	})

	err = replicaClient.CloseSession(context.Background())
	require.NoError(t, err)

	err = primaryClient.CloseSession(context.Background())
	require.NoError(t, err)

	replicaServer.Stop()
	primaryServer.Stop()
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

	primaryClientOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(primaryPort)

	primaryClient := ic.NewClient().WithOptions(primaryClientOpts)
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

	replicaClientOpts := ic.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(replicaPort)

	replicaClient := ic.NewClient().WithOptions(replicaClientOpts)
	require.NoError(t, err)

	err = replicaClient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	defer replicaClient.CloseSession(context.Background())

	t.Run("key1 should not exist", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.ErrorContains(t, err, embedded.ErrKeyNotFound.Error())
	})

	_, err = primaryClient.Set(context.Background(), []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	t.Run("key1 should exist in replicateddb@replica", func(t *testing.T) {
		_, err = replicaClient.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
	})

	_, err = replicaClient.Set(context.Background(), []byte("key2"), []byte("value2"))
	require.ErrorContains(t, err, database.ErrIsReplica.Error())
}

func BenchmarkExportTx(b *testing.B) {
	//init  server
	serverOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithMaxRecvMsgSize(204939000).
		WithSynced(false).
		WithDir(b.TempDir())

	serverOpts.SessionsOptions.WithMaxSessions(200)

	srv := server.DefaultServer().WithOptions(serverOpts).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	time.Sleep(1 * time.Second)

	// init primary client
	port := srv.Listener.Addr().(*net.TCPAddr).Port

	opts := ic.DefaultOptions().
		WithDir(b.TempDir()).
		WithPort(port)

	client := ic.NewClient().WithOptions(opts)

	err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		panic(err)
	}

	// create database as primarydb in primary server
	_, err = client.CreateDatabaseV2(context.Background(), "db1", &schema.DatabaseNullableSettings{
		MaxConcurrency: &schema.NullableUint32{Value: 200},
		VLogCacheSize:  &schema.NullableUint32{Value: 0}, // disable vLogCache
	})
	if err != nil {
		panic(err)
	}

	err = client.CloseSession(context.Background())
	if err != nil {
		panic(err)
	}

	err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "db1")
	if err != nil {
		panic(err)
	}
	defer client.CloseSession(context.Background())

	// commit some transactions
	workers := 10
	txsPerWorker := 100
	entriesPerTx := 100
	keyLen := 40
	valLen := 256

	kvs := make([]*schema.KeyValue, entriesPerTx)

	for i := 0; i < entriesPerTx; i++ {
		kvs[i] = &schema.KeyValue{
			Key:   make([]byte, keyLen),
			Value: make([]byte, valLen),
		}

		binary.BigEndian.PutUint64(kvs[i].Key, uint64(i))
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < txsPerWorker; j++ {
				_, err := client.SetAll(context.Background(), &schema.SetRequest{
					KVs: kvs,
				})
				if err != nil {
					panic(err)
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()

	replicators := 1
	txsPerReplicator := workers * txsPerWorker / replicators

	clientReplicators := make([]ic.ImmuClient, replicators)

	for r := 0; r < replicators; r++ {
		opts := ic.DefaultOptions().
			WithDir(b.TempDir()).
			WithPort(port)

		client := ic.NewClient().WithOptions(opts)

		err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "db1")
		if err != nil {
			panic(err)
		}
		defer client.CloseSession(context.Background())

		clientReplicators[r] = client
	}

	streamServiceFactory := stream.NewStreamServiceFactory(replication.DefaultChunkSize)

	b.ResetTimer()

	// measure exportTx performance
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(replicators)

		for r := 0; r < replicators; r++ {
			go func(r int) {
				defer wg.Done()

				client := clientReplicators[r]

				for tx := 1; tx <= txsPerReplicator; tx++ {
					exportTxStream, err := client.ExportTx(context.Background(), &schema.ExportTxRequest{
						Tx:                 uint64(1 + r*txsPerReplicator + tx),
						AllowPreCommitted:  false,
						SkipIntegrityCheck: true,
					})
					if err != nil {
						panic(err)
					}

					receiver := streamServiceFactory.NewMsgReceiver(exportTxStream)
					_, _, err = receiver.ReadFully()
					if err != nil {
						panic(err)
					}
				}
			}(r)
		}

		wg.Wait()
	}
}

func BenchmarkStreamExportTx(b *testing.B) {
	//init  server
	serverOpts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithMaxRecvMsgSize(204939000).
		WithSynced(false).
		WithDir(b.TempDir())

	serverOpts.SessionsOptions.WithMaxSessions(200)

	srv := server.DefaultServer().WithOptions(serverOpts).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	time.Sleep(1 * time.Second)

	// init primary client
	port := srv.Listener.Addr().(*net.TCPAddr).Port

	opts := ic.DefaultOptions().
		WithDir(b.TempDir()).
		WithPort(port)

	client := ic.NewClient().WithOptions(opts)

	err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	if err != nil {
		panic(err)
	}

	// create database as primarydb in primary server
	_, err = client.CreateDatabaseV2(context.Background(), "db1", &schema.DatabaseNullableSettings{
		MaxConcurrency: &schema.NullableUint32{Value: 200},
		VLogCacheSize:  &schema.NullableUint32{Value: 0}, // disable vLogCache
	})
	if err != nil {
		panic(err)
	}

	err = client.CloseSession(context.Background())
	if err != nil {
		panic(err)
	}

	err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "db1")
	if err != nil {
		panic(err)
	}
	defer client.CloseSession(context.Background())

	// commit some transactions
	workers := 10
	txsPerWorker := 100
	entriesPerTx := 100
	keyLen := 40
	valLen := 256

	kvs := make([]*schema.KeyValue, entriesPerTx)

	for i := 0; i < entriesPerTx; i++ {
		kvs[i] = &schema.KeyValue{
			Key:   make([]byte, keyLen),
			Value: make([]byte, valLen),
		}

		binary.BigEndian.PutUint64(kvs[i].Key, uint64(i))
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < txsPerWorker; j++ {
				_, err := client.SetAll(context.Background(), &schema.SetRequest{
					KVs: kvs,
				})
				if err != nil {
					panic(err)
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()

	replicators := 1
	txsPerReplicator := workers * txsPerWorker / replicators

	clientReplicators := make([]ic.ImmuClient, replicators)

	for r := 0; r < replicators; r++ {
		opts := ic.DefaultOptions().
			WithDir(b.TempDir()).
			WithPort(port)

		client := ic.NewClient().WithOptions(opts)

		err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "db1")
		if err != nil {
			panic(err)
		}
		defer client.CloseSession(context.Background())

		clientReplicators[r] = client
	}

	b.ResetTimer()

	// measure exportTx performance
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(replicators)

		for r := 0; r < replicators; r++ {
			go func(r int) {
				defer wg.Done()

				client := clientReplicators[r]

				streamExportTxClient, err := client.StreamExportTx(context.Background())
				if err != nil {
					panic(err)
				}

				streamSrvFactory := stream.NewStreamServiceFactory(opts.StreamChunkSize)
				exportTxStreamReceiver := streamSrvFactory.NewMsgReceiver(streamExportTxClient)

				doneCh := make(chan struct{})
				recvTxCount := 0

				go func() {
					for {
						_, _, err := exportTxStreamReceiver.ReadFully()
						if err != nil {
							if strings.Contains(err.Error(), "EOF") {
								doneCh <- struct{}{}
								return
							}

							panic(err)
						}

						recvTxCount++
					}
				}()

				for tx := 1; tx <= txsPerReplicator; tx++ {
					err = streamExportTxClient.Send(&schema.ExportTxRequest{
						Tx:                 uint64(1 + r*txsPerReplicator + tx),
						AllowPreCommitted:  false,
						SkipIntegrityCheck: true,
					})
					if err != nil {
						panic(err)
					}
				}

				err = streamExportTxClient.CloseSend()
				if err != nil {
					panic(err)
				}

				<-doneCh

				if recvTxCount != txsPerReplicator {
					panic("recvTxCount != txsPerReplicator")
				}
			}(r)
		}

		wg.Wait()
	}
}
