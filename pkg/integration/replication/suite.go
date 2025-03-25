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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	primaryDBName   = "primarydb"
	replicaDBName   = "replicadb"
	primaryUsername = "replicator"
	primaryPassword = "replicator1Pwd!"
)

// TestServer is an abstract representation of a TestServer
type TestServer interface {
	// Get the host and port under which the server can be accessed
	Address(t *testing.T) (host string, port int)

	UUID(t *testing.T) xid.ID

	// shutdown the server
	Shutdown(t *testing.T)

	// start previously shut down server
	Start(t *testing.T)
}

// TestServerProvider is a provider of server instances
type TestServerProvider interface {
	AddServer(t *testing.T) TestServer
}

type baseReplicationTestSuite struct {
	srvProvider TestServerProvider

	suite.Suite
	mu sync.Mutex

	// server settings
	primary        TestServer
	primaryDBName  string
	primaryRunning bool

	replicas        []TestServer
	replicasDBName  []string
	replicasRunning []bool

	clientStateDir string
}

func (suite *baseReplicationTestSuite) GetReplicasCount() int {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return len(suite.replicas)
}

func (suite *baseReplicationTestSuite) AddReplica(sync bool) int {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	replica := suite.srvProvider.AddServer(suite.T())

	replicaNum := len(suite.replicas)
	suite.replicas = append(suite.replicas, replica)
	suite.replicasDBName = append(suite.replicasDBName, replicaDBName)
	suite.replicasRunning = append(suite.replicasRunning, true)

	rctx, replicaClient, cleanup := suite.internalClientFor(replica, client.DefaultDB)
	defer cleanup()

	primaryHost, primaryPort := suite.primary.Address(suite.T())

	settings := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: true},
			SyncReplication: &schema.NullableBool{Value: sync},
			PrimaryDatabase: &schema.NullableString{Value: suite.primaryDBName},
			PrimaryHost:     &schema.NullableString{Value: primaryHost},
			PrimaryPort:     &schema.NullableUint32{Value: uint32(primaryPort)},
			PrimaryUsername: &schema.NullableString{Value: primaryUsername},
			PrimaryPassword: &schema.NullableString{Value: primaryPassword},
		},
	}

	// init database on the replica to replicate
	_, err := replicaClient.CreateDatabaseV2(rctx, replicaDBName, settings)
	require.NoError(suite.T(), err)

	return replicaNum
}

func (suite *baseReplicationTestSuite) StopReplica(replicaNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.replicas[replicaNum]
	f.Shutdown(suite.T())
	suite.replicasRunning[replicaNum] = false
}

func (suite *baseReplicationTestSuite) StartReplica(replicaNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.replicas[replicaNum]
	f.Start(suite.T())
	suite.replicasRunning[replicaNum] = true
}

func (suite *baseReplicationTestSuite) PromoteReplica(replicaNum, syncAcks int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// set replica as new primary and current primary as replica
	suite.primary, suite.replicas[replicaNum] = suite.replicas[replicaNum], suite.primary
	suite.primaryDBName, suite.replicasDBName[replicaNum] = suite.replicasDBName[replicaNum], suite.primaryDBName

	mctx, mClient, cleanup := suite.internalClientFor(suite.primary, suite.primaryDBName)
	defer cleanup()

	_, err := mClient.UpdateDatabaseV2(mctx, suite.primaryDBName, &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: false},
			SyncReplication: &schema.NullableBool{Value: syncAcks > 0},
			SyncAcks:        &schema.NullableUint32{Value: uint32(syncAcks)},
		},
	})
	require.NoError(suite.T(), err)

	mdb, err := mClient.UseDatabase(mctx, &schema.Database{DatabaseName: suite.primaryDBName})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	err = mClient.CreateUser(mctx, []byte(primaryUsername), []byte(primaryPassword), auth.PermissionAdmin, suite.primaryDBName)
	require.NoError(suite.T(), err)

	err = mClient.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: primaryUsername})
	require.NoError(suite.T(), err)

	host, port := suite.primary.Address(suite.T())

	for i, _ := range suite.replicas {
		ctx, client, cleanup := suite.internalClientFor(suite.replicas[i], suite.replicasDBName[i])
		defer cleanup()

		_, err = client.UpdateDatabaseV2(ctx, suite.replicasDBName[i], &schema.DatabaseNullableSettings{
			ReplicationSettings: &schema.ReplicationNullableSettings{
				Replica:         &schema.NullableBool{Value: true},
				PrimaryHost:     &schema.NullableString{Value: host},
				PrimaryPort:     &schema.NullableUint32{Value: uint32(port)},
				PrimaryDatabase: &schema.NullableString{Value: suite.primaryDBName},
				PrimaryUsername: &schema.NullableString{Value: primaryUsername},
				PrimaryPassword: &schema.NullableString{Value: primaryPassword},
			},
		})
		require.NoError(suite.T(), err)
	}
}

func (suite *baseReplicationTestSuite) StartPrimary(syncAcks int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.Nil(suite.T(), suite.primary)

	srv := suite.srvProvider.AddServer(suite.T())

	suite.primary = srv

	mctx, client, cleanup := suite.internalClientFor(srv, client.DefaultDB)
	defer cleanup()

	settings := &schema.DatabaseNullableSettings{}

	if syncAcks > 0 {
		settings.ReplicationSettings = &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: uint32(syncAcks)},
		}
	}

	_, err := client.CreateDatabaseV2(mctx, suite.primaryDBName, settings)
	require.NoError(suite.T(), err)

	mdb, err := client.UseDatabase(mctx, &schema.Database{DatabaseName: suite.primaryDBName})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	err = client.CreateUser(mctx, []byte(primaryUsername), []byte(primaryPassword), auth.PermissionAdmin, suite.primaryDBName)
	require.NoError(suite.T(), err)

	err = client.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: primaryUsername})
	require.NoError(suite.T(), err)

	suite.primaryRunning = true
}

func (suite *baseReplicationTestSuite) StopPrimary() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.NotNil(suite.T(), suite.primary)

	suite.primary.Shutdown(suite.T())
	suite.primaryRunning = false
}

func (suite *baseReplicationTestSuite) RestartPrimary() {
	suite.StopPrimary()

	suite.mu.Lock()
	defer suite.mu.Unlock()

	suite.primary.Start(suite.T())
	suite.primaryRunning = true
}

func (suite *baseReplicationTestSuite) internalClientFor(srv TestServer, dbName string) (context.Context, client.ImmuClient, func()) {
	host, port := srv.Address(suite.T())

	opts := client.
		DefaultOptions().
		WithDir(suite.clientStateDir).
		WithAddress(host).
		WithPort(port)

	c := client.NewClient().WithOptions(opts)

	err := c.OpenSession(
		context.Background(),
		[]byte(`immudb`),
		[]byte(`immudb`),
		dbName,
	)
	require.NoError(suite.T(), err)

	return context.Background(), c, func() { c.CloseSession(context.Background()) }
}

func (suite *baseReplicationTestSuite) ClientForPrimary() (mctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.primary, suite.primaryDBName)
}

func (suite *baseReplicationTestSuite) ClientForReplica(replicaNum int) (rctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.replicas[replicaNum], suite.replicasDBName[replicaNum])
}

func (suite *baseReplicationTestSuite) WaitForCommittedTx(
	ctx context.Context,
	client client.ImmuClient,
	txID uint64,
	timeout time.Duration,
) {
	var state *schema.ImmutableState
	var err error
	if !assert.Eventually(suite.T(), func() bool {
		state, err = client.CurrentState(ctx)
		require.NoError(suite.T(), err)

		return state.TxId >= txID

	}, timeout, time.Millisecond*10) {

		require.FailNowf(suite.T(),
			"Failed to get up to transaction",
			"Failed to get up to transaction %d, precommitted tx: %d, committed tx: %d",
			txID, state.PrecommittedTxId, state.TxId,
		)
	}
}

func (suite *baseReplicationTestSuite) SetupCluster(syncReplicas, syncAcks, asyncReplicas int) {
	suite.primaryDBName = primaryDBName

	suite.StartPrimary(syncAcks)

	wg := sync.WaitGroup{}

	for i := 0; i < syncReplicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.AddReplica(true)
		}()
	}

	for i := 0; i < asyncReplicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.AddReplica(false)
		}()
	}

	wg.Wait()
}

func (suite *baseReplicationTestSuite) ValidateClusterSetup() {
	uuids := make(map[string]struct{}, 1+suite.GetReplicasCount())

	uuids[suite.primary.UUID(suite.T()).String()] = struct{}{}

	for _, f := range suite.replicas {
		uuid := f.UUID(suite.T()).String()

		if _, ok := uuids[uuid]; ok {
			require.FailNowf(suite.T(), "duplicated uuid", "duplicated uuid '%s'", uuid)
		}

		uuids[uuid] = struct{}{}
	}
}

// SetupTest initializes the suite
func (suite *baseReplicationTestSuite) SetupTest() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	suite.clientStateDir = suite.T().TempDir()

	if suite.srvProvider == nil {
		suite.srvProvider = &inProcessTestServerProvider{}
	}
}

// this function executes after all tests executed
func (suite *baseReplicationTestSuite) TearDownTest() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// stop replicas
	for i, srv := range suite.replicas {
		if suite.replicasRunning[i] {
			srv.Shutdown(suite.T())
		}
	}
	suite.replicas = []TestServer{}

	// stop primary
	if suite.primary != nil {
		suite.primary.Shutdown(suite.T())
		suite.primary = nil
	}
	suite.primary = nil
}
