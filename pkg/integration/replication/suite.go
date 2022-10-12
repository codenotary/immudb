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
	masterDBName    = "masterdb"
	replicaDBName   = "replicadb"
	replicaUsername = "follower"
	replicaPassword = "follower1Pwd!"
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
	master        TestServer
	masterDBName  string
	masterRunning bool

	followers        []TestServer
	followersDBName  []string
	followersRunning []bool
}

func (suite *baseReplicationTestSuite) GetFollowersCount() int {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return len(suite.followers)
}

func (suite *baseReplicationTestSuite) AddFollower(sync bool) int {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	follower := suite.srvProvider.AddServer(suite.T())

	followerNum := len(suite.followers)
	suite.followers = append(suite.followers, follower)
	suite.followersDBName = append(suite.followersDBName, replicaDBName)
	suite.followersRunning = append(suite.followersRunning, true)

	fctx, followerClient, cleanup := suite.internalClientFor(follower, client.DefaultDB)
	defer cleanup()

	masterHost, masterPort := suite.master.Address(suite.T())

	settings := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: true},
			SyncReplication:  &schema.NullableBool{Value: sync},
			MasterDatabase:   &schema.NullableString{Value: suite.masterDBName},
			MasterAddress:    &schema.NullableString{Value: masterHost},
			MasterPort:       &schema.NullableUint32{Value: uint32(masterPort)},
			FollowerUsername: &schema.NullableString{Value: replicaUsername},
			FollowerPassword: &schema.NullableString{Value: replicaPassword},
		},
	}

	// init database on the follower to replicate
	_, err := followerClient.CreateDatabaseV2(fctx, replicaDBName, settings)
	require.NoError(suite.T(), err)

	return followerNum
}

func (suite *baseReplicationTestSuite) StopFollower(followerNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.followers[followerNum]
	f.Shutdown(suite.T())
	suite.followersRunning[followerNum] = false
}

func (suite *baseReplicationTestSuite) StartFollower(followerNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.followers[followerNum]
	f.Start(suite.T())
	suite.followersRunning[followerNum] = true
}

func (suite *baseReplicationTestSuite) PromoteFollower(followerNum, syncAcks int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// set follower as new master and current master as follower
	suite.master, suite.followers[followerNum] = suite.followers[followerNum], suite.master
	suite.masterDBName, suite.followersDBName[followerNum] = suite.followersDBName[followerNum], suite.masterDBName

	mctx, mClient, cleanup := suite.internalClientFor(suite.master, suite.masterDBName)
	defer cleanup()

	_, err := mClient.UpdateDatabaseV2(mctx, suite.masterDBName, &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:         &schema.NullableBool{Value: false},
			SyncReplication: &schema.NullableBool{Value: syncAcks > 0},
			SyncAcks:        &schema.NullableUint32{Value: uint32(syncAcks)},
		},
	})
	require.NoError(suite.T(), err)

	mdb, err := mClient.UseDatabase(mctx, &schema.Database{DatabaseName: suite.masterDBName})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	err = mClient.CreateUser(mctx, []byte(replicaUsername), []byte(replicaPassword), auth.PermissionAdmin, suite.masterDBName)
	require.NoError(suite.T(), err)

	err = mClient.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: replicaUsername})
	require.NoError(suite.T(), err)

	host, port := suite.master.Address(suite.T())

	for i, _ := range suite.followers {
		ctx, client, cleanup := suite.internalClientFor(suite.followers[i], suite.followersDBName[i])
		defer cleanup()

		_, err = client.UpdateDatabaseV2(ctx, suite.followersDBName[i], &schema.DatabaseNullableSettings{
			ReplicationSettings: &schema.ReplicationNullableSettings{
				Replica:          &schema.NullableBool{Value: true},
				MasterAddress:    &schema.NullableString{Value: host},
				MasterPort:       &schema.NullableUint32{Value: uint32(port)},
				MasterDatabase:   &schema.NullableString{Value: suite.masterDBName},
				FollowerUsername: &schema.NullableString{Value: replicaUsername},
				FollowerPassword: &schema.NullableString{Value: replicaPassword},
			},
		})
		require.NoError(suite.T(), err)
	}
}

func (suite *baseReplicationTestSuite) StartMaster(syncAcks int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.Nil(suite.T(), suite.master)

	srv := suite.srvProvider.AddServer(suite.T())

	suite.master = srv

	mctx, client, cleanup := suite.internalClientFor(srv, client.DefaultDB)
	defer cleanup()

	settings := &schema.DatabaseNullableSettings{}

	if syncAcks > 0 {
		settings.ReplicationSettings = &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncAcks:        &schema.NullableUint32{Value: uint32(syncAcks)},
		}
	}

	_, err := client.CreateDatabaseV2(mctx, suite.masterDBName, settings)
	require.NoError(suite.T(), err)

	mdb, err := client.UseDatabase(mctx, &schema.Database{DatabaseName: suite.masterDBName})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	err = client.CreateUser(mctx, []byte(replicaUsername), []byte(replicaPassword), auth.PermissionAdmin, suite.masterDBName)
	require.NoError(suite.T(), err)

	err = client.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: replicaUsername})
	require.NoError(suite.T(), err)

	suite.masterRunning = true
}

func (suite *baseReplicationTestSuite) StopMaster() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.NotNil(suite.T(), suite.master)

	suite.master.Shutdown(suite.T())
	suite.masterRunning = false
}

func (suite *baseReplicationTestSuite) RestartMaster() {
	suite.StopMaster()

	suite.mu.Lock()
	defer suite.mu.Unlock()

	suite.master.Start(suite.T())
	suite.masterRunning = true
}

func (suite *baseReplicationTestSuite) internalClientFor(srv TestServer, dbName string) (context.Context, client.ImmuClient, func()) {
	host, port := srv.Address(suite.T())

	opts := client.
		DefaultOptions().
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

func (suite *baseReplicationTestSuite) ClientForMaster() (mctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.master, suite.masterDBName)
}

func (suite *baseReplicationTestSuite) ClientForReplica(replicaNum int) (fctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.followers[replicaNum], suite.followersDBName[replicaNum])
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
	suite.masterDBName = masterDBName

	suite.StartMaster(syncAcks)

	wg := sync.WaitGroup{}

	for i := 0; i < syncReplicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.AddFollower(true)
		}()
	}

	for i := 0; i < asyncReplicas; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.AddFollower(false)
		}()
	}

	wg.Wait()
}

func (suite *baseReplicationTestSuite) ValidateClusterSetup() {
	uuids := make(map[string]struct{}, 1+suite.GetFollowersCount())

	uuids[suite.master.UUID(suite.T()).String()] = struct{}{}

	for _, f := range suite.followers {
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

	if suite.srvProvider == nil {
		suite.srvProvider = &inProcessTestServerProvider{}
	}
}

// this function executes after all tests executed
func (suite *baseReplicationTestSuite) TearDownTest() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// stop followers
	for i, srv := range suite.followers {
		if suite.followersRunning[i] {
			srv.Shutdown(suite.T())
		}
	}
	suite.followers = []TestServer{}

	// stop master
	if suite.master != nil {
		suite.master.Shutdown(suite.T())
		suite.master = nil
	}
	suite.master = nil
}
