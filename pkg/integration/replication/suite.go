package replication

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
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

	// shutdown the server
	Shutdown(t *testing.T)

	// start previously shut down server
	Start(t *testing.T)
}

// TestServerProvider is a provider of server instances
type TestServerProvider interface {
	AddServer(t *testing.T) TestServer
}

// inProcessTestServerProvider creates in-memory test servers
// those servers are using a temporary directory that's cleaned up after the test is finished
type inProcessTestServerProvider struct {
}

func (p *inProcessTestServerProvider) AddServer(t *testing.T) TestServer {
	ret := &inProcessTestServer{
		dir: t.TempDir(), // go test will clean this up
	}

	ret.Start(t)
	return ret
}

// inProcessTestServer represents an in-process test server
type inProcessTestServer struct {
	srv  *server.ImmuServer
	dir  string
	port int
}

func (s *inProcessTestServer) Address(t *testing.T) (string, int) {
	return "localhost", s.port
}

func (s *inProcessTestServer) Shutdown(t *testing.T) {
	require.NotNil(t, s.srv)
	s.srv.Stop()
	s.srv = nil
}

func (s *inProcessTestServer) Start(t *testing.T) {
	require.Nil(t, s.srv)

	opts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(s.port).
		WithDir(s.dir)

	srv := server.DefaultServer().WithOptions(opts).(*server.ImmuServer)
	err := srv.Initialize()
	require.NoError(t, err)

	go func() {
		err := srv.Start()
		require.NoError(t, err)
	}()

	// Wait for the server to initialize
	// TODO: Active notification that the server has started
	time.Sleep(time.Second)

	if s.port == 0 {
		// Save the port for reopening with the same value
		s.port = srv.Listener.Addr().(*net.TCPAddr).Port
	}

	s.srv = srv
}

type baseReplicationTestSuite struct {
	srvProvider TestServerProvider

	suite.Suite
	mu sync.Mutex

	// server settings
	master    TestServer
	followers []TestServer
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

	fctx, followerClient, cleanup := suite.internalClientFor(follower, client.DefaultDB)
	defer cleanup()

	masterHost, masterPort := suite.master.Address(suite.T())

	settings := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: true},
			SyncReplication:  &schema.NullableBool{Value: true},
			MasterDatabase:   &schema.NullableString{Value: masterDBName},
			MasterAddress:    &schema.NullableString{Value: masterHost},
			MasterPort:       &schema.NullableUint32{Value: uint32(masterPort)},
			FollowerUsername: &schema.NullableString{Value: replicaUsername},
			FollowerPassword: &schema.NullableString{Value: replicaPassword},
		},
	}

	// init database on the follower to replicate
	_, err := followerClient.CreateDatabaseV2(fctx, "replicadb", settings)
	require.NoError(suite.T(), err)

	return followerNum
}

func (suite *baseReplicationTestSuite) StopFollower(followerNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.followers[followerNum]
	f.Shutdown(suite.T())
}

func (suite *baseReplicationTestSuite) StartFollower(followerNum int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	f := suite.followers[followerNum]
	f.Start(suite.T())
}

func (suite *baseReplicationTestSuite) StartMaster(syncFollowers int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.Nil(suite.T(), suite.master)

	srv := suite.srvProvider.AddServer(suite.T())

	suite.master = srv

	mctx, client, cleanup := suite.internalClientFor(srv, client.DefaultDB)
	defer cleanup()

	settings := &schema.DatabaseNullableSettings{}

	if syncFollowers > 0 {
		settings.ReplicationSettings = &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncFollowers:   &schema.NullableUint32{Value: uint32(syncFollowers)},
		}
	}

	_, err := client.CreateDatabaseV2(mctx, masterDBName, settings)
	require.NoError(suite.T(), err)

	mdb, err := client.UseDatabase(mctx, &schema.Database{DatabaseName: masterDBName})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	err = client.CreateUser(mctx, []byte(replicaUsername), []byte(replicaPassword), auth.PermissionAdmin, masterDBName)
	require.NoError(suite.T(), err)

	err = client.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: replicaUsername})
	require.NoError(suite.T(), err)
}

func (suite *baseReplicationTestSuite) RestartMaster() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	require.NotNil(suite.T(), suite.master)

	suite.master.Shutdown(suite.T())
	suite.master.Start(suite.T())
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

func (suite *baseReplicationTestSuite) ClientForMaser() (mctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.master, masterDBName)
}

func (suite *baseReplicationTestSuite) ClientForReplica(replicaNum int) (fctx context.Context, client client.ImmuClient, cleanup func()) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	return suite.internalClientFor(suite.followers[replicaNum], replicaDBName)
}

func (suite *baseReplicationTestSuite) SetupCluster(replicas int, syncFollowers int) {
	suite.StartMaster(syncFollowers)
	for i := 0; i < replicas; i++ {
		suite.AddFollower(syncFollowers > 0)
	}
}

// SetupSuite initializes the suite
func (suite *baseReplicationTestSuite) SetupSuite() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	if suite.srvProvider == nil {
		suite.srvProvider = &inProcessTestServerProvider{}
	}
}

// this function executes after all tests executed
func (suite *baseReplicationTestSuite) TearDownSuite() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// stop followers
	for _, srv := range suite.followers {
		srv.Shutdown(suite.T())
	}

	// stop master
	if suite.master != nil {
		suite.master.Shutdown(suite.T())
	}
}
