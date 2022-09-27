package replication

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

// We'll be able to store suite-wide
// variables and add methods to this
// test suite struct
type baseReplicationTestSuite struct {
	suite.Suite
	mu sync.Mutex

	// num of followers configured for master server
	numOfFollowers int

	// server settings
	master           *server.ImmuServer
	followers        []*server.ImmuServer
	followersStartCh chan *server.ImmuServer

	// client settings
	masterC    client.ImmuClient
	followersC []client.ImmuClient

	// temp Dir
	dirs []string
}

// this function executes before the test suite begins execution
func (suite *baseReplicationTestSuite) SetupSuite() {
	suite.followersStartCh = make(chan *server.ImmuServer, suite.numOfFollowers)
	suite.followers = make([]*server.ImmuServer, 0, suite.numOfFollowers)
	suite.followersC = make([]client.ImmuClient, 0, suite.numOfFollowers)
	suite.dirs = make([]string, 0)

	//init master server
	masterDir, err := ioutil.TempDir("", "master-data")
	require.NoError(suite.T(), err)
	suite.dirs = append(suite.dirs, masterDir)
	suite.startMaster(masterDir, 0)

	// give a delay for the server to start
	time.Sleep(1 * time.Second)

	// start the followers
	masterPort := suite.master.Listener.Addr().(*net.TCPAddr).Port
	for i := 0; i < suite.numOfFollowers; i++ {
		suite.addFollower()
	}

	go func() {
		for srv := range suite.followersStartCh {
			go func(s *server.ImmuServer) {
				s.Start()
			}(srv)
			// give a delay for the server to start
			time.Sleep(1 * time.Second)
			// create the clients
			suite.addFollowerC(srv, masterPort)
		}
	}()

	// create database as masterdb in master server
	mlr, err := suite.masterC.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(suite.T(), err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	_, err = suite.masterC.CreateDatabaseV2(mctx, "masterdb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			SyncReplication: &schema.NullableBool{Value: true},
			SyncFollowers:   &schema.NullableUint32{Value: uint32(suite.numOfFollowers)},
		},
	})
	require.NoError(suite.T(), err)

	mdb, err := suite.masterC.UseDatabase(mctx, &schema.Database{DatabaseName: "masterdb"})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	mctx = metadata.NewOutgoingContext(context.Background(), mmd)

	err = suite.masterC.CreateUser(mctx, []byte("follower"), []byte("follower1Pwd!"), auth.PermissionAdmin, "masterdb")
	require.NoError(suite.T(), err)

	err = suite.masterC.SetActiveUser(mctx, &schema.SetActiveUserRequest{Active: true, Username: "follower"})
	require.NoError(suite.T(), err)
}

func (suite *baseReplicationTestSuite) addFollower() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	num := len(suite.followers) + 1

	// init followers
	followerDir, err := ioutil.TempDir("", fmt.Sprintf("follower%d-data", num))
	require.NoError(suite.T(), err)
	suite.dirs = append(suite.dirs, followerDir)

	opts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(followerDir)

	follower := server.DefaultServer().WithOptions(opts).(*server.ImmuServer)
	err = follower.Initialize()
	require.NoError(suite.T(), err)
	suite.followers = append(suite.followers, follower)

	suite.followersStartCh <- follower
}

func (suite *baseReplicationTestSuite) addFollowerC(srv *server.ImmuServer, masterPort int) {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	// init client to the follower
	followerPort := srv.Listener.Addr().(*net.TCPAddr).Port
	followerClient, err := client.NewImmuClient(client.DefaultOptions().WithPort(followerPort))
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), followerClient)
	suite.followersC = append(suite.followersC, followerClient)

	// init database on the follower to replicate
	flr, err := followerClient.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(suite.T(), err)

	fmd := metadata.Pairs("authorization", flr.Token)
	fctx := metadata.NewOutgoingContext(context.Background(), fmd)

	_, err = followerClient.CreateDatabaseV2(fctx, "replicadb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: true},
			SyncReplication:  &schema.NullableBool{Value: true},
			MasterDatabase:   &schema.NullableString{Value: "masterdb"},
			MasterAddress:    &schema.NullableString{Value: "127.0.0.1"},
			MasterPort:       &schema.NullableUint32{Value: uint32(masterPort)},
			FollowerUsername: &schema.NullableString{Value: "follower"},
			FollowerPassword: &schema.NullableString{Value: "follower1Pwd"},
		},
	})
	require.NoError(suite.T(), err)
}

func (suite *baseReplicationTestSuite) stopFollower() {
	suite.mu.Lock()
	defer suite.mu.Unlock()

	flen := len(suite.followers) - 1

	// get the last follower client from the array and stop it
	fc := suite.followersC[flen]
	fc.Logout(context.TODO())

	// get the last follower from the array and stop it
	f := suite.followers[flen]
	go func() {
		f.Stop()
	}()

	// remove the followers from the array
	suite.followers = suite.followers[:flen]
	suite.followersC = suite.followersC[:flen]
}

func (suite *baseReplicationTestSuite) startMaster(dir string, port int) {
	opts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(port).
		WithDir(dir)

	masterServer := server.DefaultServer().WithOptions(opts).(*server.ImmuServer)
	err := masterServer.Initialize()
	require.NoError(suite.T(), err)
	suite.master = masterServer

	// start the master server
	go func() {
		err = suite.master.Start()
		require.NoError(suite.T(), err)
	}()

	// init master client
	masterPort := masterServer.Listener.Addr().(*net.TCPAddr).Port
	masterClient, err := client.NewImmuClient(client.DefaultOptions().WithPort(masterPort))
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), masterClient)
	suite.masterC = masterClient
}

func (suite *baseReplicationTestSuite) restartMaster() {
	oldopts := suite.master.Options
	oldMasterPort := suite.master.Listener.Addr().(*net.TCPAddr).Port
	err := suite.master.Stop()
	time.Sleep(2 * time.Second)
	require.NoError(suite.T(), err)

	// restart master
	suite.startMaster(oldopts.Dir, oldMasterPort)

	masterPort := suite.master.Listener.Addr().(*net.TCPAddr).Port
	// refresh followers clients
	for _, f := range suite.followersC {
		flr, err := f.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
		require.NoError(suite.T(), err)

		fmd := metadata.Pairs("authorization", flr.Token)
		fctx := metadata.NewOutgoingContext(context.Background(), fmd)
		err = f.UpdateDatabase(fctx, &schema.DatabaseSettings{
			DatabaseName:     "replicadb",
			Replica:          true,
			MasterDatabase:   "masterdb",
			MasterAddress:    "127.0.0.1",
			MasterPort:       uint32(masterPort),
			FollowerUsername: "follower",
			FollowerPassword: "follower1Pwd!",
		})
		require.NoError(suite.T(), err)
	}
}

// this function executes after all tests executed
func (suite *baseReplicationTestSuite) TearDownSuite() {
	// stop master
	if suite.master != nil {
		suite.master.Stop()
	}

	// stop followers
	for _, srv := range suite.followers {
		go func(s *server.ImmuServer) {
			s.Stop()
		}(srv)
	}

	// clean temp directories
	for _, dir := range suite.dirs {
		os.RemoveAll(dir)
	}
}

type SyncTestSuite struct {
	baseReplicationTestSuite
}

func (suite *SyncTestSuite) TestSyncFromMasterToAllFollowers() {
	mlr, err := suite.masterC.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(suite.T(), err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	mdb, err := suite.masterC.UseDatabase(mctx, &schema.Database{DatabaseName: "masterdb"})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	mctx = metadata.NewOutgoingContext(context.Background(), mmd)

	_, err = suite.masterC.Set(mctx, []byte("key1"), []byte("value1"))
	require.NoError(suite.T(), err)

	_, err = suite.masterC.Set(mctx, []byte("key2"), []byte("value2"))
	require.NoError(suite.T(), err)

	for _, f := range suite.followersC {
		flr, err := f.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
		require.NoError(suite.T(), err)

		fmd := metadata.Pairs("authorization", flr.Token)
		fctx := metadata.NewOutgoingContext(context.Background(), fmd)

		fdb, err := f.UseDatabase(fctx, &schema.Database{DatabaseName: "replicadb"})
		require.NoError(suite.T(), err)
		require.NotNil(suite.T(), fdb)

		fmd = metadata.Pairs("authorization", fdb.Token)
		fctx = metadata.NewOutgoingContext(context.Background(), fmd)

		_, err = f.Get(fctx, []byte("key1"))
		require.NoError(suite.T(), err)

		_, err = f.Get(fctx, []byte("key2"))
		require.NoError(suite.T(), err)
	}
}

func (suite *SyncTestSuite) TestMasterRestart() {
	suite.restartMaster()
	mlr, err := suite.masterC.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(suite.T(), err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	mdb, err := suite.masterC.UseDatabase(mctx, &schema.Database{DatabaseName: "masterdb"})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	mctx = metadata.NewOutgoingContext(context.Background(), mmd)

	_, err = suite.masterC.Set(mctx, []byte("key3"), []byte("value3"))
	require.NoError(suite.T(), err)

	for _, f := range suite.followersC {
		flr, err := f.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
		require.NoError(suite.T(), err)

		fmd := metadata.Pairs("authorization", flr.Token)
		fctx := metadata.NewOutgoingContext(context.Background(), fmd)

		fdb, err := f.UseDatabase(fctx, &schema.Database{DatabaseName: "replicadb"})
		require.NoError(suite.T(), err)
		require.NotNil(suite.T(), fdb)

		fmd = metadata.Pairs("authorization", fdb.Token)
		fctx = metadata.NewOutgoingContext(context.Background(), fmd)

		_, err = f.Get(fctx, []byte("key3"))
		require.NoError(suite.T(), err)
	}
}

type FailSyncTestSuite struct {
	baseReplicationTestSuite
}

func (suite *FailSyncTestSuite) TestSyncFailureWithLessFollowers() {
	time.Sleep(1 * time.Second)
	// stop a follower
	suite.stopFollower()

	mlr, err := suite.masterC.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(suite.T(), err)

	mmd := metadata.Pairs("authorization", mlr.Token)
	mctx := metadata.NewOutgoingContext(context.Background(), mmd)

	mdb, err := suite.masterC.UseDatabase(mctx, &schema.Database{DatabaseName: "masterdb"})
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), mdb)

	mmd = metadata.Pairs("authorization", mdb.Token)
	mctx = metadata.NewOutgoingContext(context.Background(), mmd)

	doneCh := make(chan bool)
	go func() {
		suite.masterC.Set(mctx, []byte("key5"), []byte("value5"))
		doneCh <- true
	}()

	select {
	case <-doneCh:
		suite.T().Error("should not receive a successful set from client")
	case <-time.After(1 * time.Second):
	}
}

// We need this function to kick off the test suite, otherwise
// "go test" won't know about our tests
func TestSynchronousReplicationTestSuite(t *testing.T) {
	successScenarios := &SyncTestSuite{}
	successScenarios.numOfFollowers = 2

	failureScenarios := &FailSyncTestSuite{}
	failureScenarios.numOfFollowers = 1

	suite.Run(t, successScenarios)
	suite.Run(t, failureScenarios)
}
