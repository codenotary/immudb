package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SyncTestSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestSuite(t *testing.T) {
	suite.Run(t, &SyncTestSuite{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestSuite) SetupSuite() {
	suite.baseReplicationTestSuite.SetupSuite()
	suite.SetupCluster(3, 3)
}

func (suite *SyncTestSuite) TestSyncFromMasterToAllFollowers() {
	ctx, client, cleanup := suite.ClientForMaser()
	defer cleanup()

	tx1, err := client.Set(ctx, []byte("key1"), []byte("value1"))
	require.NoError(suite.T(), err)

	tx2, err := client.Set(ctx, []byte("key2"), []byte("value2"))
	require.NoError(suite.T(), err)

	for i := 0; i < suite.GetFollowersCount(); i++ {
		suite.Run(fmt.Sprintf("test replica %d", i), func() {
			ctx, client, cleanup := suite.ClientForReplica(i)
			defer cleanup()

			val, err := client.GetAt(ctx, []byte("key1"), tx1.Id)
			require.NoError(suite.T(), err)
			suite.Require().Equal([]byte("value1"), val.Value)

			val, err = client.GetAt(ctx, []byte("key2"), tx2.Id)
			require.NoError(suite.T(), err)
			suite.Require().Equal([]byte("value2"), val.Value)
		})
	}
}

func (suite *SyncTestSuite) TestMasterRestart() {
	suite.RestartMaster()

	ctx, client, cleanup := suite.ClientForMaser()
	defer cleanup()

	tx, err := client.Set(ctx, []byte("key3"), []byte("value3"))
	require.NoError(suite.T(), err)

	time.Sleep(time.Second)

	for i := 0; i < suite.GetFollowersCount(); i++ {
		ctx, client, cleanup := suite.ClientForReplica(i)
		defer cleanup()

		val, err := client.GetAt(ctx, []byte("key3"), tx.Id)
		require.NoError(suite.T(), err)
		require.Equal(suite.T(), []byte("value3"), val.Value)
	}
}

type FailSyncTestSuite struct {
	baseReplicationTestSuite
}

func TestFailSyncTestSuite(t *testing.T) {
	suite.Run(t, &FailSyncTestSuite{})
}

func (suite *FailSyncTestSuite) SetupSuite() {
	suite.baseReplicationTestSuite.SetupSuite()
	suite.SetupCluster(1, 1)
}

func (suite *FailSyncTestSuite) TestSyncFailureWithLessFollowers() {
	ctx, client, cleanup := suite.ClientForMaser()
	defer cleanup()

	_, err := client.Set(
		ctx,
		[]byte("key5"), []byte("value5"),
	)
	require.NoError(suite.T(), err)

	suite.StopFollower(0)

	const timeout = time.Second

	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	t1 := time.Now()
	_, err = client.Set(
		ctxWithTimeout,
		[]byte("key5"), []byte("value5"),
	)

	assert.Greater(suite.T(), time.Since(t1), timeout)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "context deadline exceeded")

	// TODO: This is a workaround for the server to shutdown cleanly, remove it when no longer needed
	suite.StartFollower(0)
}
