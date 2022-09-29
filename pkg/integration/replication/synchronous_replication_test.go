package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
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
	suite.SetupCluster(2, 2)
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

			// Tests are flaky because it takes time to commit the
			// precommitted TX, so this function just ensures the state
			// is in sync between master and follower
			suite.WaitForCommittedTx(ctx, client, tx2.Id, time.Second)

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
	var txBeforeRestart *schema.TxHeader
	suite.Run("commit before restarting primary", func() {

		ctx, client, cleanup := suite.ClientForMaser()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-before-restart"), []byte("value-before-restart"))
		require.NoError(suite.T(), err)

		txBeforeRestart = tx
	})

	suite.RestartMaster()

	suite.Run("commit after restarting master", func() {
		ctx, client, cleanup := suite.ClientForMaser()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key3"), []byte("value3"))
		require.NoError(suite.T(), err)

		for i := 0; i < suite.GetFollowersCount(); i++ {
			suite.Run(fmt.Sprintf("check follower %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				// Tests are flaky because it takes time to commit the
				// precommitted TX, so this function just ensures the state
				// is in sync between master and follower
				suite.WaitForCommittedTx(ctx, client, tx.Id, 30*time.Second) // Longer time since replica must reestablish connection to the primary

				val, err := client.GetAt(ctx, []byte("key3"), tx.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value3"), val.Value)

				val, err = client.GetAt(ctx, []byte("key-before-restart"), txBeforeRestart.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value-before-restart"), val.Value)
			})
		}
	})
}

// TestPrecommitStateSync checks if the precommit state at master
// and its followers are in sync during synchronous replication
func (suite *SyncTestSuite) TestPrecommitStateSync() {
	var (
		masterState *schema.ImmutableState
		err         error
		startCh     = make(chan bool)
	)

	ctx, client, cleanup := suite.ClientForMaser()
	defer cleanup()

	// Create goroutines for client waiting to query the state
	// of the followers. This is initialised before to avoid
	// spending time initialising the follower client for faster
	// state access
	var wg sync.WaitGroup
	for i := 0; i < suite.GetFollowersCount(); i++ {
		wg.Add(1)
		go func(followerID int) {
			defer wg.Done()
			ctx, client, cleanup := suite.ClientForReplica(followerID)
			defer cleanup()
			for {
				select {
				case <-startCh:
					suite.Run(fmt.Sprintf("test replica sync state %d", followerID), func() {
						state, err := client.CurrentState(ctx)
						require.NoError(suite.T(), err)
						suite.Require().Equal(state.PrecommittedTxId, masterState.TxId)
						suite.Require().Equal(state.PrecommittedTxHash, masterState.TxHash)
					})
					return
				}
			}
		}(i)
	}

	// add multiple keys to make update the master's state quickly
	for i := 10; i < 30; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		_, err = client.Set(ctx, []byte(key), []byte(value))
		require.NoError(suite.T(), err)
	}

	// get the current precommit txn id state of master
	masterState, err = client.CurrentState(ctx)
	require.NoError(suite.T(), err)

	// close will unblock all goroutines
	close(startCh)
	wg.Wait()
}

type SyncTestMinimumFollowersSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestMinimumFollowersSuite(t *testing.T) {
	suite.Run(t, &SyncTestMinimumFollowersSuite{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestMinimumFollowersSuite) SetupSuite() {
	suite.baseReplicationTestSuite.SetupSuite()
	suite.SetupCluster(4, 2)
}

// TestMinimumFollowers ensures the primary can operate as long as the minimum
// number of replicas send their confirmations
func (suite *SyncTestMinimumFollowersSuite) TestMinimumFollowers() {

	ctx, client, cleanup := suite.ClientForMaser()
	defer cleanup()

	suite.Run("should commit successfully without one replica", func() {
		suite.StopFollower(0)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key1"), []byte("value1"))
		require.NoError(suite.T(), err)
	})

	suite.Run("should commit successfully without two replicas", func() {
		suite.StopFollower(1)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key2"), []byte("value2"))
		require.NoError(suite.T(), err)
	})

	suite.Run("should not commit without three replicas", func() {
		suite.StopFollower(2)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key3"), []byte("value3"))
		require.Error(suite.T(), err)
		require.Contains(suite.T(), err.Error(), "deadline")
	})

	suite.Run("should commit again once first replica is back online", func() {
		suite.StartFollower(0)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key4"), []byte("value4"))
		require.NoError(suite.T(), err)
	})
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
}
