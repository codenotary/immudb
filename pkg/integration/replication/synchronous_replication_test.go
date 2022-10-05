package replication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
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
func (suite *SyncTestSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 2, 0)
}

func (suite *SyncTestSuite) TestSyncFromMasterToAllFollowers() {
	ctx, client, cleanup := suite.ClientForMaster()
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
			suite.WaitForCommittedTx(ctx, client, tx2.Id, time.Duration(3)*time.Second)

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

		ctx, client, cleanup := suite.ClientForMaster()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-before-restart"), []byte("value-before-restart"))
		require.NoError(suite.T(), err)

		txBeforeRestart = tx
	})

	suite.RestartMaster()

	suite.Run("commit after restarting master", func() {
		ctx, client, cleanup := suite.ClientForMaster()
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
			for range startCh {
				suite.Run(fmt.Sprintf("test replica sync state %d", followerID), func() {
					state, err := client.CurrentState(ctx)
					require.NoError(suite.T(), err)
					suite.Require().Equal(state.PrecommittedTxId, masterState.TxId)
					suite.Require().Equal(state.PrecommittedTxHash, masterState.TxHash)
				})
				return
			}
		}(i)
	}

	ctx, client, cleanup := suite.ClientForMaster()
	defer cleanup()

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
func (suite *SyncTestMinimumFollowersSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(4, 2, 0)
}

// TestMinimumFollowers ensures the primary can operate as long as the minimum
// number of replicas send their confirmations
func (suite *SyncTestMinimumFollowersSuite) TestMinimumFollowers() {

	ctx, client, cleanup := suite.ClientForMaster()
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

	suite.Run("should recover with all replicas replaced", func() {
		suite.StopFollower(0)
		suite.StopFollower(3)

		suite.AddFollower(true)
		suite.AddFollower(true)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key5"), []byte("value5"))
		require.NoError(suite.T(), err)
	})

	suite.Run("ensure correct data is in the database after all changes", func() {

		primaryState, err := client.CurrentState(ctx)
		require.NoError(suite.T(), err)

		for i := 4; i < 6; i++ {
			suite.Run(fmt.Sprintf("replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				suite.WaitForCommittedTx(ctx, client, primaryState.TxId, time.Second)

				for i := 1; i <= 5; i++ {
					val, err := client.Get(ctx, []byte(fmt.Sprintf("key%d", i)))
					require.NoError(suite.T(), err)
					require.Equal(suite.T(), []byte(fmt.Sprintf("value%d", i)), val.Value)
				}
			})
		}
	})
}

type SyncTestRecoverySpeedSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestRecoverySpeedSuite(t *testing.T) {
	suite.Run(t, &SyncTestRecoverySpeedSuite{})
}

func (suite *SyncTestRecoverySpeedSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 1, 0)
}

func (suite *SyncTestRecoverySpeedSuite) TestReplicaRecoverySpeed() {

	const parallelWriters = 30
	const samplingTime = time.Second * 5

	// Stop the follower, we don't replicate any transactions to it now
	// but we can still commit using the second replica
	suite.StopFollower(0)

	var txWritten uint64

	suite.Run("Write transactions for 5 seconds at maximum speed", func() {
		start := make(chan bool)
		stop := make(chan bool)
		wgStart := sync.WaitGroup{}
		wgFinish := sync.WaitGroup{}

		// Run multiple clients in parallel - let's try to hammer the DB as much as possible
		for i := 0; i < parallelWriters; i++ {
			wgStart.Add(1)
			wgFinish.Add(1)
			go func(i int) {
				defer wgFinish.Done()

				ctx, client, cleanup := suite.ClientForMaster()
				defer cleanup()

				// Wait for the start signal
				wgStart.Done()
				<-start

				for j := 0; ; j++ {

					select {
					case <-stop:
						atomic.AddUint64(&txWritten, uint64(j))
						return
					default:
					}

					_, err := client.Set(ctx,
						[]byte(fmt.Sprintf("client-%d-%d", i, j)),
						[]byte(fmt.Sprintf("value-%d-%d", i, j)),
					)
					suite.Require().NoError(err)
				}
			}(i)
		}

		// Ready, steady...
		wgStart.Wait()

		// Go...
		close(start)
		time.Sleep(samplingTime)
		close(stop)

		wgFinish.Wait()

		fmt.Println("Total TX written:", txWritten)
	})

	var tx *schema.TxHeader

	suite.Run("Ensure replica can recover in reasonable amount of time", func() {

		// Stop the second follower, now the DB is locked
		suite.StopFollower(1)

		ctx, client, cleanup := suite.ClientForMaster()
		defer cleanup()

		state, err := client.CurrentState(ctx)
		suite.Require().NoError(err)
		suite.Require().Greater(state.TxId, txWritten, "Ensure enough TXs were written")

		// Check if we can recover the cluster and perform write within the same amount of time
		// that was needed for initial sampling. The replica that was initially stopped and now
		// started has the same amount of transaction to grab from master as the other one
		// which should take the same amount of time as the initial write period or less
		// (since the primary is not persisting data this time).
		ctxTimeout, cancel := context.WithTimeout(ctx, samplingTime)
		defer cancel()

		suite.StartFollower(0) // 1 down

		tx, err = client.Set(ctxTimeout, []byte("key-after-recovery"), []byte("value-after-recovery"))
		suite.NoError(err)
	})

	suite.Run("Ensure the data is readable from replicas", func() {
		suite.StartFollower(1)

		suite.Run("primary", func() {
			ctx, client, cleanup := suite.ClientForMaster()
			defer cleanup()

			val, err := client.GetAt(ctx, []byte("key-after-recovery"), tx.Id)
			suite.NoError(err)
			suite.Equal([]byte("value-after-recovery"), val.Value)
		})

		for i := 0; i < suite.GetFollowersCount(); i++ {
			suite.Run(fmt.Sprintf("replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				suite.WaitForCommittedTx(ctx, client, tx.Id, 5*time.Second)

				val, err := client.GetAt(ctx, []byte("key-after-recovery"), tx.Id)
				suite.NoError(err)
				suite.Equal([]byte("value-after-recovery"), val.Value)
			})
		}
	})

}

type SyncTestWithAsyncFollowersSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestWithAsyncFollowersSuite(t *testing.T) {
	suite.Run(t, &SyncTestWithAsyncFollowersSuite{})
}

func (suite *SyncTestWithAsyncFollowersSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 1, 1)
}

func (suite *SyncTestWithAsyncFollowersSuite) TestSyncReplicationAlongWithAsyncFollowers() {
	const parallelWriters = 30
	const samplingTime = time.Second * 5

	var txWritten uint64

	suite.Run("Write transactions for 5 seconds at maximum speed", func() {
		start := make(chan bool)
		stop := make(chan bool)
		wgStart := sync.WaitGroup{}
		wgFinish := sync.WaitGroup{}

		// Run multiple clients in parallel - let's try to hammer the DB as much as possible
		for i := 0; i < parallelWriters; i++ {
			wgStart.Add(1)
			wgFinish.Add(1)
			go func(i int) {
				defer wgFinish.Done()

				ctx, client, cleanup := suite.ClientForMaster()
				defer cleanup()

				// Wait for the start signal
				wgStart.Done()
				<-start

				for j := 0; ; j++ {
					select {
					case <-stop:
						atomic.AddUint64(&txWritten, uint64(j))
						return
					default:
					}

					_, err := client.Set(ctx,
						[]byte(fmt.Sprintf("client-%d-%d", i, j)),
						[]byte(fmt.Sprintf("value-%d-%d", i, j)),
					)
					suite.Require().NoError(err)
				}
			}(i)
		}

		// Ready, steady...
		wgStart.Wait()

		// Go...
		close(start)
		time.Sleep(samplingTime)
		close(stop)

		wgFinish.Wait()

		fmt.Println("Total TX written:", txWritten)
	})

	suite.Run("Ensure the data is available in all the replicas", func() {
		ctx, client, cleanup := suite.ClientForMaster()
		defer cleanup()

		state, err := client.CurrentState(ctx)
		suite.Require().NoError(err)
		suite.Require().Greater(state.TxId, txWritten, "Ensure enough TXs were written")

		for i := 0; i < suite.GetFollowersCount(); i++ {
			suite.Run(fmt.Sprintf("replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				suite.WaitForCommittedTx(ctx, client, state.TxId, 5*time.Second)
			})
		}
	})

}

type SyncTestChangingMasterSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestChangingMasterSuite(t *testing.T) {
	suite.Run(t, &SyncTestChangingMasterSuite{})
}

func (suite *SyncTestChangingMasterSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 1, 0)
}

func (suite *SyncTestChangingMasterSuite) TestSyncTestChangingMasterSuite() {
	var txBeforeChangingMaster *schema.TxHeader
	suite.Run("commit before changing primary", func() {

		ctx, client, cleanup := suite.ClientForMaster()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-before-master-change"), []byte("value-before-master-change"))
		require.NoError(suite.T(), err)

		txBeforeChangingMaster = tx
	})

	suite.PromoteFollower(1, 1)

	suite.Run("commit after changing master", func() {
		ctx, client, cleanup := suite.ClientForMaster()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-after-master-change"), []byte("value-after-master-change"))
		require.NoError(suite.T(), err)

		for i := 0; i < suite.GetFollowersCount(); i++ {
			suite.Run(fmt.Sprintf("check follower %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				// Tests are flaky because it takes time to commit the
				// precommitted TX, so this function just ensures the state
				// is in sync between master and follower
				suite.WaitForCommittedTx(ctx, client, tx.Id, 30*time.Second) // Longer time since replica must reestablish connection to the primary

				val, err := client.GetAt(ctx, []byte("key-before-master-change"), txBeforeChangingMaster.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value-before-master-change"), val.Value)

				val, err = client.GetAt(ctx, []byte("key-after-master-change"), tx.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value-after-master-change"), val.Value)
			})
		}
	})
}
