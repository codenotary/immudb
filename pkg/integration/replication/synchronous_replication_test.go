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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SyncTestSuitePrimaryToAllReplicas struct {
	baseReplicationTestSuite
}

func TestSyncTestSuitePrimaryToAllReplicas(t *testing.T) {
	suite.Run(t, &SyncTestSuitePrimaryToAllReplicas{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestSuitePrimaryToAllReplicas) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 2, 0)
	suite.ValidateClusterSetup()
}

func (suite *SyncTestSuitePrimaryToAllReplicas) TestSyncFromPrimaryToAllReplicas() {
	ctx, client, cleanup := suite.ClientForPrimary()
	defer cleanup()

	tx1, err := client.Set(ctx, []byte("key1"), []byte("value1"))
	require.NoError(suite.T(), err)

	tx2, err := client.Set(ctx, []byte("key2"), []byte("value2"))
	require.NoError(suite.T(), err)

	for i := 0; i < suite.GetReplicasCount(); i++ {
		suite.Run(fmt.Sprintf("test replica %d", i), func() {
			ctx, client, cleanup := suite.ClientForReplica(i)
			defer cleanup()

			// Tests are flaky because it takes time to commit the
			// precommitted TX, so this function just ensures the state
			// is in sync between primary and replica
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

type SyncTestSuitePrimaryRestart struct {
	baseReplicationTestSuite
}

func TestSyncTestSuitePrimaryRestart(t *testing.T) {
	suite.Run(t, &SyncTestSuitePrimaryRestart{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestSuitePrimaryRestart) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 2, 0)
	suite.ValidateClusterSetup()
}

func (suite *SyncTestSuitePrimaryRestart) TestPrimaryRestart() {
	var txBeforeRestart *schema.TxHeader
	suite.Run("commit before restarting primary", func() {

		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-before-restart"), []byte("value-before-restart"))
		require.NoError(suite.T(), err)

		txBeforeRestart = tx
	})

	suite.RestartPrimary()

	suite.Run("commit after restarting primary", func() {
		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key3"), []byte("value3"))
		require.NoError(suite.T(), err)

		for i := 0; i < suite.GetReplicasCount(); i++ {
			suite.Run(fmt.Sprintf("check replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				// Tests are flaky because it takes time to commit the
				// precommitted TX, so this function just ensures the state
				// is in sync between primary and replica
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

type SyncTestSuitePrecommitStateSync struct {
	baseReplicationTestSuite
}

func TestSyncTestSuitePrecommitStateSync(t *testing.T) {
	suite.Run(t, &SyncTestSuitePrecommitStateSync{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestSuitePrecommitStateSync) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 2, 0)
	suite.ValidateClusterSetup()
}

// TestPrecommitStateSync checks if the precommit state at primary
// and its replicas are in sync during synchronous replication
func (suite *SyncTestSuitePrecommitStateSync) TestPrecommitStateSync() {
	var (
		primaryState *schema.ImmutableState
		err          error
		startCh      = make(chan bool)
	)

	ctx, client, cleanup := suite.ClientForPrimary()
	defer cleanup()

	// Create goroutines for client waiting to query the state
	// of the replicas. This is initialized before to avoid
	// spending time initializing the replica client for faster
	// state access
	var wg sync.WaitGroup
	for i := 0; i < suite.GetReplicasCount(); i++ {
		wg.Add(1)
		go func(replicaID int) {
			defer wg.Done()
			ctx, client, cleanup := suite.ClientForReplica(replicaID)
			defer cleanup()

			<-startCh

			suite.Run(fmt.Sprintf("test replica sync state %d", replicaID), func() {
				state, err := client.CurrentState(ctx)
				require.NoError(suite.T(), err)
				suite.Require().Equal(state.PrecommittedTxId, primaryState.TxId)
				suite.Require().Equal(state.PrecommittedTxHash, primaryState.TxHash)
			})
		}(i)
	}

	// add multiple keys to make update the primary's state quickly
	for i := 10; i < 30; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		_, err = client.Set(ctx, []byte(key), []byte(value))
		require.NoError(suite.T(), err)
	}

	// get the current precommit txn id state of primary
	primaryState, err = client.CurrentState(ctx)
	require.NoError(suite.T(), err)

	// close will unblock all goroutines
	close(startCh)

	wg.Wait()
}

type SyncTestMinimumReplicasSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestMinimumReplicasSuite(t *testing.T) {
	suite.Run(t, &SyncTestMinimumReplicasSuite{})
}

// this function executes before the test suite begins execution
func (suite *SyncTestMinimumReplicasSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(4, 2, 0)
	suite.ValidateClusterSetup()
}

// TestMinimumReplicas ensures the primary can operate as long as the minimum
// number of replicas send their confirmations
func (suite *SyncTestMinimumReplicasSuite) TestMinimumReplicas() {

	ctx, client, cleanup := suite.ClientForPrimary()
	defer cleanup()

	suite.Run("should commit successfully without one replica", func() {
		suite.StopReplica(0)

		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key1"), []byte("value1"))
		require.NoError(suite.T(), err)
	})

	suite.Run("should commit successfully without two replicas", func() {
		suite.StopReplica(1)

		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key2"), []byte("value2"))
		require.NoError(suite.T(), err)
	})

	suite.Run("should not commit without three replicas", func() {
		suite.StopReplica(2)

		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key3"), []byte("value3"))
		require.Error(suite.T(), err)
		require.Contains(suite.T(), err.Error(), "deadline")
	})

	suite.Run("should commit again once first replica is back online", func() {
		suite.StartReplica(0)

		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := client.Set(ctxTimeout, []byte("key4"), []byte("value4"))
		require.NoError(suite.T(), err)
	})

	suite.Run("should recover with all replicas replaced", func() {
		suite.StopReplica(0)
		suite.StopReplica(3)

		suite.AddReplica(true)
		suite.AddReplica(true)

		ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
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

				suite.WaitForCommittedTx(ctx, client, primaryState.TxId, 2*time.Second)

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
	suite.ValidateClusterSetup()
}

func (suite *SyncTestRecoverySpeedSuite) TestReplicaRecoverySpeed() {

	const parallelWriters = 30
	const samplingTime = time.Second * 5

	// Stop the replica, we don't replicate any transactions to it now
	// but we can still commit using the second replica
	suite.StopReplica(0)

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

				ctx, client, cleanup := suite.ClientForPrimary()
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

		// Stop the second replica, now the DB is locked
		suite.StopReplica(1)

		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		state, err := client.CurrentState(ctx)
		suite.Require().NoError(err)
		suite.Require().Equal(state.TxId, txWritten, "Ensure enough TXs were written")

		// Check if we can recover the cluster and perform write within a reasonable amount of time
		// that was needed for initial sampling. The replica that was initially stopped and now
		// started has the same amount of transaction to grab from primary as the other one
		// which should take the same amount of time as the initial write period or less
		// (since the primary is not persisting data this time).
		ctxTimeout, cancel := context.WithTimeout(ctx, samplingTime*4)
		defer cancel()

		suite.StartReplica(0) // 1 down

		tx, err = client.Set(ctxTimeout, []byte("key-after-recovery"), []byte("value-after-recovery"))
		suite.NoError(err)
	})

	suite.Run("Ensure the data is readable from replicas", func() {
		suite.StartReplica(1)

		suite.Run("primary", func() {
			ctx, client, cleanup := suite.ClientForPrimary()
			defer cleanup()

			val, err := client.GetAt(ctx, []byte("key-after-recovery"), tx.Id)
			suite.NoError(err)
			suite.Equal([]byte("value-after-recovery"), val.Value)
		})

		for i := 0; i < suite.GetReplicasCount(); i++ {
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

type SyncTestWithAsyncReplicaSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestWithAsyncReplicaSuite(t *testing.T) {
	suite.Run(t, &SyncTestWithAsyncReplicaSuite{})
}

func (suite *SyncTestWithAsyncReplicaSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 1, 1)
	suite.ValidateClusterSetup()
}

func (suite *SyncTestWithAsyncReplicaSuite) TestSyncReplicationAlongWithAsyncReplicas() {
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

				ctx, client, cleanup := suite.ClientForPrimary()
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
		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		state, err := client.CurrentState(ctx)
		suite.Require().NoError(err)
		suite.Require().Equal(state.TxId, txWritten, "Ensure enough TXs were written")

		for i := 0; i < suite.GetReplicasCount(); i++ {
			suite.Run(fmt.Sprintf("replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				suite.WaitForCommittedTx(ctx, client, state.TxId, 20*time.Second)
			})
		}
	})

}

type SyncTestChangingPrimarySuite struct {
	baseReplicationTestSuite
}

func TestSyncTestChangingPrimarySuite(t *testing.T) {
	suite.Run(t, &SyncTestChangingPrimarySuite{})
}

func (suite *SyncTestChangingPrimarySuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(2, 2, 0)
	suite.ValidateClusterSetup()
}

func (suite *SyncTestChangingPrimarySuite) TestSyncTestChangingPrimarySuite() {
	var txBeforeChangingPrimary *schema.TxHeader
	suite.Run("commit before changing primary", func() {

		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-before-primary-change"), []byte("value-before-primary-change"))
		require.NoError(suite.T(), err)

		txBeforeChangingPrimary = tx
	})

	// it's possible to promote any replica as new primary because ack from all replicas is required by primary
	// ensure the replica to be promoted is up to date with primary's commit state
	ctx, client, cleanup := suite.ClientForReplica(1)
	suite.WaitForCommittedTx(ctx, client, txBeforeChangingPrimary.Id, 1*time.Second)
	cleanup()

	suite.PromoteReplica(1, 1)

	suite.Run("commit after changing primary", func() {
		ctx, client, cleanup := suite.ClientForPrimary()
		defer cleanup()

		tx, err := client.Set(ctx, []byte("key-after-primary-change"), []byte("value-after-primary-change"))
		require.NoError(suite.T(), err)

		for i := 0; i < suite.GetReplicasCount(); i++ {
			suite.Run(fmt.Sprintf("check replica %d", i), func() {
				ctx, client, cleanup := suite.ClientForReplica(i)
				defer cleanup()

				// Tests are flaky because it takes time to commit the
				// precommitted TX, so this function just ensures the state
				// is in sync between primary and replica
				suite.WaitForCommittedTx(ctx, client, tx.Id, 30*time.Second) // Longer time since replica must reestablish connection to the primary

				val, err := client.GetAt(ctx, []byte("key-before-primary-change"), txBeforeChangingPrimary.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value-before-primary-change"), val.Value)

				val, err = client.GetAt(ctx, []byte("key-after-primary-change"), tx.Id)
				require.NoError(suite.T(), err)
				require.Equal(suite.T(), []byte("value-after-primary-change"), val.Value)
			})
		}
	})
}

type SyncTestChangingMasterSettingsSuite struct {
	baseReplicationTestSuite
}

func TestSyncTestChangingMasterSettingsSuite(t *testing.T) {
	suite.Run(t, &SyncTestChangingMasterSettingsSuite{})
}

func (suite *SyncTestChangingMasterSettingsSuite) SetupTest() {
	suite.baseReplicationTestSuite.SetupTest()
	suite.SetupCluster(1, 1, 0)
	suite.ValidateClusterSetup()
}

func (suite *SyncTestChangingMasterSettingsSuite) TestSyncTestChangingMasterSuite() {
	suite.Run("get one locked writer due to insufficient confirmations", func() {
		ctx, mc, cleanup := suite.ClientForPrimary()
		defer cleanup()
		_, err := mc.UpdateDatabaseV2(ctx, suite.primaryDBName, &schema.DatabaseNullableSettings{
			ReplicationSettings: &schema.ReplicationNullableSettings{
				SyncAcks: &schema.NullableUint32{
					Value: 2,
				},
			},
		})
		suite.Require().NoError(err)

		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err = mc.Set(ctxWithTimeout, []byte("key"), []byte("value"))
		suite.Require().Error(err)
		suite.Require().Contains(err.Error(), context.DeadlineExceeded.Error())
	})

	suite.Run("recover from locked write by changing database settings", func() {
		ctx, mc, cleanup := suite.ClientForPrimary()
		defer cleanup()

		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := mc.UpdateDatabaseV2(ctxWithTimeout, suite.primaryDBName, &schema.DatabaseNullableSettings{
			ReplicationSettings: &schema.ReplicationNullableSettings{
				SyncAcks: &schema.NullableUint32{
					Value: 1,
				},
			},
		})
		suite.Require().NoError(err)

		ctxWithTimeout, cancel = context.WithTimeout(ctx, time.Second)
		defer cancel()

		_, err = mc.Set(ctxWithTimeout, []byte("key2"), []byte("value2"))
		suite.Require().NoError(err)
	})

	suite.Run("ensure all commits are correctly persisted", func() {
		ctx, mc, cleanup := suite.ClientForPrimary()
		defer cleanup()

		val, err := mc.Get(ctx, []byte("key"))
		suite.Require().NoError(err)
		suite.Require().Equal([]byte("value"), val.Value)

		val, err = mc.Get(ctx, []byte("key2"))
		suite.Require().NoError(err)
		suite.Require().Equal([]byte("value2"), val.Value)
	})
}
