/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"path/filepath"
	"sync"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type stateServiceMock struct {
	cl           sync.Mutex
	m            sync.RWMutex
	state        *schema.ImmutableState
	stateHistory map[uint64]*schema.ImmutableState
}

func newServiceStateMock() *stateServiceMock {
	return &stateServiceMock{
		state:        &schema.ImmutableState{TxId: 0},
		stateHistory: make(map[uint64]*schema.ImmutableState),
	}
}

func (ssm *stateServiceMock) GetState(ctx context.Context, db string) (*schema.ImmutableState, error) {
	ssm.m.RLock()
	defer ssm.m.RUnlock()

	return ssm.state, nil
}

func (ssm *stateServiceMock) SetState(db string, state *schema.ImmutableState) error {
	ssm.m.Lock()
	defer ssm.m.Unlock()

	ssm.state = state
	ssm.stateHistory[state.TxId] = state
	return nil
}

func (ssm *stateServiceMock) CacheLock() error {
	ssm.cl.Lock()
	return nil
}

func (ssm *stateServiceMock) CacheUnlock() error {
	ssm.cl.Unlock()
	return nil
}

func TestLongLinearProofVerification(t *testing.T) {
	// Start the server with transaction data containing long linear proof
	dir := t.TempDir()
	copier := fs.NewStandardCopier()
	require.NoError(t, copier.CopyDir("../../test/data_long_linear_proof", filepath.Join(dir, "defaultdb")))

	options := server.DefaultOptions().WithDir(dir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	require.NoError(t, err)
	defer bs.Stop()

	cl, err := bs.NewAuthenticatedClient(client.DefaultOptions().WithDir(t.TempDir()))
	require.NoError(t, err)
	defer cl.CloseSession(context.Background())

	// Inject our custom state service to have insight into the state values
	ssm := newServiceStateMock()
	cl.WithStateService(ssm)

	const txCount = 30

	t.Run("verify server data", func(t *testing.T) {
		sc := cl.GetServiceClient()

		st, err := sc.CurrentState(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.EqualValues(t, txCount, st.TxId)

		t.Run("transactions 1-10 do not use linear proof longer than 1", func(t *testing.T) {
			for txID := uint64(1); txID <= 10; txID++ {
				tx, err := sc.TxById(context.Background(), &schema.TxRequest{
					Tx: txID,
					EntriesSpec: &schema.EntriesSpec{
						KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
					},
				})
				require.NoError(t, err)
				require.Equal(t, txID-1, tx.Header.BlTxId)
			}
		})

		t.Run("transactions 11-20 use long linear proof", func(t *testing.T) {
			for txID := uint64(11); txID <= 20; txID++ {
				tx, err := sc.TxById(context.Background(), &schema.TxRequest{
					Tx: txID,
					EntriesSpec: &schema.EntriesSpec{
						KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
					},
				})
				require.NoError(t, err)
				require.EqualValues(t, 10, tx.Header.BlTxId)
			}
		})

		t.Run("transactions 21-30 do not use linear proof longer than 1", func(t *testing.T) {
			for txID := uint64(21); txID <= txCount; txID++ {
				tx, err := sc.TxById(context.Background(), &schema.TxRequest{
					Tx: txID,
					EntriesSpec: &schema.EntriesSpec{
						KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
					},
				})
				require.NoError(t, err)
				require.Equal(t, txID-1, tx.Header.BlTxId)
			}
		})
	})

	t.Run("get all transaction states", func(t *testing.T) {
		for txID := uint64(1); txID <= txCount; txID++ {
			_, err = cl.VerifiedTxByID(context.Background(), txID)
			require.NoError(t, err)
			require.Contains(t, ssm.stateHistory, txID)
		}
		require.Len(t, ssm.stateHistory, txCount)
	})

	t.Run("Exhaustive consistency proof", func(t *testing.T) {
		for i := uint64(1); i <= txCount; i++ {
			for j := i; j <= txCount; j++ {
				ssm.state = ssm.stateHistory[i]

				_, err = cl.VerifiedTxByID(context.Background(), j)
				require.NoError(t, err)
				require.EqualValues(t, j, ssm.state.TxId)
			}
		}
	})

}
