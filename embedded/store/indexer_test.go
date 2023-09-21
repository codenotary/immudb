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

package store

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/watchers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexerFailure(t *testing.T) {
	indexer, err := newIndexer(DefaultIndexID, t.TempDir(), nil, nil)
	require.Nil(t, indexer)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestClosedIndexerFailures(t *testing.T) {
	store, err := Open(t.TempDir(), DefaultOptions().WithIndexOptions(
		DefaultIndexOptions().WithCompactionThld(1),
	))
	require.NoError(t, err)

	err = store.indexer.Close()
	require.NoError(t, err)

	indexer := store.indexer

	v, tx, hc, err := indexer.Get(nil)
	require.Zero(t, v)
	require.Zero(t, tx)
	require.Zero(t, hc)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	txs, hCount, err := indexer.History(nil, 0, false, 0)
	require.Zero(t, txs)
	require.Zero(t, hCount)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	snap, err := indexer.Snapshot()
	require.Zero(t, snap)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	snap, err = indexer.SnapshotMustIncludeTxIDWithRenewalPeriod(0, 0)
	require.Zero(t, snap)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, _, _, err = indexer.GetWithPrefix(nil, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = indexer.Sync()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = indexer.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = indexer.CompactIndex()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestMaxIndexWaitees(t *testing.T) {
	store, err := Open(t.TempDir(), DefaultOptions().WithMaxWaitees(1).WithMaxActiveTransactions(1))
	require.NoError(t, err)

	// Grab errors from waiters
	errCh := make(chan error)
	for i := 0; i < 2; i++ {
		go func() {
			errCh <- store.WaitForIndexingUpto(context.Background(), 1)
		}()
	}

	// One goroutine should fail
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, watchers.ErrMaxWaitessLimitExceeded)
	case <-time.After(time.Second):
		require.Fail(t, "Did not get waiter error")
	}

	// Store one transaction
	tx, err := store.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte{1}, nil, []byte{2})
	require.NoError(t, err)

	hdr, err := tx.AsyncCommit(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, hdr.ID)

	// Other goroutine should succeed
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "Did not get successful wait confirmation")
	}
}

func TestRestartIndexCornerCases(t *testing.T) {
	for _, c := range []struct {
		name string
		fn   func(t *testing.T, dir string, s *ImmuStore)
	}{
		{
			"Closed store",
			func(t *testing.T, dir string, s *ImmuStore) {
				s.Close()
				err := s.indexer.restartIndex()
				require.ErrorIs(t, err, ErrAlreadyClosed)
			},
		},
		{
			"No nodes folder",
			func(t *testing.T, dir string, s *ImmuStore) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "index/commit1"), 0777))
				err := s.indexer.restartIndex()
				require.NoError(t, err)
			},
		},
		{
			"No commit folder",
			func(t *testing.T, dir string, s *ImmuStore) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "index/nodes1"), 0777))
				err := s.indexer.restartIndex()
				require.NoError(t, err)
			},
		},
		{
			"Invalid index structure",
			func(t *testing.T, dir string, s *ImmuStore) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "index/nodes1"), 0777))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "index/commit1"), []byte{}, 0777))
				err := s.indexer.restartIndex()
				require.NoError(t, err)
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := t.TempDir()
			store, err := Open(d, DefaultOptions())
			require.NoError(t, err)
			defer store.Close()

			c.fn(t, d, store)
		})
	}
}

func TestClosedIndexer(t *testing.T) {
	i := indexer{closed: true}
	var err error
	dummy := []byte("dummy")

	_, _, _, err = i.Get(dummy)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, err = i.History(dummy, 0, false, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = i.Snapshot()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = i.SnapshotMustIncludeTxIDWithRenewalPeriod(0, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, _, _, err = i.GetWithPrefix(dummy, dummy)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	err = i.Sync()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)

	err = i.Close()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrAlreadyClosed)
}
