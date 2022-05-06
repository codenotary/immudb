/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexerFailure(t *testing.T) {
	indexer, err := newIndexer("data", nil, nil, 0)
	require.Nil(t, indexer)
	require.ErrorIs(t, err, tbtree.ErrIllegalArguments)
}

func TestClosedIndexerFailures(t *testing.T) {
	d, err := ioutil.TempDir("", "indexertest")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	store, err := Open(d, DefaultOptions().WithIndexOptions(
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
	require.Equal(t, ErrAlreadyClosed, err)

	txs, hCount, err := indexer.History(nil, 0, false, 0)
	require.Zero(t, txs)
	require.Zero(t, hCount)
	require.Equal(t, ErrAlreadyClosed, err)

	snap, err := indexer.Snapshot()
	require.Zero(t, snap)
	require.Equal(t, ErrAlreadyClosed, err)

	snap, err = indexer.SnapshotSince(0)
	require.Zero(t, snap)
	require.Equal(t, ErrAlreadyClosed, err)

	exists, err := indexer.ExistKeyWith(nil, nil)
	require.Zero(t, exists)
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.Sync()
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.CompactIndex()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestMaxIndexWaitees(t *testing.T) {
	d, err := ioutil.TempDir("", "indexertest")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	store, err := Open(d, DefaultOptions().WithMaxWaitees(1))
	require.NoError(t, err)

	// Grab errors from waiters
	errCh := make(chan error)
	for i := 0; i < 2; i++ {
		go func() {
			errCh <- store.WaitForIndexingUpto(1, make(<-chan struct{}))
		}()
	}

	// One goroutine should fail
	select {
	case err := <-errCh:
		require.Equal(t, watchers.ErrMaxWaitessLimitExceeded, err)
	case <-time.After(time.Second):
		require.Fail(t, "Did not get waiter error")
	}

	// Store one transaction
	tx, err := store.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte{1}, nil, []byte{2})
	require.NoError(t, err)

	hdr, err := tx.AsyncCommit()
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
				require.Equal(t, ErrAlreadyClosed, err)
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
			d, err := ioutil.TempDir("", "indexertest")
			require.NoError(t, err)
			defer os.RemoveAll(d)

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
	assert.Equal(t, err, ErrAlreadyClosed)

	_, _, err = i.History(dummy, 0, false, 0)
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)

	_, err = i.Snapshot()
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)

	_, err = i.SnapshotSince(0)
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)

	_, err = i.ExistKeyWith(dummy, dummy)
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)

	err = i.Sync()
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)

	err = i.Close()
	assert.Error(t, err)
	assert.Equal(t, err, ErrAlreadyClosed)
}
