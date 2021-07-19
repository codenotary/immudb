/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"testing"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/stretchr/testify/require"
)

func TestNewIndexerFailure(t *testing.T) {
	indexer, err := newIndexer("data", nil, nil, 0)
	require.Nil(t, indexer)
	require.Equal(t, tbtree.ErrIllegalArguments, err)
}

func TestClosedIndexerFailures(t *testing.T) {
	d, err := ioutil.TempDir("", "indexertest")
	require.NoError(t, err)
	defer os.RemoveAll(d)

	store, err := Open(d, DefaultOptions().WithIndexOptions(
		DefaultIndexOptions().WithCompactionThld(0),
	))
	require.NoError(t, err)

	store.indexer.closed = true
	require.NoError(t, err)

	indexer := store.indexer

	v, tx, hc, err := indexer.Get(nil)
	require.Zero(t, v)
	require.Zero(t, tx)
	require.Zero(t, hc)
	require.Equal(t, ErrAlreadyClosed, err)

	txs, err := indexer.History(nil, 0, false, 0)
	require.Zero(t, txs)
	require.Equal(t, ErrAlreadyClosed, err)

	snap, err := indexer.Snapshot()
	require.Zero(t, snap)
	require.Equal(t, ErrAlreadyClosed, err)

	snap, err = indexer.SnapshotSince(0)
	require.Zero(t, snap)
	require.Equal(t, ErrAlreadyClosed, err)

	exists, err := indexer.ExistKeyWith(nil, nil, false)
	require.Zero(t, exists)
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.Sync()
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	err = indexer.CompactIndex()
	require.Equal(t, ErrAlreadyClosed, err)
}
