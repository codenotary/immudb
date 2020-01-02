/*
Copyright 2019-2020 vChain, Inc.

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
	"math"
	"sync"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
)

type Store struct {
	db   *badger.DB
	tree *treeStore
	wg   sync.WaitGroup
	log  logger.Logger
}

func Open(options Options) (*Store, error) {
	opt := options.dataStore()
	opt.NumVersionsToKeep = math.MaxInt64 // immutability, always keep all data

	db, err := badger.OpenManaged(opt)
	if err != nil {
		return nil, err
	}

	t := &Store{
		db: db,
		// fixme(leogr): cache size could be calculated using db.MaxBatchCount()
		tree: newTreeStore(db, 750_000, opt.Logger),
		log:  opt.Logger,
	}

	// fixme(leogr): need to get all keys inserted after the tree width, if any, and replay

	t.log.Infof("Store opened at path: %s", opt.Dir)
	return t, nil
}

func (t *Store) Close() error {
	defer t.log.Infof("Store closed")
	t.wg.Wait()
	t.tree.Close()
	return t.db.Close()
}

func (t *Store) Wait() {
	t.wg.Wait()
}

func (t *Store) SetBatch(kvPairs []KVPair, options ...WriteOption) (index uint64, err error) {
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	for _, kv := range kvPairs {
		if kv.Key[0] == tsPrefix {
			err = InvalidKeyErr
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			return
		}
	}

	tsEntries := t.tree.NewBatch(kvPairs)
	ts := tsEntries[len(tsEntries)-1].ts
	index = ts - 1

	cb := func(err error) {
		if err == nil {
			for _, entry := range tsEntries {
				t.tree.Commit(entry)
			}
		} else {
			for _, entry := range tsEntries {
				t.tree.Discard(entry)
			}
		}

		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = txn.CommitAt(ts, cb) // cb will be executed in a new goroutine
	} else {
		err = txn.CommitAt(ts, nil)
		cb(err)
	}
	return
}

func (t *Store) Set(key, value []byte, options ...WriteOption) (index uint64, err error) {
	opts := makeWriteOptions(options...)
	if key[0] == tsPrefix {
		err = InvalidKeyErr
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   key,
		Value: value,
	}); err != nil {
		return
	}

	tsEntry := t.tree.NewEntry(key, value)
	index = tsEntry.ts - 1

	cb := func(err error) {
		if err == nil {
			t.tree.Commit(tsEntry)
		} else {
			t.tree.Discard(tsEntry)
		}
		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = txn.CommitAt(tsEntry.ts, cb) // cb will be executed in a new goroutine
	} else {
		err = txn.CommitAt(tsEntry.ts, nil)
		cb(err)
	}

	return
}

func (t *Store) Get(key []byte) (value []byte, index uint64, err error) {
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return
	}
	value, err = item.ValueCopy(nil)
	index = item.Version() - 1
	return
}

// fixme(leogr): need to be optmized
func (t *Store) itemAt(readTs uint64) (version uint64, key, value []byte, err error) {
	stream := t.db.NewStreamAt(readTs)
	stream.ChooseKey = func(item *badger.Item) bool {
		return item.UserMeta() != bitTreeEntry && item.Version() == readTs
	}

	found := false

	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.Kv {
			key = kv.Key
			value = kv.Value
			version = kv.Version
			found = true
			return nil
		}
		return nil
	}
	err = stream.Orchestrate(context.Background())
	if err == nil && !found {
		err = IndexNotFoundErr
	}
	return
}

func (t *Store) ByIndex(index uint64) (item *api.Item, err error) {
	version, key, value, err := t.itemAt(index + 1)
	if version != index+1 {
		err = IndexNotFoundErr
	}
	if err == nil {
		item = &api.Item{Key: key, Value: value, Index: index}
	}
	return
}

func (t *Store) History(key []byte) (items api.Items, err error) {
	txn := t.db.NewTransactionAt(math.MaxInt64, false)
	defer txn.Discard()
	it := txn.NewKeyIterator(key, badger.IteratorOptions{})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		var value []byte
		value, err = item.ValueCopy(nil)
		if err != nil {
			return
		}
		items = append(items, api.Item{
			Key:   key,
			Value: value,
			Index: item.Version() - 1,
		})
	}
	return
}

func (t *Store) HealthCheck() bool {
	_, _, err := t.Get([]byte{0})
	return err == nil || err == badger.ErrKeyNotFound
}
