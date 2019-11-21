/*
Copyright 2019 vChain, Inc.

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

package db

import (
	"math"

	"github.com/codenotary/immudb/pkg/tree"
	"github.com/dgraph-io/badger/v2"
)

type Topic struct {
	db    *badger.DB
	store *treeStore
}

func Open(options Options) (*Topic, error) {
	db, err := badger.OpenManaged(options.dataStore())
	if err != nil {
		return nil, err
	}

	t := &Topic{
		db:    db,
		store: newTreeStore(db, 500000),
	}

	return t, nil
}

func (t *Topic) Close() error {
	t.store.Close()
	return t.db.Close()
}

func (t *Topic) SetBatch(kvPairs []KVPair) error {
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	var next uint64
	for _, kv := range kvPairs {
		h := tree.LeafHash(kv.Value)
		next = t.store.Add(&h)
		if err := txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			return err
		}
	}
	return txn.CommitAt(next, nil)
}

func (t *Topic) Set(key, value []byte) error {
	h := tree.LeafHash(value)
	next := t.store.Add(&h)
	txn := t.db.NewTransactionAt(math.MaxUint64, true) // we don't read, so set readTs to max
	defer txn.Discard()
	if err := txn.SetEntry(&badger.Entry{
		Key:   key,
		Value: value,
	}); err != nil {
		return err
	}
	return txn.CommitAt(next, nil)
}

func (t *Topic) Get(key []byte) ([]byte, error) {
	txn := t.db.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}
