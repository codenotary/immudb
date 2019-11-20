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
	"github.com/dgraph-io/badger/v2"

	"github.com/codenotary/immudb/pkg/tree"
)

type Topic struct {
	db    *badger.DB
	ts    *badger.DB
	store *treeStore
}

func Open(options Options) (*Topic, error) {
	db, err := badger.Open(options.dataStore())
	if err != nil {
		return nil, err
	}

	ts, err := badger.OpenManaged(options.treeStore())
	if err != nil {
		return nil, err
	}

	t := &Topic{
		db:    db,
		ts:    ts,
		store: newTreeStore(ts, 10000),
	}

	return t, nil
}

func (t *Topic) Close() error {
	if err := t.db.Close(); err != nil {
		return err
	}
	return t.ts.Close()
}

func (t *Topic) Set(key, value []byte) error {
	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(key), value)
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	}

	// todo(leogr):
	//  - tree append error checking
	//  - index synching between db and ts
	//  - replay append after crash (if not synched)
	t.store.Lock()
	tree.Append(t.store, value) // fixme(leogr): assuming key is present inside value
	t.store.Unlock()

	return nil
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
