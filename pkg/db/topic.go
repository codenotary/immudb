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
	"fmt"

	"github.com/dgraph-io/badger/v2"

	"github.com/codenotary/immudb/pkg/tree"
)

const reservedPrefix = '_'

type Topic struct {
	db *badger.DB
}

func NewTopic(db *badger.DB) *Topic {
	return &Topic{
		db: db,
	}
}

func (t *Topic) Set(key string, value []byte) error {

	if key[0] == reservedPrefix {
		return fmt.Errorf("invalid key format: %s", key)
	}

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(key), value)
	if err != nil {
		return err
	}

	trs, err := NewTreeStore(txn)
	if err != nil {
		return err
	}
	tr := tree.New(trs)
	tr.Add(value) // fixme(leogr): we're assuming key is contained into value

	// Commit the transaction and check for error.
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (t *Topic) Get(key string) ([]byte, error) {
	txn := t.db.NewTransaction(true)
	defer txn.Discard()
	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return val, nil
}
