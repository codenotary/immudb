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
	"crypto/sha256"
	"encoding/binary"
	"math"

	"github.com/dgraph-io/badger/v2"

	"github.com/codenotary/immudb/pkg/tree"
)

type Topic struct {
	i     uint64
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
		store: newTreeStore(db, 750_000),
	}

	return t, nil
}

func (t *Topic) Close() error {
	t.store.Close()
	return t.db.Close()
}

// todo(leogr): move to public API package
func digestKV(key, value []byte) *[sha256.Size]byte {
	kl, vl := len(key), len(value)
	c := make([]byte, 1+8+kl+vl)
	c[0] = tree.LeafPrefix
	binary.BigEndian.PutUint64(c[1:9], uint64(kl))
	copy(c[9:], key)
	copy(c[9+kl:], value)
	h := sha256.Sum256(c)
	return &h
}

func (t *Topic) SetBatch(kvPairs []KVPair) error {
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	var next uint64
	for _, kv := range kvPairs {
		next = t.store.Add(digestKV(kv.Value, kv.Value))
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
	next := t.store.Add(digestKV(key, value))
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
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
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
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
func (t *Topic) HealthCheck() bool {
	_, err := t.Get([]byte{0})
	return err == nil || err == badger.ErrKeyNotFound
}
