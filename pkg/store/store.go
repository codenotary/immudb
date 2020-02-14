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

	"github.com/codenotary/merkletree"

	"github.com/codenotary/immudb/pkg/api/schema"

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

// CurrentRoot returns the index and the hash of the current tree root, if any.
// When the tree is empty and no root is available then the zerovalue for _schema.Root_ is returned instead.
func (t *Store) CurrentRoot() (root *schema.Root, err error) {
	root = &schema.Root{}

	t.tree.RLock()
	defer t.tree.RUnlock()
	if w := t.tree.Width(); w > 0 {
		r := merkletree.Root(t.tree)
		root.Root = r[:]
		root.Index = w - 1
	}

	return
}

func (t *Store) SetBatch(list schema.KVList, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	for _, kv := range list.KVs {
		err = checkKey(kv.Key)
		if err != nil {
			return
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			return
		}
	}

	tsEntries := t.tree.NewBatch(&list)
	ts := tsEntries[len(tsEntries)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}

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

func (t *Store) Set(kv schema.KeyValue, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	err = checkKey(kv.Key)
	if err != nil {
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: kv.Value,
	}); err != nil {
		return
	}

	tsEntry := t.tree.NewEntry(kv.Key, kv.Value)
	index = &schema.Index{
		Index: tsEntry.ts - 1,
	}

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

func (t *Store) Get(key schema.Key) (item *schema.Item, err error) {
	err = checkKey(key.Key)
	if err != nil {
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	i, err := txn.Get(key.Key)
	if err != nil {
		return
	}
	return itemToSchema(key.Key, i)
}

func (t *Store) Scan(options schema.ScanOptions) (list *schema.ItemList, err error) {
	if len(options.Prefix) > 0 && options.Prefix[0] == tsPrefix {
		err = InvalidKeyPrefixErr
		return
	}
	if len(options.Offset) > 0 && options.Offset[0] == tsPrefix {
		err = InvalidOffsetErr
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   int(options.Limit),
		Prefix:         options.Prefix,
		Reverse:        options.Reverse,
	})
	defer it.Close()

	if len(options.Offset) == 0 {
		it.Rewind()
	} else {
		it.Seek(options.Offset)
		if it.Valid() {
			it.Next() // skip the offset item
		}
	}

	var limit = options.Limit
	if limit == 0 {
		// we're reusing max batch count to enforce the default scan limit
		limit = uint64(t.db.MaxBatchCount())
	}
	var items []*schema.Item
	for i := uint64(0); it.Valid(); it.Next() {
		item, err := itemToSchema(nil, it.Item())
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if i++; i == limit {
			break
		}
	}
	list = &schema.ItemList{
		Items: items,
	}
	return
}

func (t *Store) Count(prefix schema.KeyPrefix) (count *schema.ItemsCount, err error) {
	if len(prefix.Prefix) == 0 || prefix.Prefix[0] == tsPrefix {
		err = InvalidKeyPrefixErr
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         prefix.Prefix,
	})
	defer it.Close()
	count = &schema.ItemsCount{}
	for it.Rewind(); it.Valid(); it.Next() {
		count.Count++
	}
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

func (t *Store) ByIndex(index schema.Index) (item *schema.Item, err error) {
	version, key, value, err := t.itemAt(index.Index + 1)
	if version != index.Index+1 {
		err = IndexNotFoundErr
	}
	if err == nil {
		item = &schema.Item{Key: key, Value: value, Index: index.Index}
	}
	return
}

func (t *Store) History(key schema.Key) (list *schema.ItemList, err error) {
	if len(key.Key) == 0 || key.Key[0] == tsPrefix {
		err = InvalidKeyErr
		return
	}
	txn := t.db.NewTransactionAt(math.MaxInt64, false)
	defer txn.Discard()
	it := txn.NewKeyIterator(key.Key, badger.IteratorOptions{})
	defer it.Close()

	var items []*schema.Item
	for it.Rewind(); it.Valid(); it.Next() {
		item, err := itemToSchema(key.Key, it.Item())
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	list = &schema.ItemList{
		Items: items,
	}
	return
}

func (t *Store) HealthCheck() bool {
	_, err := t.Get(schema.Key{Key: []byte{255}})
	return err == nil || err == badger.ErrKeyNotFound
}
