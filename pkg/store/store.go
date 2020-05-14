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
	"crypto/sha256"
	"fmt"
	"math"
	"sync"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/merkletree"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
)

type Store struct {
	sync.RWMutex
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
		return nil, mapError(err)
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
		if err = checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			err = mapError(err)
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
		err = mapError(txn.CommitAt(ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(ts, nil))
		cb(err)
	}
	return
}

func (t *Store) Set(kv schema.KeyValue, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if err = checkKey(kv.Key); err != nil {
		return nil, err
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: kv.Value,
	}); err != nil {
		err = mapError(err)
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
		err = mapError(txn.CommitAt(tsEntry.ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(tsEntry.ts, nil))
		cb(err)
	}

	return
}

func (t *Store) Get(key schema.Key) (item *schema.Item, err error) {
	if err = checkKey(key.Key); err != nil {
		return nil, err
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	i, err := txn.Get(key.Key)

	if err == nil && i.UserMeta()&bitReferenceEntry == bitReferenceEntry {
		var refkey []byte
		err = i.Value(func(val []byte) error {
			refkey = append([]byte{}, val...)
			return nil
		})
		if ref, err := txn.Get(refkey); err == nil {
			return itemToSchema(refkey, ref)
		}
	}

	if err != nil {
		err = mapError(err)
		return
	}
	return itemToSchema(key.Key, i)
}

func (t *Store) CountAll() (count uint64) {
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
	})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return
}

func (t *Store) Count(prefix schema.KeyPrefix) (count *schema.ItemsCount, err error) {
	if len(prefix.Prefix) == 0 || prefix.Prefix[0] == tsPrefix {
		err = ErrInvalidKeyPrefix
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

func (t *Store) itemAt(readTs uint64) (index uint64, key, value []byte, err error) {
	index = readTs - 1
	var refkey []byte
	// cache reference lookup
	t.tree.RLock()
	defer t.tree.RUnlock()
	if key := t.tree.rcaches[0].Get(index); key != nil {
		refkey = key.([]byte)
	}

	// disk reference lookup
	if refkey == nil {
		if err = t.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(treeKey(0, index))
			if err != nil {
				return err
			}
			if refkey, err = item.ValueCopy(nil); err != nil {
				return err
			}
			return nil
		}); err != nil {
			if err == badger.ErrKeyNotFound {
				err = ErrIndexNotFound
			}
			return 0, nil, nil, err
		}
	}

	var hash [sha256.Size]byte
	// reference parsing
	if hash, key, err = decodeRefTreeKey(refkey); err != nil {
		return 0, nil, nil, err
	}

	if key == nil {
		// this shouldn't happen
		return 0, nil, nil, ErrObsoleteDataFormat
	}

	// disk value lookup
	txn := t.db.NewTransactionAt(math.MaxInt64, false)
	defer txn.Discard()
	it := txn.NewKeyIterator(key, badger.IteratorOptions{})
	defer it.Close()
	var item *schema.Item
	for it.Rewind(); it.Valid(); it.Next() {
		item, err = itemToSchema(key, it.Item())
		if err != nil {
			return 0, nil, nil, err
		}
		// there are multiple possible versions of a key. Here we retrieve the one with the correct timestamp
		if item.Index == index {
			break
		}
	}

	// this guard ensure that the insertion order index was not tampered.
	proof := api.Digest(item.Index, key, item.Value)
	if hash != proof {
		return 0, nil, nil, fmt.Errorf("Insertion ored index %d was tampered", index)
	}
	return index, item.Key, item.Value, nil
}

func (t *Store) ByIndex(index schema.Index) (item *schema.Item, err error) {
	idx, key, value, err := t.itemAt(index.Index + 1)
	if err != nil {
		return nil, err
	}
	if err == nil {
		item = &schema.Item{Key: key, Value: value, Index: idx}
	}
	return
}

func (t *Store) History(key schema.Key) (list *schema.ItemList, err error) {
	if len(key.Key) == 0 || key.Key[0] == tsPrefix {
		err = ErrInvalidKey
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

func (t *Store) Reference(refOpts *schema.ReferenceOptions, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if len(refOpts.Key) == 0 || refOpts.Key[0] == tsPrefix {
		err = ErrInvalidKey
		return
	}
	if len(refOpts.Reference) == 0 || refOpts.Reference[0] == tsPrefix {
		err = ErrInvalidReference
		return
	}
	if err != nil {
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	i, err := txn.Get(refOpts.Key)
	if err != nil {
		err = mapError(err)
		return
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      refOpts.Reference,
		Value:    i.Key(),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(refOpts.Reference, i.Key())
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
		err = mapError(txn.CommitAt(tsEntry.ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(tsEntry.ts, nil))
		cb(err)
	}

	return index, err
}

func (t *Store) ZAdd(zaddOpts schema.ZAddOptions, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if err = checkKey(zaddOpts.Key); err != nil {
		return nil, err
	}
	if err = checkSet(zaddOpts.Set); err != nil {
		return nil, err
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	i, err := txn.Get(zaddOpts.Key)
	if err != nil {
		err = mapError(err)
		return
	}

	ik, err := SetKey(zaddOpts.Key, zaddOpts.Set, zaddOpts.Score)
	if err != nil {
		err = mapError(err)
		return
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      ik,
		Value:    i.Key(),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(zaddOpts.Key, i.Key())

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
		err = mapError(txn.CommitAt(tsEntry.ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(tsEntry.ts, nil))
		cb(err)
	}

	return index, err
}

func (t *Store) FlushToDisk() {
	defer t.tree.Unlock()
	t.tree.Lock()
	t.tree.flush()
}

func (t *Store) Dump(kvChan chan *pb.KVList) (err error) {
	defer t.tree.Unlock()
	t.tree.Lock()
	t.tree.flush()

	stream := t.db.NewStreamAt(t.tree.w)
	stream.NumGo = 16
	stream.LogPrefix = "Badger.Streaming"

	stream.Send = func(list *pb.KVList) error {
		kvChan <- list
		return nil
	}

	// Run the stream
	if err = stream.Orchestrate(context.Background()); err != nil {
		return err
	}
	close(kvChan)
	return err
}

func (t *Store) Restore(kvChan chan *pb.KVList) (i uint64, err error) {
	defer t.tree.Unlock()
	t.tree.Lock()
	ldr := t.db.NewKVLoader(16)
	for {
		kvList, more := <-kvChan
		if more {
			for _, kv := range kvList.Kv {
				if err = ldr.Set(kv); err != nil {
					return i, err
				}
			}

			if err = ldr.Finish(); err != nil {
				close(kvChan)
				return i, err
			}
			t.tree.loadTreeState()
			return t.tree.ts, err
		} else {
			err = ldr.Finish()
			close(kvChan)
			return i, err
		}
	}
}

func (t *Store) HealthCheck() bool {
	_, err := t.Get(schema.Key{Key: []byte{255}})
	return err == nil || err == ErrKeyNotFound
}

func (t *Store) DbSize() (int64, int64) {
	return t.db.Size()
}

func (t *Store) fetchFromDb(key []byte) (*schema.Item, error) {
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	i, err := txn.Get(key)

	if err == nil && i.UserMeta()&bitReferenceEntry == bitReferenceEntry {
		var refkey []byte
		err = i.Value(func(val []byte) error {
			refkey = append([]byte{}, val...)
			return nil
		})
		if ref, err := txn.Get(refkey); err == nil {
			return itemToSchema(refkey, ref)
		}
	}
	return nil, err
}
