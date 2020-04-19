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
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/merkletree"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
)

type Store struct {
	sync.Mutex
	db         *badger.DB
	tree       *treeStore
	wg         sync.WaitGroup
	log        logger.Logger
	changedAt  time.Time
	tamperedAt time.Time
	tampered   bool
}

func (t *Store) setChangedAt() {
	t.changedAt = time.Now()
}

func (t *Store) GetChangedAt() time.Time {
	return t.changedAt
}

func (t *Store) SetTamperedAt(ts time.Time) {
	t.tamperedAt = ts
	t.tampered = true
}

func (t *Store) AcknowledgeTampering() {
	t.tampered = false
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

	t.setChangedAt()
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
	t.setChangedAt()
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
	t.setChangedAt()
	return
}

func (t *Store) Set(kv schema.KeyValue, options ...WriteOption) (index *schema.Index, err error) {
	t.setChangedAt()
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
	t.setChangedAt()
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

func (t *Store) Scan(options schema.ScanOptions) (list *schema.ItemList, err error) {
	if len(options.Prefix) > 0 && options.Prefix[0] == tsPrefix {
		err = ErrInvalidKeyPrefix
		return
	}
	if len(options.Offset) > 0 && options.Offset[0] == tsPrefix {
		err = ErrInvalidOffset
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	seek := options.Prefix
	if options.Reverse {
		seek = append(options.Prefix, 0xFF)
	}
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
	i := uint64(0)
	for it.Seek(seek); it.Valid(); it.Next() {
		var item *schema.Item
		if it.Item().UserMeta()&bitReferenceEntry == bitReferenceEntry {
			if !options.Deep {
				continue
			}
			var refKey []byte
			err := it.Item().Value(func(val []byte) error {
				refKey = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return nil, err
			}
			if ref, err := txn.Get(refKey); err == nil {
				item, err = itemToSchema(refKey, ref)
			}
		} else {
			item, err = itemToSchema(nil, it.Item())
			if err != nil {
				return nil, err
			}
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
	err = mapError(stream.Orchestrate(context.Background()))
	if err == nil && !found {
		err = ErrIndexNotFound
	}
	return
}

func (t *Store) ByIndex(index schema.Index) (item *schema.Item, err error) {
	version, key, value, err := t.itemAt(index.Index + 1)
	if version != index.Index+1 {
		err = ErrIndexNotFound
	}
	if err == nil {
		item = &schema.Item{Key: key, Value: value, Index: index.Index}
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
	t.setChangedAt()
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
	t.setChangedAt()
	return index, nil
}

func (t *Store) ZAdd(zaddOpts schema.ZAddOptions, options ...WriteOption) (index *schema.Index, err error) {
	t.setChangedAt()
	opts := makeWriteOptions(options...)
	if err = checkKey(zaddOpts.Key); err != nil {
		return nil, err
	}
	if err := checkSet(zaddOpts.Set); err != nil {
		return nil, err
	}
	if err != nil {
		return
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
	t.setChangedAt()
	return index, nil
}

// ZScan The SCAN command is used in order to incrementally iterate over a collection of elements.
func (t *Store) ZScan(options schema.ZScanOptions) (list *schema.ItemList, err error) {

	if len(options.Offset) > 0 && options.Offset[0] == tsPrefix {
		err = ErrInvalidOffset
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   int(options.Limit),
		Prefix:         options.Set,
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

	seek := options.Set
	if options.Reverse {
		// https://github.com/dgraph-io/badger#frequently-asked-questions
		seek = append(options.Set, 0xFF)
	}

	var limit = options.Limit
	if limit == 0 {
		// we're reusing max batch count to enforce the default scan limit
		limit = uint64(t.db.MaxBatchCount())
	}
	var items []*schema.Item
	i := uint64(0)
	for it.Seek(seek); it.Valid(); it.Next() {
		var item *schema.Item
		if it.Item().UserMeta()&bitReferenceEntry == bitReferenceEntry {
			var refKey []byte
			err := it.Item().Value(func(val []byte) error {
				refKey = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return nil, err
			}
			if ref, err := txn.Get(refKey); err == nil {
				item, err = itemToSchema(refKey, ref)
			}
		} else {
			item, err = itemToSchema(nil, it.Item())
			if err != nil {
				return nil, err
			}
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

func (t *Store) Dump(kvChan chan *pb.KVList) (err error) {
	t.setChangedAt()
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
	if err := stream.Orchestrate(context.Background()); err != nil {
		return err
	}
	close(kvChan)
	t.setChangedAt()
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
				if err := ldr.Set(kv); err != nil {
					return i, err
				}
			}

			if err := ldr.Finish(); err != nil {
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

func (t *Store) HealthCheck() (*schema.HealthResponse, error) {
	_, err := t.Get(schema.Key{Key: []byte{255}})
	hr := schema.HealthResponse{
		Status: err == nil || err == ErrKeyNotFound,
	}
	if t.tampered {
		hr.TamperedAt = uint64(t.tamperedAt.Unix())
	}
	return &hr, nil
}

func (t *Store) DbSize() (int64, int64) {
	return t.db.Size()
}
