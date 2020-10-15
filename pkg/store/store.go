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
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"sync"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/merkletree"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
)

// Store ...
type Store struct {
	sync.RWMutex
	db   *badger.DB
	tree *treeStore
	wg   sync.WaitGroup
	log  logger.Logger
}

// Open opens the store with the specified options
func Open(options Options, badgerOptions badger.Options) (*Store, error) {
	badgerOpts := badgerOptions
	badgerOpts.ValueDir = badgerOptions.Dir
	badgerOpts.NumVersionsToKeep = math.MaxInt64 // immutability, always keep all data

	db, err := badger.OpenManaged(badgerOpts)
	if err != nil {
		return nil, mapError(err)
	}

	// fixme(leogr): cache size could be calculated using db.MaxBatchCount()
	tstore, err := newTreeStore(db, 750_000, false, options.log)
	if err != nil {
		return nil, err
	}

	t := &Store{
		db:   db,
		tree: tstore,
		log:  options.log,
	}

	if t.tree.lastFlushed < t.tree.w {
		t.log.Infof("Replaying %d missing entries...", t.tree.w-t.tree.lastFlushed)
		err = t.commitPendingTreeEntries()
		if err != nil {
			return nil, err
		}
		t.log.Infof("All missing entries had been successfully applied!")
	}

	t.log.Debugf("Store opened at path: %s", badgerOpts.Dir)
	return t, nil
}

func (t *Store) commitPendingTreeEntries() error {
	w := t.tree.w
	t.tree.w = t.tree.lastFlushed

	for i := t.tree.lastFlushed; i < w; i++ {
		idx, key, value, err := t.itemAt(i + 1)
		if err != nil {
			return err
		}
		h := api.Digest(idx, key, value)
		tsEntry := &treeStoreEntry{
			ts: i + 1,
			h:  &h,
			r:  &key,
		}
		t.tree.Commit(tsEntry)
	}
	return nil
}

// Close closes the store
func (t *Store) Close() error {
	defer t.log.Debugf("Store closed")
	t.wg.Wait()
	t.tree.Close()
	return t.db.Close()
}

// Wait ...
func (t *Store) Wait() {
	t.wg.Wait()
}

// CurrentRoot returns the index and the hash of the current tree root, if any.
// When the tree is empty and no root is available then the zerovalue for _schema.Root_ is returned instead.
func (t *Store) CurrentRoot() (root *schema.Root, err error) {
	root = schema.NewRoot()

	t.tree.RLock()
	defer t.tree.RUnlock()
	if w := t.tree.Width(); w > 0 {
		r := merkletree.Root(t.tree)
		root.SetRoot(r[:])
		root.SetIndex(w - 1)
	}

	return
}

// SetBatch adds many entries at once
func (t *Store) SetBatch(list schema.KVList, options ...WriteOption) (index *schema.Index, err error) {
	if err = list.Validate(); err != nil {
		return nil, err
	}
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	tsEntries := t.tree.NewBatch(&list)

	for i, kv := range list.KVs {
		if err = checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: WrapValueWithTS(kv.Value, tsEntries[i].ts),
		}); err != nil {
			err = mapError(err)
			return
		}
	}

	ts := tsEntries[len(tsEntries)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}

	for _, leafEntry := range tsEntries {
		if err = txn.SetEntry(&badger.Entry{
			Key:      treeKey(uint8(0), leafEntry.ts-1),
			Value:    refTreeKey(*leafEntry.h, *leafEntry.r),
			UserMeta: bitTreeEntry,
		}); err != nil {
			err = mapError(err)
			return
		}
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

// Set adds a new entry
func (t *Store) Set(kv schema.KeyValue, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if err = checkKey(kv.Key); err != nil {
		return nil, err
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	tsEntry := t.tree.NewEntry(kv.Key, kv.Value)

	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: WrapValueWithTS(kv.Value, tsEntry.ts),
	}); err != nil {
		err = mapError(err)
		return
	}

	index = &schema.Index{
		Index: tsEntry.ts - 1,
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
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

// Get fetches the entry having the specified key
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
			refkey, _ = UnwrapValueWithTS(val)
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

// CountAll returns the total number of entries
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

// Count returns the number of entris having the specified key prefix
func (t *Store) Count(prefix schema.KeyPrefix) (count *schema.ItemsCount, err error) {
	if isReservedKey(prefix.Prefix) {
		err = ErrInvalidKeyPrefix
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	count = &schema.ItemsCount{}
	it := txn.NewKeyIterator(prefix.Prefix, badger.IteratorOptions{})
	defer it.Close()
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
	if key := t.tree.rcache.Get(index); key != nil {
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
		i, err := itemToSchema(key, it.Item())
		if err != nil {
			return 0, nil, nil, err
		}
		// there are multiple possible versions of a key. Here we retrieve the one with the correct timestamp
		if i.Index == index {
			item = i
			break
		}
	}

	if item == nil {
		// this shouldn't happen
		return 0, nil, nil, ErrKeyNotFound
	}

	// this guard ensure that the insertion order index was not tampered.
	realHash := api.Digest(index, key, item.Value)
	if hash != realHash {
		return 0, nil, nil, ErrInconsistentDigest
	}
	return index, item.Key, item.Value, nil
}

// ByIndex fetches the entry at the specified index
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

// History fetches the complete history of entries for the specified key
func (t *Store) History(key schema.Key) (list *schema.ItemList, err error) {
	if isReservedKey(key.Key) {
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

// Reference adds a new entry who's value is an existing key
func (t *Store) Reference(refOpts *schema.ReferenceOptions, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if isReservedKey(refOpts.Key) {
		err = ErrInvalidKey
		return
	}
	if isReservedKey(refOpts.Reference) {
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

	tsEntry := t.tree.NewEntry(refOpts.Reference, i.Key())

	if err = txn.SetEntry(&badger.Entry{
		Key:      refOpts.Reference,
		Value:    WrapValueWithTS(i.Key(), tsEntry.ts),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	index = &schema.Index{
		Index: tsEntry.ts - 1,
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
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

// ZAdd adds a score for an existing key in a sorted set
// As a parameter of ZAddOptions is possible to provide the associated index of the provided key. In this way, when resolving reference, the specified version of the key will be returned.
// If the index is not provided the resolution will use only the key and last version of the item will be returned
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

	var referenceValue []byte
	if zaddOpts.Index != nil {
		// convert to internal timestamp for itemAt, that returns the index
		_, key, _, err := t.itemAt(zaddOpts.Index.Index + 1)
		if err != nil {
			err = mapError(err)
			return nil, err
		}
		// here we append the index to the reference value
		referenceValue = WrapZIndexReference(key, zaddOpts.Index)
	} else {
		var i *badger.Item
		i, err = txn.Get(zaddOpts.Key)
		if err != nil {
			err = mapError(err)
			return nil, err
		}
		// here we append a flag that the index reference was not specified. Thanks to this we will use only the key to calculate digest
		referenceValue = WrapZIndexReference(i.Key(), nil)
	}

	ik, err := SetKey(zaddOpts.Key, zaddOpts.Set, zaddOpts.Score)
	if err != nil {
		err = mapError(err)
		return nil, err
	}

	tsEntry := t.tree.NewEntry(ik, referenceValue)

	if err = txn.SetEntry(&badger.Entry{
		Key:      ik,
		Value:    WrapValueWithTS(referenceValue, tsEntry.ts),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return nil, err
	}

	index = &schema.Index{
		Index: tsEntry.ts - 1,
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
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

// FlushToDisk flushes cached data from memory to disk
func (t *Store) FlushToDisk() {
	defer t.tree.Unlock()
	t.wg.Wait()
	t.tree.Lock()
	t.tree.flush()
}

// Dump returns a dump of the database
func (t *Store) Dump(kvChan chan *pb.KVList) (err error) {
	defer t.tree.Unlock()
	t.tree.Lock()
	t.tree.flush()

	var emptyCaches = true
	for _, c := range t.tree.caches {
		tail := c.Tail()
		if tail == 0 {
			continue
		}
		emptyCaches = false
	}
	stream := t.db.NewStreamAt(t.tree.w)
	stream.NumGo = 16
	stream.LogPrefix = "Badger.Streaming"

	stream.Send = func(list *pb.KVList) error {
		kvChan <- list
		return nil
	}
	//workaround possible badger bug
	//ReadTs should not be retrieved for managed DB
	if !emptyCaches {
		// Run the stream
		if err = stream.Orchestrate(context.Background()); err != nil {
			return err
		}
	}
	close(kvChan)
	return err
}

// Restore restores a database
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

// HealthCheck ...
func (t *Store) HealthCheck() bool {
	_, err := t.Get(schema.Key{Key: []byte{255}})
	return err == nil || err == ErrKeyNotFound
}

// DbSize ...
func (t *Store) DbSize() (int64, int64) {
	return t.db.Size()
}

// GetTree returns a structure that rapresents merkle tree. Every node is marked as in memory, root and with reference key.
func (t *Store) GetTree() *schema.Tree {
	// Build disk tree
	disktree := &schema.Tree{}
	t.db.View(func(txn *badger.Txn) error {
		for l := uint8(0); l < math.MaxInt8; l++ {
			layer := &schema.Layer{}
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Reverse = false
			it := txn.NewIterator(opts)

			maxKey := []byte{tsPrefix, l}
			for it.Seek(maxKey); it.ValidForPrefix(maxKey); it.Next() {
				item := it.Item()
				node := &schema.Node{}
				node.I = item.KeyCopy(nil)
				temp, _ := item.ValueCopy(nil)
				var refk []byte
				var hash [32]byte
				hash, refk, _ = decodeRefTreeKey(temp)
				node.H = hash[:]
				node.Refk = refk
				node.Cache = false
				if len(refk) > 0 {
					node.Ref = true
				}
				layer.L = append(layer.L, node)
			}
			it.Close()
			if len(layer.L) > 0 {
				disktree.T = append(disktree.T, layer)
			}
		}

		return nil
	})
	t.tree.Lock()
	defer t.tree.Unlock()

	// Build cache tree
	memtree := &schema.Tree{}
	for l, c := range t.tree.caches {
		tail := c.Tail()
		if tail == 0 {
			continue
		}
		memlayer := &schema.Layer{}
		for i := t.tree.cPos[l]; i < tail; i++ {
			if h := c.Get(i); h != nil {
				var value []byte
				value = h.(*[sha256.Size]byte)[:]
				if l == 0 {
					value = t.tree.rcache.Get(i).([]byte)
				}
				memnode := &schema.Node{}
				memhash, memrefk, _ := decodeRefTreeKey(value)

				memnode.I = treeKey(uint8(l), i)
				memnode.H = memhash[:]
				memnode.Refk = memrefk
				memnode.Cache = true
				if len(memrefk) > 0 {
					memnode.Ref = true
				}
				memlayer.L = append(memlayer.L, memnode)
			}
		}
		if len(memlayer.L) > 0 {
			memtree.T = append(memtree.T, memlayer)
		}
	}

	// Merging disk and cache tree
	fulltree := &schema.Tree{}
	if len(disktree.T) > 0 && len(memtree.T) > 0 {
		for _, diskLayer := range disktree.T {
			fulllayer := &schema.Layer{}
			for _, node := range diskLayer.L {
				fulllayer.L = append(fulllayer.L, node)
			}
			fulltree.T = append(fulltree.T, fulllayer)
		}
		for _, memLayer := range memtree.T {
			var lvlb = make([]byte, 1)
			// here extract layer from first key found
			copy(lvlb, memLayer.L[0].I[1:2])
			if uint8(len(fulltree.T)-1) >= lvlb[0] {
				for _, node := range memLayer.L {
					// if node already present in fulltree.T[lvlb[0]].L is not frozen and overwrite is needed
					var found = false
					var replaceId uint64
					var inspectNode *schema.Node
					var k int
					for k, inspectNode = range fulltree.T[lvlb[0]].L {
						// if node index is presents in fulltree.T[lvlb[0]].L nodes we found not frozen index
						if bytes.Compare(node.I, inspectNode.I) == 0 {
							found = true
							replaceId = uint64(k)
							break
						}
					}

					if found {
						fulltree.T[lvlb[0]].L[replaceId] = node
					} else {
						fulltree.T[lvlb[0]].L = append(fulltree.T[lvlb[0]].L, node)
					}
				}
			} else {
				//If mem layer is not present  in  the disk tree create create new one
				fulllayer := &schema.Layer{}
				for _, node := range memLayer.L {
					fulllayer.L = append(fulllayer.L, node)
				}
				fulltree.T = append(fulltree.T, fulllayer)
			}
		}
	}

	if len(fulltree.T) > 0 {
		fulltree.T[len(fulltree.T)-1].L[0].Root = true
		return fulltree
	}
	if len(disktree.T) > 0 {
		disktree.T[len(disktree.T)-1].L[0].Root = true
		return disktree
	}
	if len(memtree.T) > 0 {
		memtree.T[len(memtree.T)-1].L[0].Root = true
		return memtree
	}

	return &schema.Tree{}
}
