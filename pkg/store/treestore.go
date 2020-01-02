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

package store

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/codenotary/immudb/pkg/ring"
	"github.com/codenotary/immudb/pkg/tree"

	"github.com/dgraph-io/badger/v2"
)

const tsPrefix = byte(0)
const bitTreeEntry = byte(255)

func treeKey(layer uint8, index uint64) []byte {
	k := make([]byte, 1+1+8, 1+1+8)
	k[0] = tsPrefix
	k[1] = layer
	binary.BigEndian.PutUint64(k[2:], index)
	return k
}

func decodeTreeKey(k []byte) (layer uint8, index uint64) {
	layer = k[1]
	index = binary.BigEndian.Uint64(k[2:])
	return
}

func treeLayerWidth(layer uint8, txn *badger.Txn) uint64 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	maxKey := []byte{tsPrefix, layer, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for it.Seek(maxKey); it.ValidForPrefix(maxKey[:2]); it.Next() {
		return binary.BigEndian.Uint64(it.Item().Key()[2:]) + 1
	}
	return 0
}

type treeStoreEntry struct {
	ts uint64
	h  *[sha256.Size]byte
}

type treeStore struct {
	// A 64-bit integer must be at the top for memory alignment
	ts          uint64 // badger timestamp
	w           uint64 // width of computed tree
	c           chan *treeStoreEntry
	quit        chan struct{}
	lastFlushed uint64
	db          *badger.DB
	log         logger.Logger
	caches      [256]ring.Buffer
	cPos        [256]uint64
	cSize       uint64
	sync.RWMutex
	closeOnce sync.Once
}

func newTreeStore(db *badger.DB, cacheSize uint64, log logger.Logger) *treeStore {

	t := &treeStore{
		db:     db,
		log:    log,
		c:      make(chan *treeStoreEntry, cacheSize),
		quit:   make(chan struct{}, 0),
		caches: [256]ring.Buffer{},
		cPos:   [256]uint64{},
		cSize:  cacheSize,
	}

	t.makeCaches()

	// load tree state
	db.View(func(txn *badger.Txn) error {
		for l := 0; l < 256; l++ {
			w := treeLayerWidth(uint8(l), txn)
			if w == 0 {
				break
			}
			t.cPos[l] = w
		}
		t.w = t.cPos[0]
		t.ts = t.w

		return nil
	})

	go t.worker()

	t.log.Infof("Tree of width %d ready with root %x", t.w, tree.Root(t))
	return t
}

func (t *treeStore) makeCaches() {
	size := t.cSize + 2
	if size < 64 {
		size = 64
	}
	for i := 0; i < 256; i++ {
		t.caches[i] = ring.NewRingBuffer(size)
		if size > 64 {
			size /= 2
		} else {
			size = 64
		}
	}
}

// Close closes a treeStore. All pending items will be processed and flushed.
// Calling treeStore.Close() multiple times would still only close the treeStore once.
func (t *treeStore) Close() {
	t.closeOnce.Do(func() {
		if t.quit != nil {
			close(t.c)
			<-t.quit
			t.quit = nil
			t.log.Infof("Tree of width %d closed with root %x", t.w, tree.Root(t))
		}
	})
}

// WaitUntil waits until the given _index_ has been added into the tree.
// If the given _index_ cannot be reached, it will never return.
func (t *treeStore) WaitUntil(index uint64) {
	for {
		t.RLock()
		if t.w >= index+1 {
			t.RUnlock()
			return
		}
		t.RUnlock()
		time.Sleep(time.Microsecond)
	}
}

// NewEntry acquires a lease for a new entry and returns it. The entry must be used with Commit() or Discard().
// It's thread-safe.
func (t *treeStore) NewEntry(key []byte, value []byte) *treeStoreEntry {
	ts := atomic.AddUint64(&t.ts, 1)
	h := api.Digest(ts-1, key, value)
	return &treeStoreEntry{
		ts: ts,
		h:  &h,
	}
}

// NewBatch is similar to NewEntry but accept a slice of key-value pairs.
// It's thread-safe.
func (t *treeStore) NewBatch(kvPairs []KVPair) []*treeStoreEntry {
	size := uint64(len(kvPairs))
	batch := make([]*treeStoreEntry, 0, size)
	lease := atomic.AddUint64(&t.ts, size)
	for i, kv := range kvPairs {
		ts := lease - size + uint64(i) + 1
		h := api.Digest(ts-1, kv.Key, kv.Value)
		batch = append(batch, &treeStoreEntry{ts, &h})
	}
	return batch
}

// Commit enqueues the given entry to be included in the tree.
// It's thread-safe. Commit will fail if called after Close().
func (t *treeStore) Commit(entry *treeStoreEntry) {
	t.c <- entry
}

// Discard enqueues the given entry to be included in the tree as discarded item.
// It's thread-safe. Discard will fail if called after Close().
func (t *treeStore) Discard(entry *treeStoreEntry) {
	h := api.Digest(entry.ts, []byte{}, []byte{})
	entry.h = &h
	t.c <- entry
}

func (t *treeStore) worker() {
	pq := make(treeStorePQ, 0, t.cSize)
	for item := range t.c {
		heap.Push(&pq, item)

		t.Lock()
		for min := pq.Min(); min == t.w+1; min = pq.Min() {
			tree.AppendHash(t, heap.Pop(&pq).(*treeStoreEntry).h)
			if t.w%2 == 0 && (t.w-t.lastFlushed) >= t.cSize/2 {
				t.flush()
			}
		}
		t.Unlock()
	}

	if t.w > 0 {
		t.Lock()
		t.flush()
		t.Unlock()
	}
	t.quit <- struct{}{}
}

// flush should be only called when the tree is in a consistent state and _t_ is locked.
// It always flushes the last portion (ie. items not yet flushed) of buffers in batch,
// in case of failure previous stored state will be preserved and cache indexes will be not advanced.
func (t *treeStore) flush() {
	t.log.Infof("Flushing tree caches at index %d", t.w-1)
	cancel := false
	var wb *badger.WriteBatch
	wb = t.db.NewWriteBatchAt(t.w)
	defer func() {
		if cancel {
			wb.Cancel()
			return
		}
		if err := wb.Flush(); err != nil {
			t.log.Errorf("Tree flush error: %s", err)
		} else {
			// advance cache indexes iff flushing has succeeded
			for l, c := range t.caches {
				t.cPos[l] = c.Tail()
			}
			t.lastFlushed = t.w
		}
	}()

	for l, c := range t.caches {
		tail := c.Tail()
		if tail == 0 {
			continue
		}

		// fmt.Printf("Flushing [l=%d, head=%d, tail=%d] from %d to (%d-1)\n", l, c.Head(), c.Tail(), t.cPos[l], tail)
		for i := t.cPos[l]; i < tail; i++ {
			if h := c.Get(i); h != nil {
				// fmt.Printf("Storing [l=%d, i=%d]\n", l, i)
				entry := badger.Entry{
					Key:      treeKey(uint8(l), i),
					Value:    h.(*[sha256.Size]byte)[:],
					UserMeta: bitTreeEntry,
				}
				// it's safe to discard, only non-frozen nodes could be overwritten
				entry.WithDiscard()

				if err := wb.SetEntry(&entry); err != nil {
					// fixme(leogr): that should never happen, use panic() ?
					t.log.Errorf("Cannot flush tree item (l=%d, i=%d): %s", err)
					t.log.Warningf("Tree flush cancelled")
					cancel = true
					return
				}
			}
		}
	}
}

func (t *treeStore) Width() uint64 {
	return t.w
}

func (t *treeStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	t.caches[layer].Set(index, &value)

	if layer == 0 && t.w <= index {
		t.w = index + 1
	}
}

func (t *treeStore) Get(layer uint8, index uint64) *[sha256.Size]byte {

	if v := t.caches[layer].Get(index); v != nil {
		return v.(*[sha256.Size]byte)
	}

	var ret [sha256.Size]byte
	if err := t.db.View(func(txn *badger.Txn) error {
		// fmt.Printf("CACHE MISS (ts=%d, w=%d, d=%d): [%d,%d]\n", t.ts, t.w, tree.Depth(t), layer, index)
		item, err := txn.Get(treeKey(layer, index))
		if err != nil {
			return err
		}
		if _, err = item.ValueCopy(ret[:]); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil
	}

	return &ret
}
