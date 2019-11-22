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
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/pkg/ring"
	"github.com/codenotary/immudb/pkg/tree"

	"github.com/dgraph-io/badger/v2"
)

var tsPrefix = byte('_')

var tsL0Prefix = []byte{
	tsPrefix,
	0x00,
}

var tsL0UpLimit = []byte{
	tsPrefix,
	0x00,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

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

func treeWidth(txn *badger.Txn) uint64 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(tsL0UpLimit); it.ValidForPrefix(tsL0Prefix); it.Next() {
		k := it.Item().Key()
		return binary.BigEndian.Uint64(k[2:])
	}
	return 0
}

type treeStore struct {
	// A 64-bit integer must be at the top for memory alignment
	ts          uint64 // badger timestamp
	w           uint64 // width of computed tree
	c           chan *[sha256.Size]byte
	quit        chan struct{}
	lastFlushed uint64
	db          *badger.DB
	caches      [256]ring.Buffer
	cPos        [256]uint64
	cSize       uint64
	sync.RWMutex
}

func newTreeStore(db *badger.DB, cacheSize uint64) *treeStore {

	t := &treeStore{
		db:     db,
		c:      make(chan *[sha256.Size]byte, cacheSize),
		quit:   make(chan struct{}, 0),
		caches: [256]ring.Buffer{},
		cPos:   [256]uint64{},
		cSize:  cacheSize,
	}

	t.resetCache()

	db.View(func(txn *badger.Txn) error {
		t.w = treeWidth(txn)
		t.ts = t.w
		return nil
	})
	go t.worker()
	return t
}

func (t *treeStore) Close() {
	if t.quit != nil {
		close(t.c)
		<-t.quit
		t.quit = nil
	}
}

func (t *treeStore) resetCache() {
	for i := 0; i < 256; i++ {
		t.caches[i] = ring.NewRingBuffer(t.cSize)
	}
}

func (t *treeStore) Add(h *[sha256.Size]byte) uint64 {
	t.c <- h
	return atomic.AddUint64(&t.ts, 1)
}

func (t *treeStore) worker() {
	for h := range t.c {
		tree.AppendHash(t, h)
		if t.w%2 == 0 && (t.w-t.lastFlushed) >= t.cSize/2 {
			t.flush()
		}
	}
	if t.w > 0 {
		t.flush()
	}
	t.quit <- struct{}{}
}

func (t *treeStore) flush() {
	// fmt.Println("FLUST at ", t.w)
	var wb *badger.WriteBatch
	wb = t.db.NewWriteBatchAt(t.w)
	defer wb.Flush()

	for l, c := range t.caches {
		tail := c.Tail()
		if tail == 0 {
			continue
		}

		// fmt.Printf("Flushing [l=%d, head=%d, tail=%d] from %d to (%d-1)\n", l, c.Head(), c.Tail(), t.cPos[l], tail)

		for i := t.cPos[l]; i < tail; i++ {
			wb.Set(treeKey(uint8(l), i), c.Get(i).(*[sha256.Size]byte)[:])
		}
		t.cPos[l] = tail
	}

	t.lastFlushed = t.w
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
	t.db.View(func(txn *badger.Txn) error {
		// fmt.Printf("CACHE MISS (ts=%d, w=%d, d=%d): [%d,%d]\n", t.ts, t.w, tree.Depth(t), layer, index)
		item, err := txn.Get(treeKey(layer, index))
		if err != nil {
			return nil
		}
		item.Value(func(val []byte) error {
			if val != nil {
				copy(ret[:], val)
				// fmt.Printf("CACHE FALLBACK (ts=%d, w=%d, d=%d): [%d,%d]\n", t.ts, t.w, tree.Depth(t), layer, index)
			}
			return nil
		})
		return nil
	})

	return &ret
}
