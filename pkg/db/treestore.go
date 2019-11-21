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
	"time"

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
	at   uint64
	c    chan *[sha256.Size]byte
	quit chan struct{}
	w    uint64
	db   *badger.DB
	// cache  *ristretto.Cache
	cache map[string]*[sha256.Size]byte
	cSize uint64
	sync.RWMutex
}

func newTreeStore(db *badger.DB, cacheSize uint64) *treeStore {

	t := &treeStore{
		db:    db,
		c:     make(chan *[sha256.Size]byte, cacheSize),
		quit:  make(chan struct{}, 0),
		cache: make(map[string]*[sha256.Size]byte, cacheSize*2),
		cSize: cacheSize,
	}
	db.View(func(txn *badger.Txn) error {
		t.at = treeWidth(txn)
		t.w = t.at
		return nil
	})
	go t.worker()
	return t
}

func (t *treeStore) Close() {
	if t.quit != nil {
		t.quit <- struct{}{}
		close(t.c)
		for h := range t.c {
			tree.AppendHash(t, h)
		}
		t.flush()
	}
}

func (t *treeStore) Add(h *[sha256.Size]byte) uint64 {
	t.c <- h
	return atomic.AddUint64(&t.at, 1)
}

func (t *treeStore) worker() {
	for {
		select {
		case h := <-t.c:
			t.Lock()
			tree.AppendHash(t, h)
			t.Unlock()
		case <-t.quit:
			close(t.quit)
			t.quit = nil
			return
		default:
			t.Lock()
			t.flush()
			t.Unlock()
			time.Sleep(time.Second * 5)
		}
	}
}

func (t *treeStore) flush() {

	if len(t.cache) < int(t.cSize/3*2) {
		return
	}

	wb := t.db.NewWriteBatchAt(t.w)

	oldCache := t.cache
	t.cache = make(map[string]*[sha256.Size]byte, t.cSize)

	d := tree.Depth(t) + 1
	limits := make([]uint64, d, d)
	// fmt.Printf("FLUST START (w=%d, d=%d)\n", t.w, d)
	limit := t.w - 1
	for i := 0; i < d; i++ {
		limits[i] = limit - limit%2
		limit /= 2
		// fmt.Printf("FROZEN (l=%d): %d\n", i, limits[i])
	}

	for k, v := range oldCache {
		l, i := decodeTreeKey([]byte(k))

		if i < limits[l] {
			wb.Set([]byte(k), v[:])
		} else {
			t.cache[k] = v
		}
	}

	wb.Flush()

}

func (t *treeStore) Width() uint64 {
	return t.w
}

func (t *treeStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	// fmt.Printf("SET (at=%d): [%d,%d]\n", t.at, layer, index)
	key := string(treeKey(layer, index))
	t.cache[key] = &value
	if layer == 0 && t.w <= index {
		t.w = index + 1
	}
}

func (t *treeStore) Get(layer uint8, index uint64) *[sha256.Size]byte {
	key := treeKey(layer, index)
	if v, ok := t.cache[string(key)]; ok {
		return v
	}

	var ret [sha256.Size]byte
	t.db.View(func(txn *badger.Txn) error {
		// fmt.Printf("CACHE MISS (at=%d, d=%d): [%d,%d]\n", t.at, tree.Depth(t), layer, index)
		item, err := txn.Get(treeKey(layer, index))
		if err != nil {
			return nil
		}
		item.Value(func(val []byte) error {
			if val != nil {
				copy(ret[:], val)
				t.cache[string(key)] = &ret
				// fmt.Printf("FALLBACK (at=%d, d=%d): [%d,%d]\n", t.at, tree.Depth(t), layer, index)
			}
			return nil
		})
		return nil
	})

	t.cache[string(key)] = &ret
	return &ret
}
