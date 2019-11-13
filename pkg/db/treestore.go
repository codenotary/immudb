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

	"github.com/codenotary/immudb/pkg/tree"

	"github.com/dgraph-io/badger/v2"
)

var tsL0Prefix = []byte{
	0x00,
}

var tsL0UpLimit = []byte{
	0x00,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

func treeKey(layer uint8, index uint64) []byte {
	k := make([]byte, 1+8, 1+8)
	k[0] = layer
	binary.BigEndian.PutUint64(k[1:], index)
	return k
}

func decodeTreeKey(k []byte) (layer uint8, index uint64) {
	layer = k[0]
	index = binary.BigEndian.Uint64(k[1:])
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
		return binary.BigEndian.Uint64(k[1:])
	}
	return 0
}

type treeStore struct {
	sync.Mutex
	db     *badger.DB
	w      uint64
	cache  map[string]*[sha256.Size]byte
	cSize  uint64
	cCount uint64
}

func newTreeStore(db *badger.DB, cacheSize uint64) *treeStore {
	t := &treeStore{
		db:    db,
		cache: make(map[string]*[sha256.Size]byte, cacheSize),
		cSize: cacheSize,
	}
	db.View(func(txn *badger.Txn) error {
		t.w = treeWidth(txn)
		return nil
	})
	return t
}

func (t *treeStore) flush() {
	wb := t.db.NewWriteBatchAt(t.w)
	defer wb.Flush()

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
	t.cCount = uint64(len(t.cache))
}

func (t *treeStore) Width() uint64 {
	return t.w
}

func (t *treeStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	key := string(treeKey(layer, index))
	t.cache[key] = &value
	if layer == 0 && t.w <= index {
		t.w = index + 1
	}
	t.cCount++
	if t.cCount >= t.cSize {
		t.flush()
	}
}

func (t *treeStore) Get(layer uint8, index uint64) *[sha256.Size]byte {
	key := treeKey(layer, index)
	if v, ok := t.cache[string(key)]; ok {
		return v
	}

	var ret [sha256.Size]byte
	t.db.View(func(txn *badger.Txn) error {
		// fmt.Printf("CACHE MISS (w=%d, d=%d): [%d,%d]\n", t.w, tree.Depth(t), layer, index)
		item, err := txn.Get(treeKey(layer, index))
		if err != nil {
			return nil
		}
		item.Value(func(val []byte) error {
			if val != nil {
				copy(ret[:], val)
				t.cache[string(key)] = &ret
			}
			return nil
		})
		return nil
	})

	t.cache[string(key)] = &ret
	return &ret
}
