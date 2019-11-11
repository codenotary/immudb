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

	"github.com/codenotary/immudb/pkg/tree"
	"github.com/dgraph-io/badger/v2"
)

const uintBytes = 8

var tsKey = []byte{reservedPrefix, 'T'}
var tsKeyLen = len(tsKey)

var tsL0Prefix = []byte{
	reservedPrefix, 'T',
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

var tsL0Upper = []byte{
	reservedPrefix, 'T',
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

func treeNodeKey(layer, index int) []byte {
	v := make([]byte, tsKeyLen+uintBytes*2, tsKeyLen+uintBytes*2)
	copy(v[0:], tsKey)
	binary.LittleEndian.PutUint64(v[tsKeyLen:], uint64(layer))
	binary.LittleEndian.PutUint64(v[tsKeyLen+uintBytes:], uint64(index))
	return v
}

type treeStore struct {
	txn    *badger.Txn
	w      int
	wCache bool
}

func NewTreeStore(txn *badger.Txn) (tree.Storer, error) {
	return &treeStore{
		txn: txn,
	}, nil
}

func (t *treeStore) Width() int {
	if t.wCache {
		return t.w
	}
	t.wCache = true
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := t.txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(tsL0Upper); it.ValidForPrefix(tsL0Prefix); it.Next() {
		k := it.Item().Key()
		return int(binary.LittleEndian.Uint64(k[tsKeyLen+uintBytes:])) + 1
	}
	return 0
}

func (t *treeStore) Set(layer, index int, value [sha256.Size]byte) {
	err := t.txn.Set(treeNodeKey(layer, index), value[:])
	if err != nil {
		if t.wCache && layer == 0 && t.w <= index {
			t.w = index + 1
		}
	}
}

func (t *treeStore) Get(layer, index int) *[sha256.Size]byte {
	item, err := t.txn.Get(treeNodeKey(layer, index))
	if err != nil {
		return nil
	}

	var r []byte
	item.Value(func(val []byte) error {
		r = val
		return nil
	})
	if r == nil {
		return nil
	}

	var ret [sha256.Size]byte
	copy(ret[:], r)
	return &ret
}
