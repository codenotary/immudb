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

package tree

import (
	"crypto/sha256"
	"math/bits"
)

// Prefixes for leaves and nodes
const (
	LeafPrefix = byte(0)
	NodePrefix = byte(1)
)

// Depth returns the length of the path from leaves to the root.
// When the tree is empty then -1 is returned.
func Depth(store Storer) int {
	w := store.Width()
	if w == 0 {
		return -1
	}
	return bits.Len64(uint64(w - 1))
}

// Root returns the root hash of the tree. Panic if store is empty.
func Root(store Storer) [sha256.Size]byte {
	return *store.Get(uint8(Depth(store)), 0)
}

// LeafHash computes the leaf's hash of the given content b.
func LeafHash(b []byte) [sha256.Size]byte {
	return sha256.Sum256(append([]byte{LeafPrefix}, b...))
}

// Append computes the hash of the given content b, appends it to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
func Append(store Storer, b []byte) {
	AppendHash(store, LeafHash(b))
}

// AppendHash appends the given hash to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
func AppendHash(store Storer, h [sha256.Size]byte) {

	// append the leaf
	l := store.Width()
	store.Set(0, l, h)

	newDepth := uint8(bits.Len64(l))
	l++

	// build up to the root
	d := uint8(0)
	c := [sha256.Size*2 + 1]byte{NodePrefix}
	for d < newDepth {
		if l%2 == 0 {
			copy(c[1:sha256.Size+1], store.Get(d, l-2)[:])
			copy(c[sha256.Size+1:], h[:])
			h = sha256.Sum256(c[:])

			d++
			l = l / 2
			store.Set(d, l-1, h)
		} else {
			// skip empty nodes
			l++
			d++
			l = l / 2
		}
	}
}

func IsFrozen(layer uint8, index, at uint64) bool {
	a := uint64(1) << layer
	return at >= index*a+a-1
}
