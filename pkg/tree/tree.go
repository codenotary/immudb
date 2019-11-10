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

const EmptySlot = byte(0)
const LeafPrefix = byte(0)
const NodePrefix = byte(1)

type Tree struct {
	store Storer
}

func New(store Storer) *Tree {
	return &Tree{
		store: store,
	}
}

// N returns the index of the last item stored in the tree.
// When the tree is empty then -1 is returned.
func (t *Tree) N() int {
	return t.store.Width() - 1
}

// Depth returns the length of the path from leaves to the root.
// When the tree is empty then -1 is returned.
func (t *Tree) Depth() int {
	w := t.store.Width()
	if w == 0 {
		return -1
	}
	return bits.Len64(uint64(w - 1))
}

// Root returns the root hash of the tree.
func (t *Tree) Root() [sha256.Size]byte {
	return *t.store.Get(t.Depth(), 0)
}

// Add computes the hash of the given content b, appends it to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
func (t *Tree) Add(b []byte) error {

	l := t.store.Width()

	// append the item's hash
	h := sha256.Sum256(append([]byte{LeafPrefix}, b...))
	t.store.Set(0, l, h)
	l++

	// build up to the root
	newDepth := bits.Len64(uint64(l - 1))
	d := 0
	for d < newDepth {
		// compute intermediate node
		c := [sha256.Size*2 + 1]byte{NodePrefix}
		var h [sha256.Size]byte

		if l%2 == 0 {
			copy(c[1:sha256.Size+1], t.store.Get(d, l-2)[:])
			copy(c[sha256.Size+1:], t.store.Get(d, l-1)[:])
			h = sha256.Sum256(c[:])
		} else {
			copy(c[1:sha256.Size+1], t.store.Get(d, l-1)[:])
			c[sha256.Size+2] = EmptySlot
			h = sha256.Sum256(c[:sha256.Size+2])
			l++
		}

		d++
		l = l / 2
		t.store.Set(d, l-1, h)
	}

	return nil
}
