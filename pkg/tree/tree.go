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
	data [][][sha256.Size]byte
}

func New() *Tree {
	return &Tree{
		data: make([][][sha256.Size]byte, 1),
	}
}

// N returns the index of the last item stored in the tree.
// When the tree is empty then -1 is returned.
func (t *Tree) N() uint64 {
	return uint64(len(t.data[0])) - 1
}

// Depth returns the length of the path from leaves to the root.
// When the tree is empty then -1 is returned.
func (t *Tree) Depth() uint64 {
	return uint64(len(t.data)) - 1
}

// Root returns the root hash of the tree.
func (t *Tree) Root() [sha256.Size]byte {
	return t.data[t.Depth()][0]
}

// Add computes the hash of the given content b, appends it to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
func (t *Tree) Add(b []byte) error {

	// append the item's hash
	h := sha256.Sum256(append([]byte{LeafPrefix}, b...))
	t.data[0] = append(t.data[0], h)

	// add a new layer, if needed
	d := bits.Len64(uint64(len(t.data[0]) - 1)) // == int(math.Ceil(math.Log2(float64(len(t.data[0])))))
	if d == len(t.data) {
		t.data = append(t.data, make([][sha256.Size]byte, 0, 256*256)) // todo(leogr): optmize pre-allocated capacity
	}

	return t.buildUp(0)
}

// buildUp is a helper function that for a given level d, constructs and sets
// the rightmost branch from latest node at level d up to the root.
// Assuming nodes are added one by one, then calling buidUp(0) after appending
// a leaf is enough to incrementally update the tree to the new state.
func (t *Tree) buildUp(d int) error {
	for depth := len(t.data) - 1; d < depth; {
		// compute intermediate node

		l := len(t.data[d])
		c := [sha256.Size*2 + 1]byte{NodePrefix}
		var h [sha256.Size]byte

		if l%2 == 0 {
			copy(c[1:sha256.Size+1], t.data[d][l-2][:])
			copy(c[sha256.Size+1:], t.data[d][l-1][:])
			h = sha256.Sum256(c[:])
		} else {
			copy(c[1:sha256.Size+1], t.data[d][l-1][:])
			c[sha256.Size+2] = EmptySlot
			h = sha256.Sum256(c[:sha256.Size+2])
			l++
		}

		d++
		nl := len(t.data[d])
		if nl*2 < l {
			t.data[d] = append(t.data[d], h)
		} else {
			t.data[d][nl-1] = h
		}
	}

	return nil
}
