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
	h := store.Get(uint8(Depth(store)), 0)
	if h == nil {
		return sha256.Sum256(nil)
	}
	return *h
}

// LeafHash computes the leaf's hash of the given content b.
func LeafHash(b []byte) [sha256.Size]byte {
	return sha256.Sum256(append([]byte{LeafPrefix}, b...))
}

// Append computes the hash of the given content b, appends it to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
func Append(store Storer, b []byte) {
	h := LeafHash(b)
	AppendHash(store, &h)
}

// AppendHash appends the given hash _h_ to the next free slot in the tree,
// then incrementally builds the intermediate nodes up to the root.
// AppendHash re-uses _h_ internally, as side-effect the new root will be set to _h_.
func AppendHash(store Storer, h *[sha256.Size]byte) {

	// append the leaf
	l := store.Width()
	store.Set(0, l, *h)
	l++

	// build up to the root
	d := uint8(0)
	c := [sha256.Size*2 + 1]byte{NodePrefix}
	for l > 1 {
		if l%2 == 0 {
			copy(c[1:sha256.Size+1], store.Get(d, l-2)[:])
			copy(c[sha256.Size+1:], h[:])
			(*h) = sha256.Sum256(c[:])

			d++
			l >>= 1
			store.Set(d, l-1, *h)
		} else {
			// skip empty nodes
			// todo(leogr): multiple nodes could be skipped when (l-1) is power of 2
			l++
			d++
			l >>= 1
		}
	}

}

// IsFrozen returns true when the node (_layer_, _index_) in a tree of width = (_at_ + 1) is frozen, otherwise false.
// Once a given subtree in the node has no more slots, the hash for the root node of that subtree is frozen
// (i.e., will not change as future nodes are added).
// In a tree with position _at_ (i.e. width = _at_ + 1), node (_layer_, _index_) is frozen
func IsFrozen(layer uint8, index, at uint64) bool {
	a := uint64(1) << layer
	return at >= index*a+a-1
}

// Path is a list of additional nodes required for proving inclusion or consistency.
type Path [][sha256.Size]byte

// ToSlice returns a copy of _Path_ content as slice of byte slices.
func (p Path) ToSlice() [][]byte {
	out := make([][]byte, len(p))
	for i, h := range p {
		out[i] = append(out[i], h[:]...)
	}
	return out
}

// FromSlice sets _Path_ from the give _slice_.
func (p *Path) FromSlice(slice [][]byte) {
	pp := make([][sha256.Size]byte, len(slice))
	for i, h := range slice {
		copy(pp[i][:], h)
	}
	*p = Path(pp)
}

func mth(store Storer, l, r uint64) *[sha256.Size]byte {
	n := r - l
	if n == 0 {
		return store.Get(0, r)
	}

	k := uint64(1) << (bits.Len64(n) - 1)

	c := [sha256.Size*2 + 1]byte{NodePrefix}
	copy(c[1:sha256.Size+1], mth(store, l, l+k-1)[:]) //MTH(D[0:k])
	copy(c[sha256.Size+1:], mth(store, l+k, r)[:])    //MTH(D[k:n])
	h := sha256.Sum256(c[:])
	return &h
}

func mthPosition(l, r uint64) (layer uint8, index uint64) {

	d := (bits.Len64(r - l))
	k := uint64(1) << d

	index = l / k
	layer = uint8(d)
	return
}

// InclusionProof returns the shortest list of additional nodes required to compute the root (i.e., MTH) from the (_i_+1)th leaf,
// assuming the (sub-)tree constructed up to the (_at_+1)th leaf stored into a given _store_.
func InclusionProof(store Storer, at, i uint64) (p Path) {

	w := store.Width()
	if i > at || at >= w || at < 1 {
		return
	}

	m := i
	n := at + 1

	offset := uint64(0)
	l := uint64(0)
	r := uint64(0)
	for {
		d := (bits.Len64(n - 1))
		k := uint64(1) << (d - 1)
		if m < k {
			l, r = offset+k, offset+n-1
			n = k
		} else {
			l, r = offset, offset+k-1
			m = m - k
			n = n - k
			offset += k
		}

		layer, index := mthPosition(l, r)
		// fmt.Printf("%d) [%d,%d] -> (%d, %d)\n", len(p), l, r, layer, index)
		if IsFrozen(layer, index, at) {
			p = append(Path{*store.Get(layer, index)}, p...)
		} else {
			p = append(Path{*mth(store, l, r)}, p...)
		}

		if n < 1 || (n == 1 && m == 0) {
			return
		}
	}
}

// VerifyInclusion returns true when the Path _p_ proves that the given _leaf_ is the (_i_+1)th leaf
// of the tree defined by _root_ and width = (_at_ + 1), otherwise false.
//
// VerifyConsistency assumes Path _p_ has been generated by using InclusionProof func.
func (p Path) VerifyInclusion(at, i uint64, root, leaf [sha256.Size]byte) bool {

	if i > at || (at > 0 && len(p) == 0) {
		return false
	}

	h := leaf
	for _, v := range p {

		c := [sha256.Size*2 + 1]byte{NodePrefix}
		if i%2 == 0 && i != at {
			copy(c[1:], h[:])
			copy(c[sha256.Size+1:], v[:])
		} else {
			copy(c[1:], v[:])
			copy(c[sha256.Size+1:], h[:])
		}
		h = sha256.Sum256(c[:])
		i /= 2
		at /= 2
	}

	return at == i && h == root
}

// ConsistencyProof returns the list of nodes required to verify that the first (_i_+1) inputs are equal in both
// (sub-)trees constructed up to the (_i_+1)th leaf and the (_at_+1)th leaf stored into the given _store_.
// The number of nodes in the resulting proof is bounded above ceil(log2(at+1))+1.
func ConsistencyProof(store Storer, at, i uint64) (p Path) {
	m := i + 1
	n := at + 1
	if !(0 < m && m < n) {
		return
	}

	if w := store.Width(); n > w {
		return
	}

	b := true
	offset := uint64(0)
	l := uint64(0)
	r := uint64(0)
	for {
		d := (bits.Len64(n - 1))
		k := uint64(1) << (d - 1)
		if m <= k {
			l, r = offset+k, offset+n-1
			n = k
		} else {
			l, r = offset, offset+k-1
			m = m - k
			n = n - k
			offset += k
			b = false
		}

		layer, index := mthPosition(l, r)
		if IsFrozen(layer, index, at) {
			p = append(Path{*store.Get(layer, index)}, p...)
		} else {
			p = append(Path{*mth(store, l, r)}, p...)
		}

		if m == n {
			if !b {
				p = append(Path{*mth(store, offset, offset+n-1)}, p...)
			}
			return
		}
	}
}

func isPowerOfTwo(x uint64) bool {
	return (x != 0) && ((x & (x - 1)) == 0)
}

// VerifyConsistency returns true when the Path _p_ proves that the first (_first_+1) inputs are equal in both
// (sub-)trees described by _firtsHash_ and _secondHash_, respectively constructed up to the (_first_+1)th leaf
// and the (_second_+1)th leaf, otherwise false.
//
// VerifyConsistency assumes Path _p_ has been generated by using ConsistencyProof func.
func (p Path) VerifyConsistency(second, first uint64, secondHash, firstHash [sha256.Size]byte) bool {

	l := len(p)
	if first == second && firstHash == secondHash && l == 0 {
		return true
	}

	if !(first < second) || l == 0 {
		return false
	}

	var pp Path
	if isPowerOfTwo(first + 1) {
		pp = append(Path{firstHash}, p...)
	} else {
		pp = p
	}

	fn := first
	sn := second

	for fn%2 == 1 {
		fn >>= 1
		sn >>= 1
	}

	fr, sr := pp[0], pp[0]

	tmp := [sha256.Size*2 + 1]byte{NodePrefix}
	for step, c := range pp {
		if step == 0 {
			continue
		}
		if sn == 0 {
			return false
		}
		if fn%2 == 1 || fn == sn {
			copy(tmp[1:], c[:])

			copy(tmp[sha256.Size+1:], fr[:])
			fr = sha256.Sum256(tmp[:])

			copy(tmp[sha256.Size+1:], sr[:])
			sr = sha256.Sum256(tmp[:])

			for fn%2 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			copy(tmp[1:], sr[:])
			copy(tmp[sha256.Size+1:], c[:])
			sr = sha256.Sum256(tmp[:])
		}
		fn >>= 1
		sn >>= 1
	}

	return fr == firstHash && sr == secondHash && sn == 0
}
