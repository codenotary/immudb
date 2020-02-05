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

package schema

import (
	"bytes"
	"crypto/sha256"

	"github.com/codenotary/immudb/pkg/tree"
)

// Verify returns true iff the _InclusionProof_ proves that _leaf_ is included into _i.Root_'s history at
// the given _index_.
// todo(leogr): can we use schema.Item instead of (index,leaf)?
func (i *InclusionProof) Verify(index uint64, leaf []byte) bool {

	if i == nil || i.Index != index || bytes.Compare(leaf, i.Leaf) != 0 {
		return false
	}

	var path tree.Path
	path.FromSlice(i.Path)

	var rt, lf [sha256.Size]byte
	copy(rt[:], i.Root)
	copy(lf[:], i.Leaf)
	return path.VerifyInclusion(i.At, i.Index, rt, lf)
}

// Verify returns true iff the _ConsistencyProof_ proves that _c.SecondRoot_'s history is including the history of
// the provided _prevRoot_ up to the position _c.First_.
func (c *ConsistencyProof) Verify(prevRoot Root) bool {
	if c == nil || c.First != prevRoot.Index {
		return false
	}

	var path tree.Path
	path.FromSlice(c.Path)

	var firstRoot, secondRoot [sha256.Size]byte
	copy(firstRoot[:], prevRoot.Root)
	copy(secondRoot[:], c.SecondRoot)
	if path.VerifyConsistency(c.Second, c.First, secondRoot, firstRoot) {
		c.FirstRoot = prevRoot.Root
		return true
	}
	return false
}

// Verify returns true iff the _Proof_ proves that the given _leaf_ is included into _p.Root_'s history at position _p.Index_
// and that the provided _prevRoot_ is included into _p.Root_'s history.
// Providing a zerovalue for _prevRoot_ signals that no previous root is available because _leaf_ is the first leaf in the tree.
func (p *Proof) Verify(leaf []byte, prevRoot Root) bool {

	if p == nil || bytes.Compare(leaf, p.Leaf) != 0 {
		return false
	}

	var path tree.Path

	path.FromSlice(p.InclusionPath)
	var rt, lf [sha256.Size]byte
	copy(rt[:], p.Root)
	copy(lf[:], p.Leaf)
	if !path.VerifyInclusion(p.At, p.Index, rt, lf) {
		return false
	}

	// we cannot check consistency when the previous root does not exists
	if p.At == 0 && p.Index == 0 && len(p.ConsistencyPath) == 0 && prevRoot.Index == 0 && len(prevRoot.Root) == 0 {
		return true
	}

	path.FromSlice(p.ConsistencyPath)

	var firstRoot, secondRoot [sha256.Size]byte
	copy(firstRoot[:], prevRoot.Root)
	copy(secondRoot[:], p.Root)
	return path.VerifyConsistency(p.At, prevRoot.Index, secondRoot, firstRoot)
}
