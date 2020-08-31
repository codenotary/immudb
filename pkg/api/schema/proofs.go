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

	"github.com/codenotary/merkletree"
)

// Verify returns true iff the _InclusionProof_ proves that _leaf_ is included into _i.Root_'s history at
// the given _index_.
// todo(leogr): can we use schema.Item instead of (index,leaf)?
func (i *InclusionProof) Verify(index uint64, leaf []byte) bool {

	if i == nil || i.Index != index || bytes.Compare(leaf, i.Leaf) != 0 {
		return false
	}

	var path merkletree.Path
	path.FromSlice(i.Path)

	var rt, lf [sha256.Size]byte
	copy(rt[:], i.Root)
	copy(lf[:], i.Leaf)
	return path.VerifyInclusion(i.At, i.Index, rt, lf)
}

// Verify returns true iff the _ConsistencyProof_ proves that _c.SecondRoot_'s history is including the history of
// the provided _prevRoot_ up to the position _c.First_.
func (c *ConsistencyProof) Verify(prevRoot Root) bool {
	if c == nil || c.First != prevRoot.Payload.Index {
		return false
	}

	var path merkletree.Path
	path.FromSlice(c.Path)

	var firstRoot, secondRoot [sha256.Size]byte
	copy(firstRoot[:], prevRoot.Payload.Root)
	copy(secondRoot[:], c.SecondRoot)
	if path.VerifyConsistency(c.Second, c.First, secondRoot, firstRoot) {
		c.FirstRoot = prevRoot.Payload.Root
		return true
	}
	return false
}

// Verify returns true iff the _Proof_ proves that the given _leaf_ is included into _p.Root_'s history at position _p.Index_
// and that the provided _prevRoot_ is included into _p.Root_'s history.
// Providing a zerovalue for _prevRoot_ signals that no previous root is available, thus consistency proof will be skipped.
func (p *Proof) Verify(leaf []byte, prevRoot Root) bool {

	if p == nil || bytes.Compare(leaf, p.Leaf) != 0 {
		return false
	}

	var path merkletree.Path

	path.FromSlice(p.InclusionPath)
	var rt, lf [sha256.Size]byte
	copy(rt[:], p.Root)
	copy(lf[:], p.Leaf)
	if !path.VerifyInclusion(p.At, p.Index, rt, lf) {
		return false
	}

	// we cannot check consistency when the previous root is not provided
	if prevRoot.Payload.Index == 0 && len(prevRoot.Payload.Root) == 0 {
		return true
	}

	path.FromSlice(p.ConsistencyPath)

	var firstRoot, secondRoot [sha256.Size]byte
	copy(firstRoot[:], prevRoot.Payload.Root)
	copy(secondRoot[:], p.Root)
	return path.VerifyConsistency(p.At, prevRoot.Payload.Index, secondRoot, firstRoot)
}

// NewRoot returns a new _Root_ object which holds values referenced by the proof _p_.
func (p *Proof) NewRoot() *Root {
	if p != nil {
		return &Root{
			Payload: &RootIndex{
				Root:  append([]byte{}, p.Root...),
				Index: p.At,
			},
		}
	}
	return nil
}
