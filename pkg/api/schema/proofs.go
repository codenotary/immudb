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

// Verify returns true iff the _ConsistencyProof_ proves that the provided _root_ at the given _index_ is included into _c.SecondRoot_'s history.
func (c *ConsistencyProof) Verify(index uint64, root []byte) bool {
	if c == nil || c.First != index {
		return false
	}

	c.FirstRoot = root

	var path tree.Path
	path.FromSlice(c.Path)

	var firstRoot, secondRoot [sha256.Size]byte
	copy(firstRoot[:], c.FirstRoot)
	copy(secondRoot[:], c.SecondRoot)
	return path.VerifyConsistency(c.Second, c.First, secondRoot, firstRoot)
}
