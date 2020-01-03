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

// Match returns true iff the _InclusionProof_ can verify the inclusion of the provided
// _leaf_ at the given _index_.
func (i *InclusionProof) Match(index uint64, leaf []byte) bool {
	if i == nil {
		return false
	}
	return i.Index == index && bytes.Compare(leaf, i.Leaf) == 0
}

// Verify returns true iff _InclusionProof_ proves that _i.Leaf_ is included into _i.Root_'s history.
func (i *InclusionProof) Verify() bool {
	if i == nil {
		return false
	}

	path := tree.Path{}
	path.FromSlice(i.Path)

	var root, leaf [sha256.Size]byte
	copy(root[:], i.Root)
	copy(leaf[:], i.Leaf)
	return path.VerifyInclusion(i.At, i.Index, root, leaf)
}
