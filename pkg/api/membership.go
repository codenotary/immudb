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

package api

import (
	"crypto/sha256"

	"github.com/codenotary/immudb/pkg/tree"
)

type MembershipProof struct {
	Index uint64
	Hash  [sha256.Size]byte

	Root [sha256.Size]byte
	At   uint64

	Path tree.Path
}

func (m *MembershipProof) Verify() bool {
	if m == nil {
		return false
	}
	return m.Path.Verify(m.At, m.Index, m.Root, m.Hash)
}
