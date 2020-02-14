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

package api

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/codenotary/merkletree"
)

// Digest returns the hash computed from the union of item's members.
func Digest(index uint64, key, value []byte) [sha256.Size]byte {
	kl, vl := len(key), len(value)
	c := make([]byte, 1+8+8+kl+vl)
	c[0] = merkletree.LeafPrefix
	binary.BigEndian.PutUint64(c[1:1+8], index)
	binary.BigEndian.PutUint64(c[1+8:1+8+8], uint64(kl))
	copy(c[1+8+8:], key)
	copy(c[1+8+8+kl:], value)
	return sha256.Sum256(c)
}
