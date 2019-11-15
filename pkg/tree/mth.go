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

// MTH returns the Merkle Tree Hash, given an ordered list of n inputs _D_.
// Reference implementation as per https://tools.ietf.org/html/rfc6962#section-2.1
func MTH(D [][]byte) [sha256.Size]byte {
	n := len(D)
	if n == 0 {
		return sha256.Sum256(nil)
	}
	if n == 1 {
		c := []byte{LeafPrefix}
		c = append(c, D[0]...)
		return sha256.Sum256(c)
	}

	log := bits.Len64(uint64(n - 1))
	k := 1 << (log - 1)

	c := []byte{NodePrefix}
	x := MTH(D[0:k])
	c = append(c, x[:]...)
	x = MTH(D[k:n])
	c = append(c, x[:]...)
	return sha256.Sum256(c)
}
