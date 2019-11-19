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
	n := uint64(len(D))
	if n == 0 {
		return sha256.Sum256(nil)
	}
	if n == 1 {
		c := []byte{LeafPrefix}
		c = append(c, D[0]...)
		return sha256.Sum256(c)
	}

	k := uint64(1) << (bits.Len64(uint64(n-1)) - 1)

	c := []byte{NodePrefix}
	x := MTH(D[0:k])
	c = append(c, x[:]...)
	x = MTH(D[k:n])
	c = append(c, x[:]...)
	return sha256.Sum256(c)
}

// MPath returns the Merkle Audit Path for the (_m_+1)th input of the given ordered list of n inputs _D_.
// The Merkle Audit Path is defined only for 0 <= _m_ < n. For undefined paths, MPath returns a _nil_ slice.
// Reference implementation as per https://tools.ietf.org/html/rfc6962#section-2.1.1
func MPath(m uint64, D [][]byte) (path [][sha256.Size]byte) {
	n := uint64(len(D))
	if !(0 <= m && m < n) {
		return
	}

	path = make([][sha256.Size]byte, 0)
	if n == 1 && m == 0 {
		return
	}

	k := uint64(1) << (bits.Len64(uint64(n-1)) - 1)

	if m < k {
		path = append(path, MPath(m, D[0:k])...)
		path = append(path, MTH(D[k:n]))
	} else {
		path = append(path, MPath(m-k, D[k:n])...)
		path = append(path, MTH(D[0:k]))
	}
	return
}

func mSubproof(m uint64, D [][]byte, b bool) (path [][sha256.Size]byte) {
	path = make([][sha256.Size]byte, 0)
	n := uint64(len(D))

	if m == n {
		if !b {
			path = append(path, MTH(D))
		}
		return
	}

	if m < n {
		k := uint64(1) << (bits.Len64(uint64(n-1)) - 1)

		if m <= k {
			path = append(path, mSubproof(m, D[0:k], b)...)
			path = append(path, MTH(D[k:n]))
		} else {
			path = append(path, mSubproof(m-k, D[k:n], false)...)
			path = append(path, MTH(D[0:k]))
		}
	}
	return
}

// MProof returns the Merke Consistency Proof for the MTH(_D_[n]) and the previously advertised MTH(_D_[_m_:0])
// of the first _m_ leaves when _m_ <= n, where n is the length of the given ordered list of inputs _D_.
// The Merke Consistency Proof is defined only for 0 < _m_ < n. For undefined proofs, MProof returns a _nil_ slice.
// Reference implementation as per https://tools.ietf.org/html/rfc6962#section-2.1.2
func MProof(m uint64, D [][]byte) [][sha256.Size]byte {
	n := uint64(len(D))
	// PROOF is defined only for 0 < m < n
	if 0 < m && m < n {
		return mSubproof(m, D, true)
	}
	return nil
}
