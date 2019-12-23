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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The binary Merkle Tree with 7 leaves:
//                hash
//               /    \
//              /      \
//             /        \
//            /          \
//           /            \
//          k              l
//         / \            / \
//        /   \          /   \
//       /     \        /     \
//      g       h      i       \
//     / \     / \    / \      |
//     a b     c d    e f      j
//     | |     | |    | |      |
//    d0 d1   d2 d3  d4 d5     d6
func make7leaves() (m map[string][sha256.Size]byte, D [][]byte, s Storer) {
	m = make(map[string][sha256.Size]byte)
	s = NewMemStore()
	for i := 0; i < 7; i++ {
		v := "d" + strconv.FormatInt(int64(i), 10)
		D = append(D, []byte(v))
		Append(s, []byte(v))
	}

	m["a"] = *s.Get(0, 0)
	m["b"] = *s.Get(0, 1)
	m["c"] = *s.Get(0, 2)
	m["d"] = *s.Get(0, 3)
	m["e"] = *s.Get(0, 4)
	m["f"] = *s.Get(0, 5)

	m["g"] = *s.Get(1, 0)
	m["h"] = *s.Get(1, 1)
	m["i"] = *s.Get(1, 2)
	m["j"] = *s.Get(0, 6)

	m["k"] = *s.Get(2, 0)
	m["l"] = *s.Get(2, 1)

	m["hash"] = *s.Get(3, 0)

	return
}

func TestInclusionPath(t *testing.T) {
	m, D, s := make7leaves()

	// The audit path for d0 is [b, h, l].
	path := InclusionProof(s, 6, 0)
	assert.Equal(t, Path(MPath(0, D)), path)
	assert.Len(t, path, 3)
	assert.Equal(t, m["b"], path[0])
	assert.Equal(t, m["h"], path[1])
	assert.Equal(t, m["l"], path[2])

	// The audit path for d3 is [c, g, l].
	path = InclusionProof(s, 6, 3)
	assert.Equal(t, Path(MPath(3, D)), path)
	assert.Len(t, path, 3)
	assert.Equal(t, m["c"], path[0])
	assert.Equal(t, m["g"], path[1])
	assert.Equal(t, m["l"], path[2])

	// The audit path for d4 is [f, j, k].
	path = InclusionProof(s, 6, 4)
	assert.Equal(t, Path(MPath(4, D)), path)
	assert.Len(t, path, 3)
	assert.Equal(t, m["f"], path[0])
	assert.Equal(t, m["j"], path[1])
	assert.Equal(t, m["k"], path[2])

	// The audit path for d6 is [i, k]
	path = InclusionProof(s, 6, 6)
	assert.Equal(t, Path(MPath(6, D)), path)
	assert.Len(t, path, 2)
	assert.Equal(t, m["i"], path[0])
	assert.Equal(t, m["k"], path[1])
}

func TestConsistencyPath(t *testing.T) {
	m, D, s := make7leaves()

	// The consistency proof between hash0 and hash is PROOF(3, D[7]) = [c,
	// d, g, l].  c, g are used to verify hash0, and d, l are additionally
	// used to show hash is consistent with hash0.
	path := ConsistencyProof(s, 6, 3)
	assert.Equal(t, Path(MProof(3, D)), path)
	assert.Len(t, path, 4)
	assert.Equal(t, m["c"], path[0])
	assert.Equal(t, m["d"], path[1])
	assert.Equal(t, m["g"], path[2])
	assert.Equal(t, m["l"], path[3])

	// The consistency proof between hash1 and hash is PROOF(4, D[7]) = [l].
	// hash can be verified using hash1=k and l.
	path = ConsistencyProof(s, 6, 4)
	assert.Equal(t, Path(MProof(4, D)), path)
	assert.Len(t, path, 1)
	assert.Equal(t, m["l"], path[0])

	// The consistency proof between hash2 and hash is PROOF(6, D[7]) = [i,
	// j, k].  k, i are used to verify hash2, and j is additionally used to
	// show hash is consistent with hash2.
	path = ConsistencyProof(s, 6, 6)
	assert.Equal(t, Path(MProof(6, D)), path)
	assert.Len(t, path, 3)
	assert.Equal(t, m["i"], path[0])
	assert.Equal(t, m["j"], path[1])
	assert.Equal(t, m["k"], path[2])
}
