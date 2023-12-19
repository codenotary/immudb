/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ahtree

import "crypto/sha256"

func VerifyInclusion(iproof [][sha256.Size]byte, i, j uint64, iLeaf, jRoot [sha256.Size]byte) bool {
	if i > j || i == 0 || (i < j && len(iproof) == 0) {
		return false
	}

	ciRoot := EvalInclusion(iproof, i, j, iLeaf)

	return jRoot == ciRoot
}

func EvalInclusion(iproof [][sha256.Size]byte, i, j uint64, iLeaf [sha256.Size]byte) [sha256.Size]byte {
	i1 := i - 1
	j1 := j - 1

	ciRoot := iLeaf

	b := [1 + sha256.Size*2]byte{NodePrefix}

	for _, h := range iproof {

		if i1%2 == 0 && i1 != j1 {
			copy(b[1:], ciRoot[:])
			copy(b[sha256.Size+1:], h[:])
		} else {
			copy(b[1:], h[:])
			copy(b[sha256.Size+1:], ciRoot[:])
		}

		ciRoot = sha256.Sum256(b[:])

		i1 >>= 1
		j1 >>= 1
	}

	return ciRoot
}

func VerifyConsistency(cproof [][sha256.Size]byte, i, j uint64, iRoot, jRoot [sha256.Size]byte) bool {
	if i > j || i == 0 || (i < j && len(cproof) == 0) {
		return false
	}

	if i == j && len(cproof) == 0 {
		return iRoot == jRoot
	}

	ciRoot, cjRoot := EvalConsistency(cproof, i, j)

	return iRoot == ciRoot && jRoot == cjRoot
}

func EvalConsistency(cproof [][sha256.Size]byte, i, j uint64) ([sha256.Size]byte, [sha256.Size]byte) {
	fn := i - 1
	sn := j - 1

	for fn%2 == 1 {
		fn >>= 1
		sn >>= 1
	}

	ciRoot, cjRoot := cproof[0], cproof[0]

	b := [1 + sha256.Size*2]byte{NodePrefix}

	for _, h := range cproof[1:] {
		if fn%2 == 1 || fn == sn {
			copy(b[1:], h[:])

			copy(b[1+sha256.Size:], ciRoot[:])
			ciRoot = sha256.Sum256(b[:])

			copy(b[1+sha256.Size:], cjRoot[:])
			cjRoot = sha256.Sum256(b[:])

			for fn%2 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			copy(b[1:], cjRoot[:])
			copy(b[1+sha256.Size:], h[:])
			cjRoot = sha256.Sum256(b[:])
		}
		fn >>= 1
		sn >>= 1
	}

	return ciRoot, cjRoot
}

func VerifyLastInclusion(iproof [][sha256.Size]byte, i uint64, leaf, root [sha256.Size]byte) bool {
	if i == 0 {
		return false
	}

	return root == EvalLastInclusion(iproof, i, leaf)
}

func EvalLastInclusion(iproof [][sha256.Size]byte, i uint64, leaf [sha256.Size]byte) [sha256.Size]byte {
	i1 := i - 1

	root := leaf

	b := [1 + sha256.Size*2]byte{NodePrefix}

	for _, h := range iproof {

		copy(b[1:], h[:])
		copy(b[sha256.Size+1:], root[:])

		root = sha256.Sum256(b[:])

		i1 >>= 1
	}

	return root
}
