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

package htree

import (
	"crypto/sha256"
	"errors"
	"math/bits"
)

var ErrMaxWidthExceeded = errors.New("htree: max width exceeded")
var ErrIllegalArguments = errors.New("htree: illegal arguments")
var ErrIllegalState = errors.New("htree: illegal state")

const LeafPrefix = byte(0)
const NodePrefix = byte(1)

type HTree struct {
	levels   [][][sha256.Size]byte
	maxWidth int
	width    int
	root     [sha256.Size]byte
}

type InclusionProof struct {
	Leaf  int
	Width int
	Terms [][sha256.Size]byte
}

func New(maxWidth int) (*HTree, error) {
	var levels [][][sha256.Size]byte

	if maxWidth > 0 {
		lw := 1
		for lw < maxWidth {
			lw = lw << 1
		}

		height := bits.Len64(uint64(maxWidth-1)) + 1

		levels = make([][][sha256.Size]byte, height)
		for l := 0; l < height; l++ {
			levels[l] = make([][sha256.Size]byte, lw>>l)
		}
	}

	return &HTree{
		levels:   levels,
		maxWidth: maxWidth,
	}, nil
}

func (t *HTree) BuildWith(digests [][sha256.Size]byte) error {
	if len(digests) > t.maxWidth {
		return ErrMaxWidthExceeded
	}

	if len(digests) == 0 {
		t.width = 0
		t.root = sha256.Sum256(nil)
		return nil
	}

	for i, d := range digests {
		leaf := [1 + sha256.Size]byte{LeafPrefix}
		copy(leaf[1:], d[:])
		t.levels[0][i] = sha256.Sum256(leaf[:])
	}

	l := 0
	w := len(digests)

	for w > 1 {
		b := [1 + 2*sha256.Size]byte{NodePrefix}

		wn := 0

		for i := 0; i+1 < w; i += 2 {
			copy(b[1:], t.levels[l][i][:])
			copy(b[1+sha256.Size:], t.levels[l][i+1][:])
			t.levels[l+1][wn] = sha256.Sum256(b[:])
			wn++
		}

		if w%2 == 1 {
			t.levels[l+1][wn] = t.levels[l][w-1]
			wn++
		}

		l++
		w = wn
	}

	t.width = len(digests)
	t.root = t.levels[l][0]

	return nil
}

func (t *HTree) Root() [sha256.Size]byte {
	return t.root
}

// InclusionProof returns the shortest list of additional nodes required to compute the root
// It's an adaption from the algorithm for proof construction at github.com/codenotary/merkletree
func (t *HTree) InclusionProof(i int) (proof *InclusionProof, err error) {
	if i >= t.width {
		return nil, ErrIllegalArguments
	}

	m := i
	n := t.width

	var offset int
	var l int
	var r int

	proof = &InclusionProof{
		Leaf:  i,
		Width: t.width,
	}

	if t.width == 1 {
		return
	}

	for {
		d := bits.Len(uint(n - 1))
		k := 1 << (d - 1)
		if m < k {
			l, r = offset+k, offset+n-1
			n = k
		} else {
			l, r = offset, offset+k-1
			m = m - k
			n = n - k
			offset += k
		}

		layer := bits.Len(uint(r - l))
		index := l / (1 << layer)

		proof.Terms = append([][sha256.Size]byte{t.levels[layer][index]}, proof.Terms...)

		if n < 1 || (n == 1 && m == 0) {
			return
		}
	}
}

func VerifyInclusion(proof *InclusionProof, digest, root [sha256.Size]byte) bool {
	if proof == nil {
		return false
	}

	leaf := [1 + sha256.Size]byte{LeafPrefix}
	copy(leaf[1:], digest[:])

	calcRoot := sha256.Sum256(leaf[:])
	i := proof.Leaf
	r := proof.Width - 1

	for _, t := range proof.Terms {
		b := [1 + 2*sha256.Size]byte{NodePrefix}

		if i%2 == 0 && i != r {
			copy(b[1:], calcRoot[:])
			copy(b[1+sha256.Size:], t[:])
		} else {
			copy(b[1:], t[:])
			copy(b[1+sha256.Size:], calcRoot[:])
		}

		calcRoot = sha256.Sum256(b[:])
		i /= 2
		r /= 2
	}

	return i == r && root == calcRoot
}
