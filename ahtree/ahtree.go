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

package ahtree

import (
	"crypto/sha256"
	"errors"
	"math/bits"
)

var ErrIllegalArguments = errors.New("illegal arguments")

//AHtree stands for Appendable Hash Tree
type AHtree struct {
	data    [][]byte            // TODO appendable
	digests [][sha256.Size]byte // TODO appendable
}

const NodePrefix = byte(1)

func (t *AHtree) Append(d []byte) (n uint64, r [sha256.Size]byte, err error) {
	t.data = append(t.data, d)

	n = uint64(len(t.data))

	h := sha256.Sum256(d)
	t.digests = append(t.digests, h)

	w := n - 1
	l := 0

	k := n - 1

	for w > 0 {
		if w%2 == 1 {
			b := [1 + sha256.Size*2]byte{NodePrefix}

			hkl := t.node(k, l)

			copy(b[1:], hkl[:])
			copy(b[1+sha256.Size:], h[:])

			h = sha256.Sum256(b[:])

			t.digests = append(t.digests, h)
		}

		k = k &^ uint64(1<<l)
		w = w >> 1
		l++
	}

	return n, t.digests[len(t.digests)-1], nil
}

func (t *AHtree) node(n uint64, l int) [sha256.Size]byte {
	off := nodesUntil(n) + uint64(l) // when working with appendable offsets will be * sha256.Size
	return t.digests[int(off)]
}

func nodesUntil(n uint64) uint64 {
	if n == 1 {
		return 0
	}
	return nodesUpto(n - 1)
}

func nodesUpto(n uint64) uint64 {
	o := n
	l := 0

	for {
		if n < (1 << l) {
			break
		}

		o += n >> (l + 1) << l

		if (n/(1<<l))%2 == 1 {
			o += n % (1 << l)
		}

		l++
	}

	return o
}

func levelsAt(n uint64) int {
	w := n - 1
	l := 0
	for w > 0 {
		if w%2 == 1 {
			l++
		}
		w = w >> 1
	}
	return l
}

func (t *AHtree) InclusionProof(i, j uint64) ([][sha256.Size]byte, error) {
	if i > j {
		return nil, ErrIllegalArguments
	}

	return t.inclusionProof(i, j, bits.Len64(j-1))
}

func (t *AHtree) inclusionProof(i, j uint64, height int) ([][sha256.Size]byte, error) {
	var proof [][sha256.Size]byte

	for h := height - 1; h >= 0; h-- {
		if (j-1)&(1<<h) > 0 {
			k := (j - 1) >> h << h

			if i <= k {
				proof = append([][sha256.Size]byte{t.highestNode(j, h)}, proof...)

				p, err := t.inclusionProof(i, k, h)
				if err != nil {
					return nil, err
				}

				proof = append(p, proof...)

				return proof, nil
			}

			proof = append([][sha256.Size]byte{t.node(k, h)}, proof...)
		}
	}

	return proof, nil
}

func (t *AHtree) highestNode(i uint64, d int) [sha256.Size]byte {
	l := 0

	for r := d - 1; r >= 0; r-- {
		if (i-1)&(1<<r) > 0 {
			l++
		}
	}

	return t.node(i, l)
}

func (t *AHtree) Size() (uint64, error) {
	return uint64(len(t.data)), nil
}

func (t *AHtree) Root() ([sha256.Size]byte, error) {
	return t.digests[len(t.digests)-1], nil
}

func (t *AHtree) RootAt(n uint64) ([sha256.Size]byte, error) {
	off := nodesUntil(n) + uint64(levelsAt(n))
	return t.digests[off], nil
}

func (t *AHtree) Flush() error {
	return nil
}

func (t *AHtree) Sync() error {
	return nil
}

func (t *AHtree) Close() error {
	return nil
}
