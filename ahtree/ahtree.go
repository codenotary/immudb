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
)

var ErrIllegalArguments = errors.New("illegal arguments")

//AHtree stands for Appendable Hash Tree
type AHtree struct {
	data    [][]byte            // TODO appendable
	digests [][sha256.Size]byte // TODO appendable
}

func (t *AHtree) Append(d []byte) (n int64, r [sha256.Size]byte, err error) {
	t.data = append(t.data, d)

	n = int64(len(t.data))

	h := sha256.Sum256(d)
	t.digests = append(t.digests, h)

	w := n - 1
	o := int64(0)
	l := int64(0)

	for w > 0 {
		if w%2 == 1 {
			var b [sha256.Size * 2]byte // add Node prefix for compatibility with sdks

			off := nodesUntil(n-(1<<o)) + l // when working with appendable offsets will be * sha256.Size

			copy(b[:], t.digests[int(off)][:])
			copy(b[sha256.Size:], h[:])

			t.digests = append(t.digests, sha256.Sum256(b[:]))

			o++
		}
		l++
		w = w >> 1
	}

	return int64(len(t.data)), t.digests[len(t.digests)-1], nil
}

func nodesUntil(n int64) int64 {
	if n == 1 {
		return 0
	}
	return nodesUpto(n - 1)
}

func nodesUpto(n int64) int64 {
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

func levelsAt(n int64) int {
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

	w := j - 1
	o := int64(1)
	l := int64(0)

	for w > 0 {
		if w%2 == 1 {

			n := j - (1 << o)

			if i < n {

				//append current hash level
			}
			// off := (t.nodesUntil(j-(1<<o)) + l) // when working with appendable offsets will be * sha256.Size

			o++
		}

		l++
		w = w >> 1
	}

	return nil, nil
}

func (t *AHtree) Size() (int64, error) {
	return int64(len(t.data)), nil
}

func (t *AHtree) Root() ([sha256.Size]byte, error) {
	return t.digests[len(t.digests)-1], nil
}

func (t *AHtree) RootAt(n int64) ([sha256.Size]byte, error) {
	off := nodesUntil(n) + int64(levelsAt(n))
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
