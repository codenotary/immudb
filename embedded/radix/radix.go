/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package radix

import (
	"errors"
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrMaxKeyLenExceeded = errors.New("max key len exceeded")
	ErrDuplicateKey      = errors.New("duplicate key")
	ErrEmptyKey          = errors.New("empty key")
)

const MaxChildren = 256

type Tree struct {
	rootNode node
}

type node struct {
	prefix   []byte
	children [MaxChildren + 1]*node
	value    []byte
	HC       uint64
}

func (nd *node) nextChild(i int, reversed bool) (int, *node) {
	if reversed {
		for c := i; c >= 0; c-- {
			if child := nd.children[c]; child != nil {
				return c, child
			}
		}
	} else {
		for c := i; c < len(nd.children); c++ {
			if child := nd.children[c]; child != nil {
				return c, child
			}
		}
	}
	return -1, nil
}

func (nd *node) isLeaf() bool {
	return nd.value != nil
}

func New() *Tree {
	return &Tree{}
}

func (t *Tree) Insert(key, value []byte, hc uint64) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(key) > MaxKeySize {
		return ErrMaxKeyLenExceeded
	}

	if !insertNode(&t.rootNode, key, value, hc) {
		return ErrDuplicateKey
	}
	return nil
}

func (t *Tree) Get(key []byte) ([]byte, uint64, error) {
	nd := findNode(&t.rootNode, key)
	if nd == nil {
		return nil, 0, ErrKeyNotFound
	}
	return nd.value, nd.HC, nil
}

func findNode(nd *node, key []byte) *node {
	lcp := longestCommonPrefix(nd.prefix, key)
	if nd.isLeaf() && len(key) == len(nd.prefix) && lcp == len(key) {
		return nd
	}

	c, keySuffix := cutAt(key, lcp, false)
	newNode := nd.children[c]
	if newNode == nil {
		return nil
	}
	return findNode(newNode, keySuffix)
}

func insertNode(nd *node, key, value []byte, hc uint64) bool {
	lcp := longestCommonPrefix(nd.prefix, key)
	if lcp < len(nd.prefix) {
		nd.insert(lcp, key, value, hc)
		return true
	}

	if nd.isLeaf() {
		if lcp == len(key) {
			return false
		}
		nd.insert(lcp, key, value, hc)
		return true
	}

	c, newKey := cutAt(key, lcp, false)

	newNode := nd.children[c]
	if newNode != nil {
		return insertNode(newNode, newKey, value, hc)
	}

	nd.children[c] = &node{
		prefix: cp(newKey),
		value:  value,
		HC:     hc,
	}
	return true
}

func (nd *node) insert(lcp int, key, value []byte, hc uint64) {
	c, newSuffix := cutAt(key, lcp, true)

	newInnerNode := &node{}
	nd.splitAt(newInnerNode, lcp)

	nd.children[c] = &node{
		prefix: newSuffix,
		value:  value,
		HC:     hc,
	}
}

func (nd *node) splitAt(dst *node, i int) {
	dst.children = nd.children
	dst.value = nd.value
	dst.HC = nd.HC

	nd.value = nil
	nd.HC = 0
	for i := range nd.children {
		nd.children[i] = nil
	}

	c := 0
	if i < len(nd.prefix) {
		c = int(nd.prefix[i]) + 1
	}

	dst.prefix = nd.prefix[i:]
	nd.prefix = nd.prefix[:i]

	nd.children[c] = dst
}

func longestCommonPrefix(a, b []byte) int {
	n := min(len(a), len(b))

	lcp := 0
	for lcp < n && a[lcp] == b[lcp] {
		lcp++
	}
	return lcp
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func cutAt(b []byte, i int, shouldCopy bool) (uint16, []byte) {
	if i < len(b) {
		var rem []byte = b[i:]
		if shouldCopy {
			rem = cp(b[i:])
		}
		return uint16(b[i]) + 1, rem
	}
	return 0, nil
}

func cp(b []byte) []byte {
	bc := make([]byte, len(b))
	copy(bc, b)
	return bc
}
