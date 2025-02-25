package radix

import (
	"bytes"
	"errors"

	"github.com/codenotary/immudb/embedded/container"
)

const MaxKeySize = 4096

var ErrStopIteration = errors.New("stop iteration")

type Iterator struct {
	t *Tree

	keyBuf [MaxKeySize]byte
	n      int

	reversed bool
	stack    *container.Stack[stackEntry]
}

func (t *Tree) NewIterator(reversed bool) *Iterator {
	return &Iterator{
		t:        t,
		reversed: reversed,
		stack:    container.NewStack[stackEntry](100),
	}
}

type Entry struct {
	Key   []byte
	Value []byte
	HC    uint64
}

func (it *Iterator) Next() (*Entry, error) {
	nd := it.popNextLeaf()
	if nd == nil {
		return nil, ErrStopIteration
	}

	return &Entry{
		Key:   it.keyBuf[:it.n],
		Value: nd.value,
		HC:    nd.HC,
	}, nil
}

func (it *Iterator) popNextLeaf() *node {
	for {
		e, ok := it.stack.Pop()
		if !ok {
			return nil
		}
		nd := e.node

		if nd.isLeaf() {
			return nd
		}

		nextIdx := e.currChild + 1
		if it.reversed {
			nextIdx = e.currChild - 1
		}

		assert(nd.children[e.currChild] != nil, "is nil")

		it.n = e.n
		idx, child := nd.nextChild(nextIdx, it.reversed)
		if child != nil {
			it.stack.Push(
				stackEntry{
					n:         it.n,
					currChild: idx,
					node:      nd,
				},
			)
			it.seekToMin(child)
		}
	}
}

func (it *Iterator) Seek(key []byte) error {
	it.reset()
	it.seek(&it.t.rootNode, key)
	return nil
}

func (it *Iterator) seek(nd *node, key []byte) bool {
	lcp := longestCommonPrefix(nd.prefix, key)
	if nd.isLeaf() {
		if lcp == len(key) {
			it.push(nd, 0)
			return true
		}

		res := bytes.Compare(nd.prefix, key)
		if (it.reversed && res < 0) || (!it.reversed && res > 0) {
			it.push(nd, 0)
			return true
		}
		return false
	}

	c, keySuffix := cutAt(key, lcp, false)

	idx, child := nd.nextChild(int(c), it.reversed)
	if child == nil {
		return false
	}

	if idx != int(c) {
		it.push(nd, int(idx))
		it.seekToMin(child)
		return true
	}

	stackSize := it.stack.Len()
	nBefore := it.n

	it.push(nd, int(idx))

	seeked := it.seek(child, keySuffix)
	if seeked {
		return true
	}

	stackDiff := it.stack.Len() - stackSize
	it.stack.Discard(stackDiff)
	it.n = nBefore

	nextIdx := int(c) + 1
	if it.reversed {
		nextIdx = int(c) - 1
	}

	idx, newChild := nd.nextChild(nextIdx, it.reversed)
	if newChild != nil {
		it.push(nd, int(idx))
		it.seekToMin(newChild)
		return true
	}
	return false
}

func (it *Iterator) seekToMin(nd *node) {
	for !nd.isLeaf() {
		i := 0
		if it.reversed {
			i = len(nd.children) - 1
		}

		idx, newNode := nd.nextChild(i, it.reversed)
		it.push(nd, idx)
		nd = newNode
		assert(nd != nil, "no child found")
	}
	it.push(nd, 0)
}

func (it *Iterator) push(nd *node, idx int) {
	it.n += copy(it.keyBuf[it.n:], nd.prefix)
	it.stack.Push(
		stackEntry{
			n:         it.n,
			currChild: idx,
			node:      nd,
		},
	)
}

func (it *Iterator) reset() {
	it.stack.Reset()
	it.n = 0
}

type stackEntry struct {
	n         int
	currChild int
	node      *node
}
