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
package tbtree

type QueryNode interface {
	Get(key []byte) (value []byte, ts uint64, err error)
	Ts() uint64
	Reader(spec *ReaderSpec) (*Reader, error)
}

type Reader struct {
	prefix   []byte
	ascOrder bool
	leafNode *leafNode
	offset   int
}

type ReaderSpec struct {
	prefix   []byte
	ascOrder bool
}

type nodeWrapper struct {
	node
}

func (n nodeWrapper) Get(key []byte) (value []byte, ts uint64, err error) {
	return n.get(key)
}

func (n nodeWrapper) Ts() uint64 {
	return n.ts()
}

func (n nodeWrapper) Reader(spec *ReaderSpec) (*Reader, error) {
	if spec == nil {
		return nil, ErrIllegalArgument
	}

	startingLeaf, startingOffset, err := n.findLeafNode(spec.prefix, nil, spec.ascOrder)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		prefix:   spec.prefix,
		ascOrder: spec.ascOrder,
		leafNode: startingLeaf,
		offset:   startingOffset,
	}

	return reader, nil
}

func (c *Reader) Read() (key []byte, value []byte, ts uint64, err error) {
	if (c.ascOrder && len(c.leafNode.values) == c.offset) || (!c.ascOrder && c.offset < 0) {
		parent := c.leafNode.parent

		for {
			if parent == nil {
				return nil, nil, 0, ErrKeyNotFound
			}

			leaf, off, err := parent.findLeafNode(c.prefix, c.leafNode.maxKey(), c.ascOrder)

			if err == ErrKeyNotFound {
				parent = parent.parent
				continue
			}

			if err != nil {
				return nil, nil, 0, err
			}

			c.leafNode = leaf
			c.offset = off
			break
		}
	}

	leafValue := c.leafNode.values[c.offset]

	if c.ascOrder {
		c.offset++
	} else {
		c.offset--
	}

	return leafValue.key, leafValue.value, leafValue.ts, nil
}
