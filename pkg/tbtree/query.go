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

import (
	"bytes"
	"errors"
)

var ErrNoMoreEntries = errors.New("no more entries")

type QueryNode interface {
	Get(key []byte) (value []byte, ts uint64, err error)
	Ts() uint64
	Reader(spec *ReaderSpec) (*Reader, error)
}

type Reader struct {
	prefix      []byte
	matchPrefix bool
	ascOrder    bool
	path        path
	leafNode    *leafNode
	offset      int
}

type ReaderSpec struct {
	prefix      []byte
	matchPrefix bool
	ascOrder    bool
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

	path, startingLeaf, startingOffset, err := n.findLeafNode(spec.prefix, nil, nil, spec.ascOrder)
	if err == ErrKeyNotFound {
		return nil, ErrNoMoreEntries
	}
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		prefix:      spec.prefix,
		matchPrefix: spec.matchPrefix,
		ascOrder:    spec.ascOrder,
		path:        path,
		leafNode:    startingLeaf,
		offset:      startingOffset,
	}

	return reader, nil
}

func (r *Reader) Read() (key []byte, value []byte, ts uint64, err error) {
	for {
		if (r.ascOrder && len(r.leafNode.values) == r.offset) || (!r.ascOrder && r.offset < 0) {
			for {
				if len(r.path) == 0 {
					return nil, nil, 0, ErrNoMoreEntries
				}

				parent := r.path[len(r.path)-1]

				var parentPath []*innerNode
				if len(r.path) > 1 {
					parentPath = r.path[:len(r.path)-1]
				}

				path, leaf, off, err := parent.findLeafNode(r.prefix, parentPath, r.leafNode.maxKey(), r.ascOrder)

				if err == ErrKeyNotFound {
					r.path = r.path[:len(r.path)-1]
					continue
				}

				if err != nil {
					return nil, nil, 0, err
				}

				r.path = path
				r.leafNode = leaf
				r.offset = off
				break
			}
		}

		leafValue := r.leafNode.values[r.offset]

		if r.ascOrder {
			r.offset++
		} else {
			r.offset--
		}

		if !r.matchPrefix || bytes.Equal(r.prefix, leafValue.key[:len(r.prefix)]) {
			return leafValue.key, leafValue.value, leafValue.ts, nil
		}
	}
}
