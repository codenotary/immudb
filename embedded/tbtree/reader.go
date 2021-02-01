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
)

type Reader struct {
	snapshot      *Snapshot
	id            int
	seekKey       []byte
	prefix        []byte
	inclusiveSeek bool
	descOrder     bool
	path          path
	leafNode      *leafNode
	offset        int
	closed        bool
}

type ReaderSpec struct {
	SeekKey       []byte
	Prefix        []byte
	InclusiveSeek bool
	DescOrder     bool
}

func (r *Reader) Read() (key []byte, value []byte, ts uint64, hc uint64, err error) {
	if r.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	for {
		if (!r.descOrder && len(r.leafNode.values) == r.offset) || (r.descOrder && r.offset < 0) {
			for {
				if len(r.path) == 0 {
					return nil, nil, 0, 0, ErrNoMoreEntries
				}

				parent := r.path[len(r.path)-1]

				var parentPath []*innerNode
				if len(r.path) > 1 {
					parentPath = r.path[:len(r.path)-1]
				}

				neqKey := r.leafNode.maxKey()
				if r.descOrder {
					neqKey = r.leafNode.minKey()
				}

				path, leaf, off, err := parent.findLeafNode(r.seekKey, parentPath, neqKey, r.descOrder)

				if err == ErrKeyNotFound {
					r.path = r.path[:len(r.path)-1]
					continue
				}

				if err != nil {
					return nil, nil, 0, 0, err
				}

				r.path = path
				r.leafNode = leaf
				r.offset = off
				break
			}
		}

		leafValue := r.leafNode.values[r.offset]

		if r.descOrder {
			r.offset--
		} else {
			r.offset++
		}

		if !r.inclusiveSeek && bytes.Equal(r.seekKey, leafValue.key) {
			continue
		}

		if len(leafValue.key) >= len(r.prefix) && bytes.Equal(r.prefix, leafValue.key[:len(r.prefix)]) {
			return leafValue.key, leafValue.value, leafValue.ts, leafValue.hCount, nil
		}
	}
}

func (r *Reader) Close() error {
	if r.closed {
		return ErrAlreadyClosed
	}

	r.snapshot.closedReader(r.id)
	r.closed = true

	return nil
}
