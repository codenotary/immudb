/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

type Reader struct {
	snapshot      *Snapshot
	id            int
	seekKey       []byte
	endKey        []byte
	prefix        []byte
	inclusiveSeek bool
	inclusiveEnd  bool
	descOrder     bool
	path          path
	leafNode      *leafNode
	leafOffset    int

	offset  uint64
	skipped uint64

	closed bool
}

type ReaderSpec struct {
	SeekKey       []byte
	EndKey        []byte
	Prefix        []byte
	InclusiveSeek bool
	InclusiveEnd  bool
	DescOrder     bool
	Offset        uint64
}

func (r *Reader) Reset() error {
	if r.closed {
		return ErrAlreadyClosed
	}

	r.leafNode = nil

	return nil
}

func (r *Reader) ReadBetween(initialTs, finalTs uint64) (key []byte, value []byte, ts, hc uint64, err error) {
	if r.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	if r.leafNode == nil {
		path, startingLeaf, startingOffset, err := r.snapshot.root.findLeafNode(r.seekKey, nil, 0, nil, r.descOrder)
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil, 0, 0, ErrNoMoreEntries
		}
		if err != nil {
			return nil, nil, 0, 0, err
		}

		r.path = path
		r.leafNode = startingLeaf
		r.leafOffset = startingOffset
		r.skipped = 0
	}

	for {
		if (!r.descOrder && len(r.leafNode.values) == r.leafOffset) || (r.descOrder && r.leafOffset < 0) {
			for {
				if len(r.path) == 0 {
					return nil, nil, 0, 0, ErrNoMoreEntries
				}

				parent := r.path[len(r.path)-1]

				var parentPath []*pathNode
				if len(r.path) > 1 {
					parentPath = r.path[:len(r.path)-1]
				}

				path, leaf, off, err := parent.node.findLeafNode(r.seekKey, parentPath, parent.offset+1, nil, r.descOrder)

				if errors.Is(err, ErrKeyNotFound) {
					r.path = r.path[:len(r.path)-1]
					continue
				}

				if err != nil {
					return nil, nil, 0, 0, err
				}

				r.path = path
				r.leafNode = leaf
				r.leafOffset = off
				break
			}
		}

		leafValue := r.leafNode.values[r.leafOffset]

		if r.descOrder {
			r.leafOffset--
		} else {
			r.leafOffset++
		}

		if !r.inclusiveSeek && bytes.Equal(r.seekKey, leafValue.key) {
			continue
		}

		if len(r.endKey) > 0 {
			cmp := bytes.Compare(r.endKey, leafValue.key)

			if r.descOrder && (cmp > 0 || (cmp == 0 && !r.inclusiveEnd)) {
				return nil, nil, 0, 0, ErrNoMoreEntries
			}

			if !r.descOrder && (cmp < 0 || (cmp == 0 && !r.inclusiveEnd)) {
				return nil, nil, 0, 0, ErrNoMoreEntries
			}
		}

		// prefix mismatch
		if len(r.prefix) > 0 &&
			(len(leafValue.key) < len(r.prefix) || !bytes.Equal(r.prefix, leafValue.key[:len(r.prefix)])) {
			continue
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		value, ts, hc, err := leafValue.lastUpdateBetween(r.snapshot.t.hLog, initialTs, finalTs)
		if err == nil {
			return cp(leafValue.key), cp(value), ts, hc, nil
		}
	}
}

func (r *Reader) Read() (key []byte, value []byte, ts, hc uint64, err error) {
	if r.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	if r.leafNode == nil {
		path, startingLeaf, startingOffset, err := r.snapshot.root.findLeafNode(r.seekKey, nil, 0, nil, r.descOrder)
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil, 0, 0, ErrNoMoreEntries
		}
		if err != nil {
			return nil, nil, 0, 0, err
		}

		r.path = path
		r.leafNode = startingLeaf
		r.leafOffset = startingOffset
		r.skipped = 0
	}

	for {
		if (!r.descOrder && len(r.leafNode.values) == r.leafOffset) || (r.descOrder && r.leafOffset < 0) {
			for {
				if len(r.path) == 0 {
					return nil, nil, 0, 0, ErrNoMoreEntries
				}

				parent := r.path[len(r.path)-1]

				var parentPath []*pathNode
				if len(r.path) > 1 {
					parentPath = r.path[:len(r.path)-1]
				}

				path, leaf, off, err := parent.node.findLeafNode(r.seekKey, parentPath, parent.offset+1, nil, r.descOrder)

				if errors.Is(err, ErrKeyNotFound) {
					r.path = r.path[:len(r.path)-1]
					continue
				}

				if err != nil {
					return nil, nil, 0, 0, err
				}

				r.path = path
				r.leafNode = leaf
				r.leafOffset = off
				break
			}
		}

		leafValue := r.leafNode.values[r.leafOffset]

		if r.descOrder {
			r.leafOffset--
		} else {
			r.leafOffset++
		}

		if !r.inclusiveSeek && bytes.Equal(r.seekKey, leafValue.key) {
			continue
		}

		if len(r.endKey) > 0 {
			cmp := bytes.Compare(r.endKey, leafValue.key)

			if r.descOrder && (cmp > 0 || (cmp == 0 && !r.inclusiveEnd)) {
				return nil, nil, 0, 0, ErrNoMoreEntries
			}

			if !r.descOrder && (cmp < 0 || (cmp == 0 && !r.inclusiveEnd)) {
				return nil, nil, 0, 0, ErrNoMoreEntries
			}
		}

		// prefix mismatch
		if len(r.prefix) > 0 &&
			(len(leafValue.key) < len(r.prefix) || !bytes.Equal(r.prefix, leafValue.key[:len(r.prefix)])) {
			continue
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		return cp(leafValue.key), cp(leafValue.timedValue().Value), leafValue.timedValue().Ts, leafValue.historyCount(), nil
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

func cp(s []byte) []byte {
	if s == nil {
		return nil
	}

	c := make([]byte, len(s))
	copy(c, s)

	return c
}
