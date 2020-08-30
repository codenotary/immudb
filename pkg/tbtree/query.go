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
	"sync"
)

var ErrNoMoreEntries = errors.New("no more entries")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadersNotClosed = errors.New("readers not closed")

type Snapshot struct {
	t       *TBtree
	root    node
	readers map[int]*Reader
	closed  bool
	rwmutex sync.RWMutex
}

type Reader struct {
	snapshot   *Snapshot
	id         int
	initialKey []byte
	isPrefix   bool
	ascOrder   bool
	path       path
	leafNode   *leafNode
	offset     int
	closed     bool
	rwmutex    sync.RWMutex
}

type ReaderSpec struct {
	initialKey []byte
	isPrefix   bool
	ascOrder   bool
}

func NewSnapshot(t *TBtree) *Snapshot {
	return &Snapshot{t: t, root: t.root, readers: make(map[int]*Reader)}
}

func (s *Snapshot) Get(key []byte) (value []byte, ts uint64, err error) {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()

	if s.closed {
		return nil, 0, ErrAlreadyClosed
	}

	return s.root.get(key)
}

func (s *Snapshot) Ts() (uint64, error) {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()

	if s.closed {
		return 0, ErrAlreadyClosed
	}

	return s.root.ts(), nil
}

func (s *Snapshot) Reader(spec *ReaderSpec) (*Reader, error) {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	if spec == nil {
		return nil, ErrIllegalArgument
	}

	path, startingLeaf, startingOffset, err := s.root.findLeafNode(spec.initialKey, nil, nil, spec.ascOrder)
	if err == ErrKeyNotFound {
		return nil, ErrNoMoreEntries
	}
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		snapshot:   s,
		id:         len(s.readers),
		initialKey: spec.initialKey,
		isPrefix:   spec.isPrefix,
		ascOrder:   spec.ascOrder,
		path:       path,
		leafNode:   startingLeaf,
		offset:     startingOffset,
		closed:     false,
	}

	s.readers[reader.id] = reader

	return reader, nil
}

func (s *Snapshot) Close() error {
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	if len(s.readers) > 0 {
		return ErrReadersNotClosed
	}

	s.closed = true

	return nil
}

func (s *Snapshot) closedReader(r *Reader) error {
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	delete(s.readers, r.id)

	return nil
}

func (r *Reader) Read() (key []byte, value []byte, ts uint64, err error) {
	r.rwmutex.RLock()
	defer r.rwmutex.RUnlock()

	if r.closed {
		return nil, nil, 0, ErrAlreadyClosed
	}

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

				path, leaf, off, err := parent.findLeafNode(r.initialKey, parentPath, r.leafNode.maxKey(), r.ascOrder)

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

		if !r.isPrefix || bytes.Equal(r.initialKey, leafValue.key[:len(r.initialKey)]) {
			return leafValue.key, leafValue.value, leafValue.ts, nil
		}
	}
}

func (r *Reader) Close() error {
	r.rwmutex.Lock()
	defer r.rwmutex.Unlock()

	if r.closed {
		return ErrAlreadyClosed
	}

	r.snapshot.closedReader(r)
	r.closed = true

	return nil
}
