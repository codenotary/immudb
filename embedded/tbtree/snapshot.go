/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
)

var ErrNoMoreEntries = errors.New("no more entries")
var ErrReadersNotClosed = errors.New("readers not closed")

const (
	InnerNodeType = iota
	LeafNodeType
)

type Snapshot struct {
	t           *TBtree
	id          uint64
	ts          uint64
	root        node
	readers     map[int]io.Closer
	maxReaderID int
	closed      bool
	mutex       sync.RWMutex
}

func (s *Snapshot) Set(key, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	k := make([]byte, len(key))
	copy(k, key)

	v := make([]byte, len(value))
	copy(v, value)

	n1, n2, depth, err := s.root.insertAt(k, v, s.ts)
	if err != nil {
		return err
	}

	if n2 == nil {
		s.root = n1
	} else {
		newRoot := &innerNode{
			t:       s.t,
			nodes:   []node{n1, n2},
			_minKey: n1.minKey(),
			_maxKey: n2.maxKey(),
			_ts:     s.ts,
			maxSize: s.t.maxNodeSize,
			mut:     true,
		}

		s.root = newRoot
		depth++
	}

	metricsBtreeDepth.WithLabelValues(s.t.path).Set(float64(depth))

	return nil
}

func (s *Snapshot) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	v, ts, hc, err := s.root.get(key)
	return cp(v), ts, hc, err
}

func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (tss []uint64, err error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	if key == nil {
		return nil, ErrIllegalArguments
	}

	if limit < 1 {
		return nil, ErrIllegalArguments
	}

	return s.root.history(key, offset, descOrder, limit)
}

func (s *Snapshot) Ts() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.root.ts()
}

func (s *Snapshot) ExistKeyWith(prefix []byte, neq []byte) (bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return false, ErrAlreadyClosed
	}

	_, leaf, off, err := s.root.findLeafNode(prefix, nil, neq, false)
	if err == ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	v := leaf.values[off]

	if len(prefix) > len(v.key) {
		return false, nil
	}

	return bytes.Equal(prefix, v.key[:len(prefix)]), nil
}

func (s *Snapshot) NewHistoryReader(spec *HistoryReaderSpec) (*HistoryReader, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	reader, err := newHistoryReader(s.maxReaderID, s, spec)
	if err != nil {
		return nil, err
	}

	s.readers[reader.id] = reader
	s.maxReaderID++

	return reader, nil
}

func (s *Snapshot) NewReader(spec *ReaderSpec) (r *Reader, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	if spec == nil || len(spec.SeekKey) > s.t.maxKeyLen || len(spec.Prefix) > s.t.maxKeyLen {
		return nil, ErrIllegalArguments
	}

	greatestPrefixedKey := greatestKeyOfSize(s.t.maxKeyLen)
	copy(greatestPrefixedKey, spec.Prefix)

	// Adjust seekKey based on key prefix
	seekKey := spec.SeekKey
	inclusiveSeek := spec.InclusiveSeek

	if spec.DescOrder {
		if len(spec.SeekKey) == 0 || bytes.Compare(spec.SeekKey, greatestPrefixedKey) > 0 {
			seekKey = greatestPrefixedKey
			inclusiveSeek = true
		}
	} else {
		if bytes.Compare(spec.SeekKey, spec.Prefix) < 0 {
			seekKey = spec.Prefix
			inclusiveSeek = true
		}
	}

	// Adjust endKey based on key prefix
	endKey := spec.EndKey
	inclusiveEnd := spec.InclusiveEnd

	if spec.DescOrder {
		if bytes.Compare(spec.EndKey, spec.Prefix) < 0 {
			endKey = spec.Prefix
			inclusiveEnd = true
		}
	} else {
		if len(spec.EndKey) == 0 || bytes.Compare(spec.EndKey, greatestPrefixedKey) > 0 {
			endKey = greatestPrefixedKey
			inclusiveEnd = true
		}
	}

	r = &Reader{
		snapshot:      s,
		id:            s.maxReaderID,
		seekKey:       seekKey,
		endKey:        endKey,
		prefix:        spec.Prefix,
		inclusiveSeek: inclusiveSeek,
		inclusiveEnd:  inclusiveEnd,
		descOrder:     spec.DescOrder,
		closed:        false,
	}

	s.readers[r.id] = r
	s.maxReaderID++

	return r, nil
}

func (s *Snapshot) closedReader(id int) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.readers, id)
	return nil
}

func (s *Snapshot) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	if len(s.readers) > 0 {
		return ErrReadersNotClosed
	}

	err := s.t.snapshotClosed(s)
	if err != nil {
		return err
	}

	s.closed = true

	return nil
}

func (s *Snapshot) WriteTo(nw, hw appendable.Appendable, writeOpts *WriteOpts) (nOff int64, wN, wH int64, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.root.writeTo(nw, hw, writeOpts)
}

func (n *innerNode) writeTo(nw, hw appendable.Appendable, writeOpts *WriteOpts) (nOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !n.mutated() {
		return n.off, 0, 0, nil
	}

	var cnw, chw int64

	offsets := make([]int64, len(n.nodes))

	for i, c := range n.nodes {
		wopts := &WriteOpts{
			OnlyMutated:    writeOpts.OnlyMutated,
			BaseNLogOffset: writeOpts.BaseNLogOffset + cnw,
			BaseHLogOffset: writeOpts.BaseHLogOffset + chw,
			commitLog:      writeOpts.commitLog,
		}

		no, wn, wh, err := c.writeTo(nw, hw, wopts)
		if err != nil {
			return 0, wn, wh, err
		}

		offsets[i] = no
		cnw += wn
		chw += wh
	}

	size := n.size()

	availableBytesInBlock := n.maxSize - int(nw.Offset()%int64(n.maxSize))

	var leftPaddingLen int

	if size > availableBytesInBlock {
		// padding is needed when the node does not fit into a partially used block
		leftPaddingLen = availableBytesInBlock
	}

	buf := make([]byte, leftPaddingLen+size)
	bi := leftPaddingLen

	buf[bi] = InnerNodeType
	bi++

	binary.BigEndian.PutUint32(buf[bi:], uint32(size)) // Size
	bi += 4

	binary.BigEndian.PutUint32(buf[bi:], uint32(len(n.nodes)))
	bi += 4

	for i, c := range n.nodes {
		n := writeNodeRefToWithOffset(c, offsets[i], buf[bi:])
		bi += n
	}

	_, wn, err := nw.Append(buf)
	if err != nil {
		return 0, int64(wn), chw, err
	}

	metricsFlushingNodesProgress.WithLabelValues(n.t.path).Inc()
	metricsFlushingNodesTotal.WithLabelValues(n.t.path).Inc()

	wN = cnw + int64(wn)
	nOff = writeOpts.BaseNLogOffset + int64(leftPaddingLen) + cnw

	if writeOpts.commitLog {
		n.off = nOff
		n.mut = false

		nodes := make([]node, len(n.nodes))

		for i, c := range n.nodes {
			nodes[i] = &nodeRef{
				t:       n.t,
				_minKey: c.minKey(),
				_maxKey: c.maxKey(),
				_ts:     c.ts(),
				_size:   c.size(),
				off:     c.offset(),
			}
		}

		n.nodes = nodes

		n.t.cachePut(n)
	}

	return nOff, wN, chw, nil
}

func (l *leafNode) writeTo(nw, hw appendable.Appendable, writeOpts *WriteOpts) (nOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !l.mutated() {
		return l.off, 0, 0, nil
	}

	size := l.size()

	availableBytesInBlock := l.maxSize - int(nw.Offset()%int64(l.maxSize))

	var leftPaddingLen int

	if size > availableBytesInBlock {
		// padding is needed when the node does not fit into a partially used block
		leftPaddingLen = availableBytesInBlock
	}

	buf := make([]byte, leftPaddingLen+size)
	bi := leftPaddingLen

	buf[bi] = LeafNodeType
	bi++

	binary.BigEndian.PutUint32(buf[bi:], uint32(size)) // Size
	bi += 4

	binary.BigEndian.PutUint32(buf[bi:], uint32(len(l.values)))
	bi += 4

	accH := int64(0)

	for _, v := range l.values {
		binary.BigEndian.PutUint32(buf[bi:], uint32(len(v.key)))
		bi += 4

		copy(buf[bi:], v.key)
		bi += len(v.key)

		binary.BigEndian.PutUint32(buf[bi:], uint32(len(v.value)))
		bi += 4

		copy(buf[bi:], v.value)
		bi += len(v.value)

		binary.BigEndian.PutUint64(buf[bi:], v.ts)
		bi += 8

		hOff := v.hOff
		hCount := v.hCount + uint64(len(v.tss))

		if len(v.tss) > 0 {
			hbuf := make([]byte, 4+len(v.tss)*8+8)
			hi := 0

			binary.BigEndian.PutUint32(hbuf[hi:], uint32(len(v.tss)))
			hi += 4

			for _, ts := range v.tss {
				binary.BigEndian.PutUint64(hbuf[hi:], uint64(ts))
				hi += 8
			}

			binary.BigEndian.PutUint64(hbuf[hi:], uint64(v.hOff))
			hi += 8

			_, n, err := hw.Append(hbuf)
			if err != nil {
				return 0, 0, int64(n), err
			}

			hOff = writeOpts.BaseHLogOffset + accH

			accH += int64(n)
		}

		binary.BigEndian.PutUint64(buf[bi:], uint64(hOff))
		bi += 8

		binary.BigEndian.PutUint64(buf[bi:], hCount)
		bi += 8

		if writeOpts.commitLog {
			v.tss = nil
			v.hOff = hOff
			v.hCount = hCount
		}
	}

	_, n, err := nw.Append(buf)
	if err != nil {
		return 0, int64(n), accH, err
	}

	metricsFlushingNodesProgress.WithLabelValues(l.t.path).Inc()
	metricsFlushingNodesTotal.WithLabelValues(l.t.path).Inc()

	wN = int64(n)
	nOff = writeOpts.BaseNLogOffset + int64(leftPaddingLen)

	if writeOpts.commitLog {
		l.off = nOff
		l.mut = false

		l.t.cachePut(l)
	}

	return nOff, wN, accH, nil
}

func (n *nodeRef) writeTo(nw, hw appendable.Appendable, writeOpts *WriteOpts) (nOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated {
		return n.offset(), 0, 0, nil
	}

	node, err := n.t.nodeAt(n.off)
	if err != nil {
		return 0, 0, 0, err
	}

	off, wn, wh, err := node.writeTo(nw, hw, writeOpts)
	if err != nil {
		return 0, wn, wh, err
	}

	if writeOpts.commitLog {
		n.off = off
	}

	return off, wn, wh, nil
}

func writeNodeRefToWithOffset(n node, offset int64, buf []byte) int {
	i := 0

	minKey := n.minKey()
	binary.BigEndian.PutUint32(buf[i:], uint32(len(minKey)))
	i += 4

	copy(buf[i:], minKey)
	i += len(minKey)

	maxKey := n.maxKey()
	binary.BigEndian.PutUint32(buf[i:], uint32(len(maxKey)))
	i += 4

	copy(buf[i:], maxKey)
	i += len(maxKey)

	binary.BigEndian.PutUint64(buf[i:], n.ts())
	i += 8

	binary.BigEndian.PutUint32(buf[i:], uint32(n.size()))
	i += 4

	binary.BigEndian.PutUint64(buf[i:], uint64(offset))
	i += 8

	return i
}
