/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"math"
	"sync"
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

	_buf []byte

	mutex sync.RWMutex
}

func (s *Snapshot) Set(key, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	k := make([]byte, len(key))
	copy(k, key)

	v := make([]byte, len(value))
	copy(v, value)

	nodes, depth, err := s.root.insertAt(k, v, s.ts)
	if err != nil {
		return err
	}

	for len(nodes) > 1 {
		newRoot := &innerNode{
			t:     s.t,
			nodes: nodes,
			_ts:   s.ts,
			mut:   true,
		}

		depth++

		nodes, err = newRoot.split()
		if err != nil {
			return err
		}
	}

	s.root = nodes[0]

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

func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (tss []uint64, hCount uint64, err error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, ErrIllegalArguments
	}

	if limit < 1 {
		return nil, 0, ErrIllegalArguments
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

	_, leaf, off, err := s.root.findLeafNode(prefix, nil, 0, neq, false)
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

	if spec == nil || len(spec.SeekKey) > s.t.maxKeySize || len(spec.Prefix) > s.t.maxKeySize {
		return nil, ErrIllegalArguments
	}

	greatestPrefixedKey := greatestKeyOfSize(s.t.maxKeySize)
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

func (s *Snapshot) WriteTo(nw, hw io.Writer, writeOpts *WriteOpts) (rootOffset, minOffset int64, wN, wH int64, err error) {
	if nw == nil || writeOpts == nil {
		return 0, 0, 0, 0, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.root.writeTo(nw, hw, writeOpts, s._buf)
}

func (n *innerNode) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !n.mutated() && n._minOff >= writeOpts.MinOffset {
		return n.off, n._minOff, 0, 0, nil
	}

	var cnw, chw int64

	wopts := &WriteOpts{
		OnlyMutated:    writeOpts.OnlyMutated,
		commitLog:      writeOpts.commitLog,
		reportProgress: writeOpts.reportProgress,
		MinOffset:      writeOpts.MinOffset,
	}

	offsets := make([]int64, len(n.nodes))
	minOffsets := make([]int64, len(n.nodes))
	minOff = math.MaxInt64

	for i, c := range n.nodes {
		wopts.BaseNLogOffset = writeOpts.BaseNLogOffset + cnw
		wopts.BaseHLogOffset = writeOpts.BaseHLogOffset + chw

		no, mo, wn, wh, err := c.writeTo(nw, hw, wopts, buf)
		if err != nil {
			return 0, 0, cnw, chw, err
		}

		offsets[i] = no
		minOffsets[i] = mo

		if minOffsets[i] < minOff {
			minOff = minOffsets[i]
		}

		cnw += wn
		chw += wh
	}

	size, err := n.size()
	if err != nil {
		return 0, 0, cnw, chw, err
	}

	bi := 0

	buf[bi] = InnerNodeType
	bi++

	binary.BigEndian.PutUint16(buf[bi:], uint16(len(n.nodes)))
	bi += 2

	for i, c := range n.nodes {
		n := writeNodeRefToWithOffset(c, offsets[i], minOffsets[i], buf[bi:])
		bi += n
	}

	wn, err := nw.Write(buf[:bi])
	if err != nil {
		return 0, 0, int64(wn), chw, err
	}

	wN = cnw + int64(size)
	nOff = writeOpts.BaseNLogOffset + cnw

	if writeOpts.commitLog {
		n.off = writeOpts.BaseNLogOffset + cnw
		n._minOff = minOff

		if n.mut {
			n.mut = false

			for i, c := range n.nodes {
				_, isNodeRef := c.(*nodeRef)

				if isNodeRef {
					continue
				}

				n.nodes[i] = &nodeRef{
					t:       n.t,
					_minKey: c.minKey(),
					_ts:     c.ts(),
					off:     c.offset(),
					_minOff: c.minOffset(),
				}

				n.t.cachePut(c)
			}
		}
	}

	writeOpts.reportProgress(1, 0, 0)

	return nOff, minOff, wN, chw, nil
}

func (l *leafNode) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !l.mutated() && l.off >= writeOpts.MinOffset {
		return l.off, l.off, 0, 0, nil
	}

	size, err := l.size()
	if err != nil {
		return 0, 0, 0, 0, err
	}

	bi := 0

	buf[bi] = LeafNodeType
	bi++

	binary.BigEndian.PutUint16(buf[bi:], uint16(len(l.values)))
	bi += 2

	accH := int64(0)

	for _, v := range l.values {
		binary.BigEndian.PutUint16(buf[bi:], uint16(len(v.key)))
		bi += 2

		copy(buf[bi:], v.key)
		bi += len(v.key)

		binary.BigEndian.PutUint16(buf[bi:], uint16(len(v.value)))
		bi += 2

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

			n, err := hw.Write(hbuf)
			if err != nil {
				return 0, 0, 0, int64(n), err
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

	n, err := nw.Write(buf[:bi])
	if err != nil {
		return 0, 0, int64(n), accH, err
	}

	wN = int64(size)
	nOff = writeOpts.BaseNLogOffset

	if writeOpts.commitLog {
		l.off = nOff

		if l.mut {
			l.mut = false
			l.t.cachePut(l)
		}
	}

	writeOpts.reportProgress(0, 1, len(l.values))

	return nOff, nOff, wN, accH, nil
}

func (n *nodeRef) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && n._minOff >= writeOpts.MinOffset {
		return n.off, n._minOff, 0, 0, nil
	}

	node, err := n.t.nodeAt(n.off, false)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	off, mOff, wn, wh, err := node.writeTo(nw, hw, writeOpts, buf)
	if err != nil {
		return 0, 0, wn, wh, err
	}

	if writeOpts.commitLog {
		n.off = off
		n._minOff = mOff
	}

	return off, mOff, wn, wh, nil
}

func writeNodeRefToWithOffset(n node, offset, minOff int64, buf []byte) int {
	i := 0

	minKey := n.minKey()
	binary.BigEndian.PutUint16(buf[i:], uint16(len(minKey)))
	i += 2

	copy(buf[i:], minKey)
	i += len(minKey)

	binary.BigEndian.PutUint64(buf[i:], n.ts())
	i += 8

	binary.BigEndian.PutUint64(buf[i:], uint64(offset))
	i += 8

	binary.BigEndian.PutUint64(buf[i:], uint64(minOff))
	i += 8

	return i
}
