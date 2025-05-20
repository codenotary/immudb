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

package tbtree

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/codenotary/immudb/v2/embedded/container"
	"github.com/codenotary/immudb/v2/embedded/radix"
)

var (
	ErrSnapshotNotFlushed   = errors.New("snapshot is not flushed")
	ErrCannotModifySnapshot = errors.New("cannot modify snapshot")
	ErrInvalidHistoryOffset = errors.New("invalid history offset")
)

type Snapshot interface {
	UseEntry(key []byte, onEntry func(e *Entry) error) error
	Get(key []byte) (value []byte, ts uint64, hc uint64, err error)
	GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error)
	GetBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error)
	History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []TimedValue, hCount uint64, err error)
	NewIterator(opts IteratorOptions) (Iterator, error)
	Set(key []byte, value []byte) error
	Ts() uint64
	Close() error
}

type TBTreeSnapshot struct {
	isWriteSnapshot bool
	rootID          PageID
	maxTs           uint64

	tree         *TBTree
	numIterators uint32
	closed       bool
}

func (t *TBTree) newReadSnapshot(rootID PageID, maxTs uint64) (Snapshot, error) {
	return t.newSnapshot(false, rootID, maxTs)
}

func (t *TBTree) newSnapshot(
	isWriteSnapshot bool,
	rootID PageID,
	maxTs uint64,
) (Snapshot, error) {
	if n := atomic.AddUint64(&t.snapshotCount, 1); n > uint64(t.maxActiveSnapshots) {
		atomic.AddUint64(&t.snapshotCount, ^uint64(0))
		return nil, ErrorToManyActiveSnapshots
	}

	snap := &TBTreeSnapshot{
		isWriteSnapshot: isWriteSnapshot,
		rootID:          rootID,
		maxTs:           maxTs,
		tree:            t,
	}

	return &localSnapshot{
		snap: snap,
	}, nil
}

func (snap *TBTreeSnapshot) UseEntry(
	key []byte,
	onEntry func(e *Entry) error,
) error {
	var found bool

	err := traverse(
		snap.tree,
		snap.rootID,
		key,
		func(pg *Page, pgID PageID) (bool, error) {
			if !pg.IsLeaf() {
				return true, nil
			}

			e, err := pg.GetEntry(key)
			if err == nil {
				found = true
				return true, onEntry(e)
			}
			return true, err
		})
	if err == nil && !found {
		return ErrKeyNotFound
	}
	return err
}

func (snap *TBTreeSnapshot) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	return snap.GetBetween(key, 0, snap.maxTs)
}

func (snap *TBTreeSnapshot) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error) {
	it, err := snap.NewIterator(DefaultIteratorOptions())
	if err != nil {
		return nil, nil, 0, 0, err
	}
	defer it.Close()

	return getWithPrefix(it, prefix, neq)
}

func getWithPrefix(it Iterator, prefix, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error) {
	if err := it.Seek(prefix); err != nil {
		return nil, nil, 0, 0, err
	}

	e, err := it.Next()
	if errors.Is(err, ErrNoMoreEntries) {
		return nil, nil, 0, 0, ErrKeyNotFound
	}

	if !bytes.HasPrefix(e.Key, prefix) || bytes.Equal(e.Key, neq) {
		return nil, nil, 0, 0, ErrKeyNotFound
	}

	cp := e.Copy()
	return cp.Key, cp.Value, cp.Ts, cp.HC + 1, nil
}

func (snap *TBTreeSnapshot) GetBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	if snap.isWriteSnapshot && finalTs != snap.maxTs {
		return nil, 0, 0, fmt.Errorf("cannot query history: %w", ErrSnapshotNotFlushed)
	}

	if finalTs > snap.maxTs {
		finalTs = snap.maxTs
	}

	it, err := snap.NewHistoryIterator(key, snap.maxTs)
	if err != nil {
		return nil, 0, 0, err
	}

	for {
		tv, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			return nil, 0, 0, ErrKeyNotFound
		}
		if err != nil {
			return nil, 0, 0, err
		}

		if tv.Ts > finalTs {
			continue
		}

		if tv.Ts < initialTs {
			return nil, 0, 0, ErrKeyNotFound
		}

		tvc := tv.Copy()
		return tvc.Value, tvc.Ts, uint64(it.Entries()), err
	}
}

func (snap *TBTreeSnapshot) History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []TimedValue, hCount uint64, err error) {
	if snap.isWriteSnapshot {
		return nil, 0, fmt.Errorf("cannot query history: %w", ErrSnapshotNotFlushed)
	}

	it, err := snap.NewHistoryIterator(key, snap.maxTs)
	if err != nil {
		return nil, 0, err
	}

	if offset == uint64(it.Entries()) {
		return nil, 0, ErrNoMoreEntries
	}

	if offset > uint64(it.Entries()) {
		return nil, 0, ErrOffsetOutOfRange
	}

	if it.Entries() == 0 {
		return nil, 0, nil
	}

	var end = it.Entries() - int(offset)
	var start = end - limit

	if descOrder {
		start = int(offset)
		end = start + int(limit)
	}

	if start < 0 {
		start = 0
	}

	if end > it.Entries() {
		end = it.Entries()
	}

	if start >= end {
		return nil, uint64(it.Entries()), ErrInvalidHistoryOffset
	}

	if _, err := it.Discard(uint64(start)); err != nil {
		return nil, 0, err
	}

	n := end - start
	values := make([]TimedValue, n)
	for n := 0; n < end-start; n++ {
		tv, err := it.Next()
		if err != nil {
			return nil, 0, err // TODO: handle unexpected ErrStopIteration error
		}
		values[n] = tv.Copy()
	}

	if !descOrder {
		reverseSlice(values)
	}
	return values, uint64(it.Entries()), nil
}

func reverseSlice[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func (snap *TBTreeSnapshot) NewIterator(opts IteratorOptions) (Iterator, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	it := &TBTreeIterator{
		opts:       opts,
		currPageID: PageNone,
		currPage:   nil,
		tree:       snap.tree,
		rootID:     snap.rootID,
		maxTs:      snap.maxTs,
		stack:      container.NewStack[PageID](100),
		onClose: func() error {
			atomic.AddUint32(&snap.numIterators, ^uint32(0))
			return nil
		},
	}
	if it.rootID != PageNone {
		it.stack.Push(it.rootID)
	}
	atomic.AddUint32(&snap.numIterators, 1)

	return it, nil
}

func (snap *TBTreeSnapshot) Set(key []byte, value []byte) error {
	return ErrCannotModifySnapshot
}

func (snap *TBTreeSnapshot) Ts() uint64 {
	return snap.maxTs
}

func (snap *TBTreeSnapshot) Close() error {
	if snap.closed {
		return nil
	}

	if atomic.LoadUint32(&snap.numIterators) > 0 {
		return ErrReadersNotClosed
	}

	atomic.AddUint64(&snap.tree.snapshotCount, ^uint64(0))

	if snap.isWriteSnapshot {
		snap.tree.mtx.RUnlock()
	}
	snap.closed = true

	return nil
}

type localSnapshot struct {
	snap Snapshot

	tree *radix.Tree
}

func (snap *localSnapshot) UseEntry(key []byte, onEntry func(e *Entry) error) error {
	if snap.tree != nil {
		value, hc, err := snap.tree.Get(key)
		if err == nil {
			return onEntry(&Entry{
				Ts:    snap.localTs(),
				HOff:  OffsetNone,
				HC:    hc + 1,
				Key:   key,
				Value: value,
			})
		}
	}
	return snap.snap.UseEntry(key, onEntry)
}

func (snap *localSnapshot) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	if snap.tree != nil {
		v, hc, err := snap.tree.Get(key)
		if err == nil {
			return v, snap.localTs(), hc + 1, nil
		}
	}
	return snap.snap.Get(key)
}

func (snap *localSnapshot) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error) {
	it, err := snap.NewIterator(DefaultIteratorOptions())
	if err != nil {
		return nil, nil, 0, 0, err
	}
	defer it.Close()

	return getWithPrefix(it, prefix, neq)
}

func (snap *localSnapshot) GetBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	if finalTs >= snap.snap.Ts() && snap.tree != nil {
		value, hc, err := snap.tree.Get(key)
		if err == nil {
			return value, snap.localTs(), hc, nil
		}
		if !errors.Is(err, radix.ErrKeyNotFound) {
			return nil, 0, 0, err
		}
	}
	return snap.snap.GetBetween(key, initialTs, finalTs)
}

func (snap *localSnapshot) History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []TimedValue, hCount uint64, err error) {
	values, hc, err := snap.snap.History(key, offset, descOrder, limit)
	if errors.Is(err, ErrInvalidHistoryOffset) && offset == hc {
		err = nil
	}
	if err != nil {
		return nil, 0, err
	}

	if snap.tree == nil || len(values) >= limit {
		return values, hc, err
	}

	v, _, err := snap.tree.Get(key)
	if errors.Is(err, radix.ErrKeyNotFound) {
		return values, hc, nil
	}
	if err != nil {
		return nil, 0, err
	}

	values = append(values, TimedValue{
		Value: v,
		Ts:    snap.snap.Ts() + 1,
	})
	return values, hc + 1, nil
}

func (snap *localSnapshot) NewIterator(opts IteratorOptions) (Iterator, error) {
	it, err := snap.snap.NewIterator(opts)
	if snap.tree == nil {
		return it, err
	}

	if err != nil {
		return nil, err
	}

	return &MergeIterator{
		iterA: it,
		iterB: &radixTreeIterator{
			Iterator: snap.tree.NewIterator(opts.Reversed),
			ts:       snap.localTs(),
		},
		reversed: opts.Reversed,
	}, nil
}

func (snap *localSnapshot) Set(key []byte, value []byte) error {
	if snap.tree == nil {
		snap.tree = radix.New()
	}
	_, _, hc, err := snap.snap.Get(key)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	return snap.tree.Insert(key, value, hc)
}

func (snap *localSnapshot) Ts() uint64 {
	return snap.snap.Ts()
}

func (snap *localSnapshot) localTs() uint64 {
	return snap.Ts() + 1
}

func (snap *localSnapshot) Close() error {
	return snap.snap.Close()
}
