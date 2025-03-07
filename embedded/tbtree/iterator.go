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
	"math"

	"github.com/codenotary/immudb/embedded/container"
	"github.com/codenotary/immudb/embedded/radix"
)

var (
	ErrNoSnapshotAvailable = errors.New("no snapshot available")
)

type IteratorOptions struct {
	StartTs        uint64
	EndTs          uint64
	IncludeHistory bool
	Reversed       bool
}

func (opts *IteratorOptions) validate() error {
	if opts.StartTs > opts.EndTs {
		return fmt.Errorf("%w: start timestamp cannot be greater than end timestamp", ErrInvalidOptions)
	}

	if opts.IncludeHistory && (opts.StartTs != 0 || opts.EndTs != math.MaxUint64) {
		return fmt.Errorf("%w: start timestamp and end timestamp cannot be used when include history is enabled", ErrInvalidOptions)
	}
	return nil
}

type Iterator interface {
	Seek(key []byte) error
	Next() (*Entry, error)
	Close() error
}

type TBTreeIterator struct {
	tree *TBTree

	// TODO: only allocate a single page per iterator
	// that can be reused

	opts   IteratorOptions
	rootID PageID
	maxTs  uint64

	currHistoryKey []byte
	currHistoryIt  *HistoryIterator

	currPageID PageID
	currPage   *Page
	nextIdx    int
	stack      *container.Stack[PageID]
}

func DefaultIteratorOptions() IteratorOptions {
	return IteratorOptions{
		StartTs:        0,
		EndTs:          math.MaxUint64,
		Reversed:       false,
		IncludeHistory: false,
	}
}

func (t *TBTree) NewIterator(opts IteratorOptions) (Iterator, error) {
	snap, err := t.WriteSnapshot()
	if err != nil {
		return nil, err
	}
	return snap.NewIterator(opts)
}

func traverse(
	t *TBTree,
	rootPageID PageID,
	key []byte,
	onPage func(pg *Page, ip PageID) (bool, error),
) error {
	for currPageID := rootPageID; currPageID != PageNone; {
		pg, err := t.getPage(currPageID)
		if err != nil {
			return err
		}

		shouldRelease, err := onPage(pg, currPageID)

		releasePage := func() {
			if shouldRelease {
				t.release(currPageID)
			}
		}

		if err != nil {
			releasePage()
			return err
		}

		if pg.IsLeaf() {
			releasePage()
			break
		}

		_, childPageID, err := pg.Find(key)
		if err != nil {
			return err
		}

		releasePage()

		currPageID = childPageID
	}
	return nil
}

func (it *TBTreeIterator) Seek(key []byte) error {
	it.reset()

	return traverse(
		it.tree,
		it.rootID,
		key,
		func(pg *Page, pgID PageID) (bool, error) {
			idx, found := pg.find(key)
			if pg.IsLeaf() {
				it.currPageID = pgID
				it.currPage = pg

				if it.opts.Reversed && !found {
					it.nextIdx = idx - 1
				} else {
					it.nextIdx = idx
				}
				return false, nil
			}

			if it.opts.Reversed {
				end := idx
				if !found {
					end--
				}

				for i := 0; i < end; i++ {
					it.stack.Push(pg.ChildPageAt(i))
				}
			} else {
				end := idx
				if found {
					end++
				}

				for i := int(pg.NumEntries) - 1; i >= end; i-- {
					it.stack.Push(pg.ChildPageAt(i))
				}
			}
			return true, nil
		})
}

func (it *TBTreeIterator) Next() (*Entry, error) {
	endTs := it.opts.EndTs
	if it.maxTs < endTs {
		endTs = it.maxTs
	}

	e, err := it.nextBetween(it.opts.StartTs, endTs)
	if err == nil {
		// TODO: this is a temporary hack to prevent in place modifications
		// from caller code.
		cp := e.Copy()
		return &cp, err
	}
	return e, err
}

func (it *TBTreeIterator) nextBetween(initialTs, finalTs uint64) (*Entry, error) {
	if initialTs > finalTs {
		return nil, ErrNoMoreEntries
	}

	e, exists, err := it.seekToNextEntry(initialTs, finalTs)
	if err != nil {
		return nil, err
	}
	if exists {
		return e, nil
	}

	it.releaseCurrentPage()

	for {
		pgID, ok := it.stack.Pop()
		if !ok {
			return nil, ErrNoMoreEntries
		}

		pg, err := it.tree.getPage(pgID)
		if err != nil {
			return nil, err
		}

		if pg.IsLeaf() {
			it.currPageID = pgID
			it.currPage = pg
			if it.opts.Reversed {
				it.nextIdx = int(it.currPage.NumEntries) - 1
			} else {
				it.nextIdx = 0
			}

			e, exists, err := it.seekToNextEntry(initialTs, finalTs)
			if err != nil {
				return nil, err
			}

			if exists {
				return e, nil
			}

			it.currPageID = PageNone
			it.currPage = nil
		} else {
			it.pushInnerPage(pg)
		}

		it.tree.release(pgID)
	}
}

func (it *TBTreeIterator) releaseCurrentPage() {
	if it.currPage != nil {
		it.tree.release(it.currPageID)
		it.currPage = nil
		it.currPageID = PageNone
	}
}

func (it *TBTreeIterator) pushInnerPage(pg *Page) {
	if !it.opts.Reversed {
		for i := int(pg.NumEntries) - 1; i >= 0; i-- {
			it.stack.Push(pg.ChildPageAt(i))
		}
		return
	}

	for i := 0; i < int(pg.NumEntries); i++ {
		it.stack.Push(pg.ChildPageAt(i))
	}
}

func (it *TBTreeIterator) seekToNextEntry(initialTs, finalTs uint64) (*Entry, bool, error) {
	if it.currPage == nil {
		return nil, false, nil
	}

	e, exists, err := it.nextHistoryEntry()
	if err != nil {
		return nil, false, err
	}
	if exists {
		return &e, true, nil
	}

	for it.nextIdx >= 0 && it.nextIdx < int(it.currPage.NumEntries) {
		idx := it.nextIdx
		if it.opts.Reversed {
			it.nextIdx--
		} else {
			it.nextIdx++
		}

		e, err := it.currPage.GetEntryAt(idx)
		if err != nil {
			return nil, false, err
		}

		e, exists, err := it.nextEntryBetween(&e, initialTs, finalTs)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return &e, true, nil
		}
	}
	return nil, false, nil
}

func (it *TBTreeIterator) nextHistoryEntry() (Entry, bool, error) {
	if it.currHistoryIt != nil {
		assert(it.opts.IncludeHistory, "include history must be true")

		tv, err := it.currHistoryIt.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			return Entry{}, false, nil
		}

		return Entry{
			Ts:   tv.Ts,
			HOff: it.currHistoryIt.e.HOff,
			// TODO:	HC:    e.HC - uint64(discarded),
			HC:    uint64(it.currHistoryIt.Revision()),
			Key:   it.currHistoryKey,
			Value: tv.Value,
		}, true, nil
	}

	it.currHistoryIt = nil
	it.currHistoryKey = nil

	return Entry{}, false, nil
}

func (it *TBTreeIterator) nextEntryBetween(e *Entry, initialTs, finalTs uint64) (Entry, bool, error) {
	if e.Ts < initialTs {
		return Entry{}, false, nil
	}

	getHistoryIt := func() *HistoryIterator {
		return &HistoryIterator{
			e: entryInfo{
				Ts:        e.Ts,
				HOff:      e.HOff,
				ValueSize: uint16(len(e.Value)),
			},
			historyApp:      it.tree.historyLog,
			n:               int(e.HC) + 1,
			consumedEntries: 0,
		}
	}

	if e.Ts <= finalTs {
		if it.opts.IncludeHistory {
			historyIt := getHistoryIt()

			historyIt.consumedEntries = 1
			it.currHistoryKey = e.Key
			it.currHistoryIt = historyIt
		}
		return *e, true, nil
	}

	historyIt := getHistoryIt()

	discarded, err := historyIt.discardUpToTx(finalTs)
	if err != nil {
		return Entry{}, false, err
	}

	// TODO: avoid historyIt.buf to be moved to the heap
	// by preallocating an iterator buffer.
	tv, err := historyIt.Next()
	if errors.Is(err, ErrNoMoreEntries) || tv.Ts < initialTs {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, nil
	}

	if it.opts.IncludeHistory {
		it.currHistoryKey = e.Key
		it.currHistoryIt = historyIt
	}

	assert(tv.Ts >= initialTs && tv.Ts <= finalTs, "ts mismatch")
	assert(discarded <= int(e.HC), "discarded > hc")

	newEntry := Entry{
		Ts:    tv.Ts,
		HOff:  historyIt.e.HOff,
		HC:    uint64(historyIt.Revision()),
		Key:   e.Key,
		Value: tv.Value,
	}
	return newEntry, true, err
}

func (it *TBTreeIterator) Close() error {
	it.reset()
	return nil
}

func (it *TBTreeIterator) reset() {
	it.stack.Reset()
	it.releaseCurrentPage()
	it.currHistoryIt = nil
	it.currHistoryKey = nil
}

type radixTreeIterator struct {
	*radix.Iterator
	ts uint64
}

func (it *radixTreeIterator) NextBetween(initialTs, finalTs uint64) (*Entry, error) {
	if finalTs < it.ts {
		return nil, ErrNoMoreEntries
	}
	return it.Next()
}

func (it *radixTreeIterator) Next() (*Entry, error) {
	e, err := it.Iterator.Next()
	if errors.Is(err, radix.ErrStopIteration) {
		return nil, ErrNoMoreEntries
	}

	return &Entry{
		Key:   e.Key,
		Value: e.Value,
		HOff:  OffsetNone,
		HC:    e.HC,
		Ts:    it.ts,
	}, nil
}

func (it *radixTreeIterator) Close() error {
	return nil
}

type MergeIterator struct {
	iterA, iterB Iterator

	reversed     bool
	nextA, nextB *Entry
}

func (it *MergeIterator) Seek(key []byte) error {
	if err := it.iterA.Seek(key); err != nil {
		return err
	}

	if err := it.iterB.Seek(key); err != nil {
		return err
	}

	it.nextA = nil
	it.nextB = nil
	return nil
}

func (it *MergeIterator) Next() (*Entry, error) {
	if err := it.advance(); err != nil {
		return nil, err
	}

	if it.nextA == nil && it.nextB == nil {
		return nil, ErrNoMoreEntries
	}

	if it.nextA == nil {
		next := it.nextB
		it.nextB = nil
		return next, nil
	}

	if it.nextB == nil {
		next := it.nextA
		it.nextA = nil
		return next, nil
	}

	res := bytes.Compare(it.nextA.Key, it.nextB.Key)
	if res == 0 {
		var next *Entry
		if it.nextA.Ts > it.nextB.Ts {
			next = it.nextA
		} else {
			next = it.nextB
		}
		it.nextA = nil
		it.nextB = nil
		return next, nil
	}

	if (res > 0) == it.reversed {
		next := it.nextA
		it.nextA = nil
		return next, nil
	}

	next := it.nextB
	it.nextB = nil
	return next, nil
}

func (it *MergeIterator) advance() error {
	if it.nextA == nil {
		next, err := advanceIter(it.iterA)
		if err != nil {
			return err
		}
		it.nextA = next
	}

	if it.nextB == nil {
		next, err := advanceIter(it.iterB)
		if err != nil {
			return err
		}
		it.nextB = next
	}
	return nil
}

func advanceIter(it Iterator) (*Entry, error) {
	e, err := it.Next()
	if errors.Is(err, ErrNoMoreEntries) {
		return nil, nil
	}
	return e, err
}

func (it *MergeIterator) Close() error {
	if err := it.iterA.Close(); err != nil {
		return err
	}
	return it.iterB.Close()
}
