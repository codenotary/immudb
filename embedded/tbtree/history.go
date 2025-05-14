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
	"encoding/binary"
	"errors"

	"github.com/codenotary/immudb/v2/embedded/appendable"
)

var (
	ErrValueTooLarge  = errors.New("value is too large")
	ErrCorruptedEntry = errors.New("entry is corrupted")
)

type HistoryEntry struct {
	PrevOffset uint64
	Ts         uint64
	Value      []byte
}

func (e *HistoryEntry) requiredSpace() int {
	return 8 + // PrevOffset
		8 + // Ts
		2 + // ValueSize
		len(e.Value)
}

type HistoryIterator struct {
	valueBuf        [MaxEntrySize]byte
	e               entryInfo
	historyApp      appendable.Appendable
	consumedEntries int
	n               int
}

type entryInfo struct {
	Ts        uint64
	HOff      uint64
	ValueSize uint16
}

func (snap *TBTreeSnapshot) NewHistoryIterator(
	key []byte,
	maxTs uint64,
) (*HistoryIterator, error) {
	return newHistoryIterator(snap.tree.historyLog, snap, key, maxTs)
}

func newHistoryIterator(
	historyApp appendable.Appendable,
	snap Snapshot,
	key []byte,
	maxTs uint64,
) (*HistoryIterator, error) {
	it := &HistoryIterator{
		e:          entryInfo{HOff: OffsetNone},
		historyApp: historyApp,
	}

	err := snap.UseEntry(key, func(e *Entry) error {
		it.e = entryInfo{
			Ts:        e.Ts,
			HOff:      e.HOff,
			ValueSize: uint16(len(e.Value)),
		}
		it.n = int(e.HC) + 1

		if len(e.Value) > MaxEntrySize {
			return ErrCorruptedEntry
		}
		copy(it.valueBuf[:], e.Value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	_, err = it.discardUpToTx(maxTs)
	return it, err
}

func (it *HistoryIterator) Discard(n uint64) (int, error) {
	if n == 0 {
		return 0, nil
	}

	discarded := 0
	for discarded < int(n) {
		_, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return discarded, err
		}
		discarded++
	}
	return discarded, nil
}

func (it *HistoryIterator) discardUpToTx(txID uint64) (int, error) {
	discarded := 0
	for it.currTs() > txID {
		_, err := it.Next()
		if errors.Is(err, ErrNoMoreEntries) {
			it.n = 0
			it.consumedEntries = 0
			return discarded, nil
		}
		if err != nil {
			return discarded, err
		}
		discarded++
	}

	if discarded == 0 {
		return 0, nil
	}

	it.n -= discarded - 1
	it.consumedEntries = 0
	return discarded - 1, nil
}

func (it *HistoryIterator) Next() (*TimedValue, error) {
	if it.n == 0 {
		return nil, ErrNoMoreEntries
	}

	if it.consumedEntries > 0 {
		if err := it.seekNext(); err != nil {
			return nil, err
		}
	}
	it.consumedEntries++

	return &TimedValue{
		Ts:    it.e.Ts,
		Value: it.valueBuf[:it.e.ValueSize],
	}, nil
}

func (it *HistoryIterator) currTs() uint64 {
	return it.e.Ts
}

func (it *HistoryIterator) Entries() int {
	return it.n
}

func (it *HistoryIterator) Revision() int {
	return it.Entries() - it.consumedEntries
}

func (it *HistoryIterator) seekNext() error {
	if it.e.HOff == OffsetNone {
		return ErrNoMoreEntries
	}

	vlen, ts, hoff, err := readHistoryEntry(it.historyApp, int64(it.e.HOff), it.valueBuf[:])
	if err != nil {
		return err
	}

	it.e = entryInfo{
		Ts:        ts,
		ValueSize: vlen,
		HOff:      hoff,
	}
	return nil
}

const HistoryEntrySize = 8 + 8 + 2 // PrevOffset + Ts + ValueSize

func readHistoryEntry(app appendable.Appendable, off int64, valueBuf []byte) (vlen uint16, ts uint64, hoff uint64, err error) {
	var buf [HistoryEntrySize]byte

	_, err = app.ReadAt(buf[:], off)
	if err != nil {
		return 0, 0, 0, err
	}

	vlen = binary.BigEndian.Uint16(buf[16:])
	if int(vlen) > MaxEntrySize {
		return 0, 0, 0, ErrCorruptedEntry
	}

	_, err = app.ReadAt(valueBuf[:vlen], off+HistoryEntrySize)
	if err != nil {
		return 0, 0, 0, err
	}

	hoff = binary.BigEndian.Uint64(buf[:])
	ts = binary.BigEndian.Uint64(buf[8:])

	return vlen,
		ts,
		hoff,
		nil
}
