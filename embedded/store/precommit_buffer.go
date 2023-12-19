/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
)

var ErrBufferIsFull = errors.New("buffer is full")
var ErrBufferFullyConsumed = errors.New("buffer fully consumed")
var ErrNotEnoughData = fmt.Errorf("%w: not enough data", ErrBufferFullyConsumed)

type precommittedEntry struct {
	txID   uint64
	alh    [sha256.Size]byte
	txOff  int64
	txSize int
}

// precommitBuffer is a read-ahead circular buffer
// this buffer is used to hold a portion of the clog in memory:
//   - entries put into the buffer as they are precommitted
//   - entries are removed from the buffer as they are committed (content was successfully written into clog)
type precommitBuffer struct {
	buf []*precommittedEntry

	rpos int // buf read position
	wpos int // buf write position

	full bool

	mux sync.Mutex
}

func newPrecommitBuffer(size int) *precommitBuffer {
	b := make([]*precommittedEntry, size)

	for i := range b {
		b[i] = &precommittedEntry{}
	}

	return &precommitBuffer{
		buf: b,
	}
}

func (b *precommitBuffer) freeSlots() int {
	if b.full {
		return 0
	}

	if b.rpos <= b.wpos {
		return len(b.buf) - (b.wpos - b.rpos)
	}

	return b.rpos - b.wpos
}

func (b *precommitBuffer) put(txID uint64, alh [sha256.Size]byte, txOff int64, txSize int) error {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.full {
		return ErrBufferIsFull
	}

	b.wpos = (b.wpos + 1) % len(b.buf)

	e := b.buf[b.wpos]

	e.txID = txID
	e.alh = alh
	e.txOff = txOff
	e.txSize = txSize

	b.full = b.rpos == b.wpos

	return nil
}

func (b *precommitBuffer) recedeWriter(n int) error {
	if n <= 0 {
		return ErrIllegalArguments
	}

	if len(b.buf)-b.freeSlots() < n {
		return ErrNotEnoughData
	}

	b.wpos = (b.wpos + len(b.buf) - n) % len(b.buf)

	b.full = false

	return nil
}

func (b *precommitBuffer) readAhead(n int) (txID uint64, alh [sha256.Size]byte, txOff int64, txSize int, err error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if n < 0 {
		err = ErrIllegalArguments
		return
	}

	if len(b.buf)-b.freeSlots() <= n {
		err = ErrNotEnoughData
		return
	}

	rpos := (b.rpos + n + 1) % len(b.buf)

	e := b.buf[rpos]

	txID = e.txID
	alh = e.alh
	txOff = e.txOff
	txSize = e.txSize

	return
}

func (b *precommitBuffer) advanceReader(n int) error {
	if n <= 0 {
		return ErrIllegalArguments
	}

	if len(b.buf)-b.freeSlots() < n {
		return ErrNotEnoughData
	}

	b.rpos = (b.rpos + n) % len(b.buf)
	b.full = false

	return nil
}
