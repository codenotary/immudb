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
	"errors"
	"fmt"
	"sync/atomic"
)

const ChunkNone = -1

var (
	ErrWriteBufferFull  = errors.New("write buffer is full")
	ErrInvalidPageID    = errors.New("invalid page id")
	ErrNoChunkAvailable = errors.New("no chunk available")
)

type SharedWriteBuffer struct {
	buf   []byte
	state []atomic.Bool
	hand  atomic.Uint32

	chunkSize       int
	allocatedChunks atomic.Uint32
}

func NewSharedWriteBuffer(
	size,
	chunkSize int,
) *SharedWriteBuffer {
	if chunkSize%PageSize != 0 {
		chunkSize = ((chunkSize + (PageSize - 1)) / PageSize) * PageSize
	}
	assert(chunkSize%PageSize == 0, "chunkSize % PageSize != 0")

	numChunks := (size + (chunkSize - 1)) / chunkSize

	return &SharedWriteBuffer{
		buf:       make([]byte, numChunks*chunkSize),
		state:     make([]atomic.Bool, numChunks),
		chunkSize: chunkSize,
	}
}

func (wb *SharedWriteBuffer) AllocPageChunk() int {
	n := uint32(wb.NumChunks())

	hand := wb.hand.Load()
	for i := hand; i < n; i++ {
		numChunk := i % n
		if wb.state[numChunk].CompareAndSwap(false, true) {
			// NOTE: hand might have been changed by Release()
			wb.reset(int(numChunk))
			wb.hand.CompareAndSwap(hand, (i+1)%n)
			wb.allocatedChunks.Add(1)

			return int(i)
		}
	}
	return ChunkNone
}

func (wb *SharedWriteBuffer) reset(numChunk int) {
	off := numChunk * wb.chunkSize
	chunk := wb.buf[off : off+wb.chunkSize]
	for i := range chunk {
		chunk[i] = 0
	}
}

func (wb *SharedWriteBuffer) ReleaseAll() {
	for i := range wb.state {
		wb.state[i].Store(false)
	}
}

func (wb *SharedWriteBuffer) Release(numChunk int) {
	wasAllocated := wb.state[numChunk].Swap(false)
	assert(wasAllocated, "attempt to release a non allocated slot")

	wb.hand.Store(uint32(numChunk))
	wb.allocatedChunks.Add(^uint32(0))
}

func (wb *SharedWriteBuffer) GetPageBuf(pgSlot int) []byte {
	off := pgSlot * PageSize
	return wb.buf[off : off+PageSize]
}

func (wb *SharedWriteBuffer) NumChunks() int {
	return len(wb.buf) / wb.chunkSize
}

func (wb *SharedWriteBuffer) PagesPerChunk() int {
	return wb.ChunkSize() / PageSize
}

func (wb *SharedWriteBuffer) ChunkSize() int {
	return wb.chunkSize
}

func (wb *SharedWriteBuffer) NumPages() int {
	return wb.PagesPerChunk() * wb.NumChunks()
}

func (wb *SharedWriteBuffer) AllocatedChunks() int {
	return int(wb.allocatedChunks.Load())
}

func (wb *SharedWriteBuffer) Size() int {
	return wb.NumChunks() * wb.ChunkSize()
}

type WriteBuffer struct {
	swb *SharedWriteBuffer

	bufferChunks []uint32

	minReservedChunks            int
	reservedChunks               int
	allocatedChunks              int
	allocatedPagesInCurrentChunk int

	numAllocs atomic.Uint32
}

func NewWriteBuffer(
	swb *SharedWriteBuffer,
	minSize,
	maxSize int,
) (*WriteBuffer, error) {
	minChunks := (minSize + (swb.ChunkSize() - 1)) / swb.ChunkSize()
	maxChunks := (maxSize + (swb.ChunkSize() - 1)) / swb.ChunkSize()

	if minChunks > maxChunks {
		return nil, fmt.Errorf("min chunks cannot be greater than max chunks")
	}

	wb := &WriteBuffer{
		swb:                          swb,
		bufferChunks:                 make([]uint32, maxChunks),
		minReservedChunks:            minChunks,
		reservedChunks:               0,
		allocatedChunks:              0,
		allocatedPagesInCurrentChunk: swb.ChunkSize() / PageSize,
	}

	if !wb.Grow(minChunks * swb.PagesPerChunk()) {
		return nil, fmt.Errorf("unable to reserve minimum number of write buffer chunks")
	}
	return wb, nil
}

func (wb *WriteBuffer) Get(pgID PageID) (*Page, error) {
	buf, err := wb.getRawPage(pgID)
	if err != nil {
		return nil, err
	}
	return PageFromBytes(buf), nil
}

func (wb *WriteBuffer) getRawPage(pgID PageID) ([]byte, error) {
	if !pgID.isMemPage() {
		return nil, fmt.Errorf("%w: not an in memory page", ErrInvalidPageID)
	}

	buf := wb.swb.GetPageBuf(pgID.slot())
	return buf, nil
}

func (wb *WriteBuffer) GetHistoryPage(pgID PageID) (*HistoryPage, error) {
	pg, err := wb.getRawPage(pgID)
	if err != nil {
		return nil, err
	}
	return HistoryPageFromBytes(pg), nil
}

func (wb *WriteBuffer) GetOrDup(pgID PageID, dupPage func(PageID, []byte) error) (*Page, PageID, error) {
	pgSlot := pgID.slot()
	if pgID.isMemPage() && pgSlot*PageSize >= wb.swb.Size() {
		return nil, PageNone, fmt.Errorf("%w: not an in memory page", ErrInvalidPageID)
	}

	if pgID.isMemPage() {
		return PageFromBytes(wb.swb.GetPageBuf(pgSlot)), pgID, nil
	}

	slot, err := wb.allocPageSlot()
	if err != nil {
		return nil, PageNone, err
	}

	pgBuf := wb.swb.GetPageBuf(slot)
	if err := dupPage(pgID, pgBuf); err != nil {
		wb.deallocSlot()
		return nil, PageNone, err
	}

	wb.numAllocs.Add(1)

	newPgID := memPageID(slot)
	pg := PageFromBytes(pgBuf)
	pg.SetAsCopied()

	return pg, newPgID, nil
}

func (wb *WriteBuffer) allocPageSlot() (int, error) {
	pagesPerChunk := wb.swb.PagesPerChunk()
	if wb.allocatedPagesInCurrentChunk < wb.swb.PagesPerChunk() {
		globalPageSlot := wb.bufferChunks[wb.allocatedChunks-1] * uint32(pagesPerChunk)
		pageSlot := wb.allocatedPagesInCurrentChunk
		wb.allocatedPagesInCurrentChunk++
		return int(globalPageSlot) + pageSlot, nil
	}

	if wb.allocatedChunks == len(wb.bufferChunks) {
		return -1, ErrWriteBufferFull
	}

	numChunk, err := wb.allocPageChunk()
	if err != nil {
		return -1, err
	}

	wb.bufferChunks[wb.allocatedChunks] = uint32(numChunk)
	wb.allocatedChunks++
	wb.allocatedPagesInCurrentChunk = 1

	return numChunk * pagesPerChunk, nil
}

func (wb *WriteBuffer) allocPageChunk() (int, error) {
	if wb.allocatedChunks < wb.reservedChunks {
		slot := wb.bufferChunks[wb.allocatedChunks]
		return int(slot), nil
	}

	if wb.allocatedChunks == len(wb.bufferChunks) {
		return -1, ErrWriteBufferFull
	}

	numChunk := wb.swb.AllocPageChunk()
	if numChunk == ChunkNone {
		return -1, ErrNoChunkAvailable
	}
	return numChunk, nil
}

func (wb *WriteBuffer) deallocSlot() {
	assert(wb.allocatedPagesInCurrentChunk > 0, "currPageSlot <= 0")
	wb.allocatedPagesInCurrentChunk--
}

func (wb *WriteBuffer) AllocLeafPage() (*Page, PageID, error) {
	return wb.allocPage(true)
}

func (wb *WriteBuffer) AllocInnerPage() (*Page, PageID, error) {
	return wb.allocPage(false)
}

func (wb *WriteBuffer) AllocHistoryPage() (*HistoryPage, PageID, error) {
	buf, pgID, err := wb.allocPageBuf()
	if err != nil {
		return nil, PageNone, err
	}

	pg := HistoryPageFromBytes(buf)
	pg.init()
	return pg, pgID, nil
}

func (wb *WriteBuffer) allocPage(leaf bool) (*Page, PageID, error) {
	buf, pgID, err := wb.allocPageBuf()
	if err != nil {
		return nil, PageNone, err
	}

	pg := PageFromBytes(buf)
	pg.init(leaf)
	return pg, pgID, nil
}

func (wb *WriteBuffer) allocPageBuf() ([]byte, PageID, error) {
	slot, err := wb.allocPageSlot()
	if err != nil {
		return nil, PageNone, err
	}

	wb.numAllocs.Add(1)

	pgBuf := wb.swb.GetPageBuf(slot)
	return pgBuf, memPageID(slot), nil
}

func (wb *WriteBuffer) Grow(numPages int) bool {
	for wb.availablePages() < numPages {
		chunk := wb.swb.AllocPageChunk()
		if chunk == ChunkNone {
			return false
		}
		wb.bufferChunks[wb.reservedChunks] = uint32(chunk)
		wb.reservedChunks++
	}
	return true
}

func (wb *WriteBuffer) availablePages() int {
	if wb.reservedChunks == 0 {
		return 0
	}

	numPages := (wb.reservedChunks - wb.allocatedChunks) * wb.swb.PagesPerChunk()
	numPages += wb.swb.PagesPerChunk() - wb.allocatedPagesInCurrentChunk

	return numPages
}

func (wb *WriteBuffer) Pages() int {
	if wb.reservedChunks == 0 {
		return 0
	}
	return wb.reservedChunks * wb.swb.PagesPerChunk()
}

func (wb *WriteBuffer) UsedPages() int {
	return wb.allocatedChunks*wb.swb.PagesPerChunk() - (wb.swb.PagesPerChunk() - wb.allocatedPagesInCurrentChunk)
}

func (wb *WriteBuffer) FreePages() int {
	return wb.availablePages()
}

func (wb *WriteBuffer) Allocs() int {
	return int(wb.numAllocs.Load())
}

func (wb *WriteBuffer) Reset() {
	for i := wb.minReservedChunks; i < wb.reservedChunks; i++ {
		wb.swb.Release(int(wb.bufferChunks[i]))
	}

	wb.allocatedChunks = 0
	wb.reservedChunks = wb.minReservedChunks
	wb.allocatedPagesInCurrentChunk = wb.swb.PagesPerChunk()
}

func roundUpPages(n int) int {
	return (n + (PageSize - 1)) / PageSize
}

func memPageID(slot int) PageID {
	return PageID((1 << 47) | slot)
}
