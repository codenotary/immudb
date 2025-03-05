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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"unsafe"
)

var (
	ErrCorruptedTreeLog    = errors.New("tree log is corrupted")
	ErrPageFull            = errors.New("page is full")
	ErrNotLeafPage         = errors.New("not a leaf page")
	ErrNotInnerPage        = errors.New("not an inner page")
	ErrKeyRevisionNotFound = errors.New("key revision not found")
)

const (
	LeafPageEntryDataSize  = int(unsafe.Sizeof(LeafPageEntryData{}))
	InnerPageEntryDataSize = int(unsafe.Sizeof(InnerPageEntryData{}))
	PageHeaderDataSize     = int(unsafe.Sizeof(PageHeaderData{}))
	PageSize               = 4096
	MaxFreePageSpace       = PageSize - PageHeaderDataSize
	MaxEntrySize           = MaxFreePageSpace / 2

	LeafNodeFlag = 1 << 0
	RootFlag     = 1 << 1
	CopiedFlag   = 1 << 2

	PageNone   = PageID(1<<48 - 1)
	MaxPageID  = PageNone
	OffsetNone = uint64(1 << 63)

	ChecksumSize = 4

	CommitMagic     = 0xC435
	CommitMagicSize = 2
)

type PageID uint64

func (id PageID) isMemPage() bool {
	return id&PageID(1<<47) != 0
}

const slotMsk = ^(uint64(1) << 47)

func (id PageID) slot() int {
	return int(id & PageID(slotMsk))
}

type Entry struct {
	Ts    uint64
	HOff  uint64
	HC    uint64
	Key   []byte
	Value []byte
}

func (e *Entry) requiredPageItemSize() int {
	return requiredPageItemSize(len(e.Key), len(e.Value))
}

func (e *Entry) Copy() Entry {
	return Entry{
		Ts:    e.Ts,
		HOff:  e.HOff,
		HC:    e.HC,
		Key:   cp(e.Key),
		Value: cp(e.Value),
	}
}

func cp(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

type CommitEntry struct {
	Ts                uint64
	TLogOff           uint64
	HLogOff           uint64
	HLogFlushedBytes  uint32
	TotalPages        uint64
	StalePages        uint32
	IndexedEntryCount uint32
	HLogChecksum      [sha256.Size]byte
	TLogChecksum      [sha256.Size]byte
}

type PageHeaderData struct {
	Flags          uint16
	NumEntries     uint16
	FreeSpaceStart uint16
	FreeSpaceEnd   uint16
	UnusedSpace    uint16
	Padding        uint16 // take into account space for the size field
}

func (hdr *PageHeaderData) Put(buf []byte) int {
	off := 0

	binary.BigEndian.PutUint16(buf[off:], hdr.Flags)
	off += 2

	binary.BigEndian.PutUint16(buf[off:], hdr.NumEntries)
	off += 2

	binary.BigEndian.PutUint16(buf[off:], hdr.FreeSpaceStart)
	off += 2

	binary.BigEndian.PutUint16(buf[off:], hdr.FreeSpaceEnd)
	off += 2

	binary.BigEndian.PutUint16(buf[off:], hdr.UnusedSpace)
	off += 2

	return off
}

func (p *PageHeaderData) Read(buf []byte) {
	off := 0

	p.Flags = binary.BigEndian.Uint16(buf[off:])
	off += 2

	p.NumEntries = binary.BigEndian.Uint16(buf[off:])
	off += 2

	p.FreeSpaceStart = binary.BigEndian.Uint16(buf[off:])
	off += 2

	p.FreeSpaceEnd = binary.BigEndian.Uint16(buf[off:])
	off += 2

	p.UnusedSpace = binary.BigEndian.Uint16(buf[off:])
}

type Page struct {
	PageHeaderData
	data [PageSize - PageHeaderDataSize]byte
}

type LeafPageEntryData struct {
	Offset    uint16
	KeySize   uint16
	ValueSize uint16
}

func (e *LeafPageEntryData) dataSize() int {
	return 24 + int(e.KeySize) + int(e.ValueSize)
}

type InnerPageEntryData struct {
	Offset  uint16
	KeySize uint16
}

func (e *InnerPageEntryData) dataSize() int {
	return 8 + int(e.KeySize)
}

func newPage(isLeaf bool) *Page {
	pg := &Page{
		PageHeaderData: PageHeaderData{
			FreeSpaceEnd: uint16(PageSize - PageHeaderDataSize),
		},
	}
	pg.init(isLeaf)
	return pg
}

func (pg *Page) init(isLeaf bool) {
	pg.reset()

	if isLeaf {
		pg.SetAsLeaf()
	} else {
		pg.initInnerPage()
	}
}

func (p *Page) copy() *Page {
	newPg := *p
	return &newPg
}

func NewLeafPage() *Page {
	return newPage(true)
}

func NewInnerPage() *Page {
	return newPage(false)
}

func NewPageFromBytes(buf []byte) *Page {
	pg := PageFromBytes(buf)
	pg.reset()
	return pg
}

func NewLeafPageFromBytes(buf []byte) *Page {
	pg := PageFromBytes(buf)
	pg.init(true)
	return pg
}

func NewInnerPageFromBytes(buf []byte) *Page {
	pg := PageFromBytes(buf)
	pg.init(false)
	return pg
}

func PageFromBytes(buf []byte) *Page {
	return (*Page)(unsafe.Pointer(&buf[0]))
}

func (pg *Page) reset() {
	pg.PageHeaderData = PageHeaderData{}
	pg.FreeSpaceEnd = uint16(PageSize - PageHeaderDataSize)
}

func (pg *Page) initInnerPage() {
	assert(!pg.IsLeaf(), "not an inner page")

	pg.InsertKey(nil, PageNone)
}

func (p *Page) pop() {
	assert(!p.IsLeaf(), "pop called on a leaf")

	e := p.innerPageEntryAt(int(p.NumEntries) - 1)
	assert(e.Offset == p.FreeSpaceEnd, "invalid pop")

	p.NumEntries--
	p.FreeSpaceStart -= uint16(InnerPageEntryDataSize)
	p.FreeSpaceEnd += uint16(e.dataSize())
}

func (pg *Page) InsertKey(key []byte, id PageID) (int, error) {
	if pg.IsLeaf() {
		return -1, ErrNotInnerPage
	}

	idx, found := pg.find(key)
	if found {
		e := pg.innerPageEntryAt(idx)
		binary.BigEndian.PutUint64(pg.data[e.Offset:], uint64(id))
		return idx, nil
	}

	space := requiredInnerPageItemSize(len(key))
	if pg.FreeSpace() < space {
		return -1, ErrPageFull
	}

	newEntry := pg.pushInnerItem(key, id, space)
	pg.insertInnerEntryAt(idx, newEntry)

	pg.NumEntries++
	return idx, nil
}

func (pg *Page) pushEntry(e *Entry, dataSpace int) LeafPageEntryData {
	pg.FreeSpaceEnd -= uint16(dataSpace)

	n := int(pg.FreeSpaceEnd)
	binary.BigEndian.PutUint64(pg.data[n:], e.Ts)
	n += 8

	binary.BigEndian.PutUint64(pg.data[n:], e.HOff) // HOff
	n += 8

	binary.BigEndian.PutUint64(pg.data[n:], e.HC) // HCount
	n += 8

	copy(pg.data[n:], e.Key)
	n += len(e.Key)

	copy(pg.data[n:], e.Value)

	return LeafPageEntryData{
		Offset:    uint16(pg.FreeSpaceEnd),
		KeySize:   uint16(len(e.Key)),
		ValueSize: uint16(len(e.Value)),
	}
}

func (pg *Page) pushInnerItem(key []byte, id PageID, totalSpace int) InnerPageEntryData {
	pg.FreeSpaceEnd -= uint16(totalSpace) - uint16(InnerPageEntryDataSize)

	binary.BigEndian.PutUint64(pg.data[pg.FreeSpaceEnd:], uint64(id))
	copy(pg.data[pg.FreeSpaceEnd+8:], key)

	return InnerPageEntryData{
		Offset:  uint16(pg.FreeSpaceEnd),
		KeySize: uint16(len(key)),
	}
}

func (pg *Page) moveItemsTo(dst *Page, start, end int) {
	for i := start; i < end; i++ {
		if pg.IsLeaf() {
			e, err := pg.GetEntryAt(i)
			assert(err == nil, "get antry at")

			_, _, err = dst.InsertEntry(&e)
			if err != nil {
				panic(err)
			}
			assert(err == nil, "err != nil")
		} else {
			e := pg.innerPageEntryAt(i)
			pgID := binary.BigEndian.Uint64(pg.data[e.Offset : e.Offset+8])
			key := pg.data[e.Offset+8 : e.Offset+8+e.KeySize]
			_, err := dst.InsertKey(key, PageID(pgID))
			if err != nil {
				panic(err)
			}
		}
	}
}

func (pg *Page) entrySpaceAt(idx int) int {
	if pg.IsLeaf() {
		return pg.leafPageEntryAt(idx).dataSize() + LeafPageEntryDataSize
	}
	return pg.innerPageEntryAt(idx).dataSize() + InnerPageEntryDataSize
}

func (pg *Page) findSplitIdx(insertIdx, newItemSpace, desiredMaxSpace int) (int, int) {
	var splitIdx, space int
	for ; splitIdx < int(pg.NumEntries) && space < desiredMaxSpace; splitIdx++ {
		var s int
		switch {
		case splitIdx < insertIdx:
			s = pg.entrySpaceAt(splitIdx)
		case splitIdx == insertIdx:
			s = newItemSpace
		case splitIdx > insertIdx:
			s = pg.entrySpaceAt(splitIdx - 1)
		}

		if space+s > MaxFreePageSpace {
			return splitIdx, space
		}
		space += s
	}
	return splitIdx, space
}

func (pg *Page) compact() (int, error) {
	assert(pg.IsLeaf(), "not a leaf page")

	var tempPg Page
	tempPg.init(true)
	tempPg.Flags = pg.Flags

	i := 0
	for i < int(pg.NumEntries) {
		key := pg.keyAt(i)

		j := i + 1
		for j < int(pg.NumEntries) && bytes.Equal(pg.keyAt(j), key) {
			j++
		}

		e, err := pg.GetEntryAt(j - 1)
		if err != nil {
			return -1, err
		}

		if _, _, err := tempPg.InsertEntry(&e); err != nil {
			return -1, err
		}
		i = j
	}

	freedSpace := tempPg.FreeSpace() - pg.FreeSpace()

	*pg = tempPg

	assert(pg.NumEntries >= 1, "num entries")

	return freedSpace, nil
}

func (pg *Page) splitInnerPage(newPg *Page, key []byte, pgID, prevID PageID) int {
	assert(!pg.IsLeaf(), "splitInnerPage() called on a leaf page")

	requiredSpace := requiredInnerPageItemSize(len(key))
	spacePerPage := (pg.UsedSpace() + requiredSpace) / 2

	// assuming no page fragmentation

	idx, found := pg.find(key)

	// NOTE: found should never be true here,
	// as a split can only happen after inserting a non-existing key
	assert(!found, "splitInnerPage(): duplicate key")

	splitIdx, _ := pg.findSplitIdx(idx, requiredSpace, spacePerPage)
	assert(splitIdx > 0, "splitIdx == 0")

	var tempPage Page
	tempPage.init(pg.IsLeaf())
	tempPage.Flags = pg.Flags

	if splitIdx-1 < idx { // e belongs to the new page
		pg.moveItemsTo(&tempPage, 0, splitIdx)

		pg.moveItemsTo(newPg, splitIdx, idx)

		insertIdx, err := newPg.InsertKey(key, pgID)
		assertNoErr(err)

		pg.moveItemsTo(newPg, idx, int(pg.NumEntries))

		newPg.SetPageID(insertIdx-1, prevID)
	} else {
		pg.moveItemsTo(newPg, splitIdx-1, int(pg.NumEntries))

		pg.moveItemsTo(&tempPage, 0, idx)

		insertIdx, err := tempPage.InsertKey(key, pgID)
		assertNoErr(err)

		pg.moveItemsTo(&tempPage, idx, splitIdx-1)
		tempPage.SetPageID(insertIdx-1, prevID)
	}

	if !bytes.Equal(newPg.keyAt(1), key) {
		lastChildID := tempPage.ChildPageAt(int(tempPage.NumEntries) - 1)

		newPg.SetPageID(0, lastChildID)
	}

	tempPage.pop()

	assert(tempPage.NumEntries >= 1, "empty left page after split")
	assert(newPg.NumEntries >= 1, "empty right page split")

	*pg = tempPage

	return idx
}

func (pg *Page) splitLeafPage(newPg *Page, e *Entry) int {
	assert(pg.IsLeaf(), "splitLeafPage() called on a non leaf page")

	requiredSpace := e.requiredPageItemSize()
	maxSpaceLeft := (pg.UsedSpace() - int(pg.UnusedSpace) + requiredSpace) / 2

	idx, found := pg.find(e.Key)
	assert(!found, "splitLeafPage(): key already exists")

	splitIdx, _ := pg.findSplitIdx(idx, requiredSpace, maxSpaceLeft)
	assert(splitIdx > 0, "splitIdx == 0")

	var tempPage Page
	tempPage.init(pg.IsLeaf())
	tempPage.Flags = pg.Flags

	if splitIdx-1 < idx { // e belongs to the new page
		pg.moveItemsTo(&tempPage, 0, splitIdx)

		pg.moveItemsTo(newPg, splitIdx, idx)

		_, _, err := newPg.InsertEntry(e)
		if err != nil {
			panic(err)
		}

		pg.moveItemsTo(newPg, idx, int(pg.NumEntries))
	} else {
		pg.moveItemsTo(newPg, splitIdx-1, int(pg.NumEntries))

		pg.moveItemsTo(&tempPage, 0, idx)

		_, _, err := tempPage.InsertEntry(e)
		if err != nil {
			panic(err)
		}

		pg.moveItemsTo(&tempPage, idx, splitIdx-1)
	}

	*pg = tempPage

	assert(tempPage.NumEntries >= 1, "empty left page after split")
	assert(newPg.NumEntries >= 1, "empty right page split")

	return idx
}

func (pg *Page) firstKey() []byte {
	if pg.IsLeaf() {
		return pg.keyAt(0)
	}
	return pg.keyAt(1)
}

func (pg *Page) keyAt(idx int) []byte {
	if pg.IsLeaf() {
		e := pg.leafPageEntryAt(idx)
		return pg.data[e.Offset+24 : e.Offset+24+e.KeySize]
	}
	e := pg.innerPageEntryAt(idx)
	return pg.data[e.Offset+8 : e.Offset+8+e.KeySize]
}

func (pg *Page) Remove(key []byte) (*Entry, error) {
	idx, found := pg.find(key)
	if !found {
		return nil, ErrKeyNotFound
	}

	e, err := pg.GetEntryAt(idx)
	if err != nil {
		return nil, err
	}
	pg.removeLeafEntryAt(idx)
	pg.NumEntries--
	return &e, nil
}

func (pg *Page) InsertEntry(e *Entry) (Entry, bool, error) {
	prevEntry, has, err := pg.insertEntry(e)
	if err != nil && !errors.Is(err, ErrPageFull) {
		return Entry{}, false, err
	}

	if errors.Is(err, ErrPageFull) && int(pg.UnusedSpace) >= e.requiredPageItemSize() {
		_, err := pg.compact()
		if err != nil {
			return Entry{}, false, err
		}
		return pg.insertEntry(e)
	}
	return prevEntry, has, err
}

func (pg *Page) insertEntry(e *Entry) (Entry, bool, error) {
	assert(pg.IsLeaf(), "not a leaf page")

	idx, found := pg.find(e.Key)

	requiredSpace := e.requiredPageItemSize()
	dataSpace := requiredSpace - LeafPageEntryDataSize
	if found {
		requiredSpace -= LeafPageEntryDataSize
	}

	if pg.FreeSpace() < requiredSpace {
		return Entry{}, false, ErrPageFull
	}

	// TODO: replace entry in place when there is enough space
	newEntry := pg.pushEntry(e, dataSpace)
	if found {
		prevEntry, err := pg.GetEntryAt(idx)
		if err != nil {
			return Entry{}, false, err
		}

		if e.Ts <= prevEntry.Ts {
			return Entry{}, false, fmt.Errorf("%w: %d is not greater than %d", ErrInvalidTimestamp, e.Ts, prevEntry.Ts)
		}

		binary.BigEndian.PutUint64(
			pg.data[newEntry.Offset+16:],
			prevEntry.HC,
		)

		le := pg.leafPageEntryAt(idx)
		pg.UnusedSpace += uint16(le.dataSize())

		*le = newEntry
		return prevEntry, true, nil
	}

	pg.insertLeafEntryAt(idx, newEntry)
	pg.NumEntries++

	return Entry{}, false, nil
}

func (pg *Page) GetEntry(key []byte) (*Entry, error) {
	if !pg.IsLeaf() {
		return nil, ErrNotLeafPage
	}

	idx, found := pg.find(key)
	if !found {
		return nil, ErrKeyNotFound
	}

	e := pg.leafPageEntryAt(idx)
	return &Entry{
		Ts:    binary.BigEndian.Uint64(pg.data[e.Offset:]),
		HOff:  binary.BigEndian.Uint64(pg.data[e.Offset+8:]),
		HC:    binary.BigEndian.Uint64(pg.data[e.Offset+16:]),
		Key:   pg.data[e.Offset+24 : e.Offset+24+e.KeySize],
		Value: pg.data[e.Offset+24+e.KeySize : e.Offset+24+e.KeySize+e.ValueSize],
	}, nil
}

func (pg *Page) GetEntryAt(idx int) (Entry, error) {
	if !pg.IsLeaf() {
		return Entry{}, ErrNotLeafPage
	}

	if idx >= int(pg.NumEntries) {
		return Entry{}, ErrKeyNotFound
	}

	e := pg.leafPageEntryAt(idx)
	return Entry{
		Ts:    binary.BigEndian.Uint64(pg.data[e.Offset:]),
		HOff:  binary.BigEndian.Uint64(pg.data[e.Offset+8:]),
		HC:    binary.BigEndian.Uint64(pg.data[e.Offset+16:]),
		Key:   pg.data[e.Offset+24 : e.Offset+24+e.KeySize],
		Value: pg.data[e.Offset+24+e.KeySize : e.Offset+24+e.KeySize+e.ValueSize],
	}, nil
}

func (pg *Page) Get(key []byte) (PageID, error) {
	if pg.IsLeaf() {
		return 0, ErrNotInnerPage
	}

	idx, found := pg.find(key)
	if !found {
		return 0, ErrKeyNotFound
	}

	e := pg.innerPageEntryAt(idx)
	return PageID(binary.BigEndian.Uint64(pg.data[e.Offset:])), nil
}

func (pg *Page) Find(key []byte) (int, PageID, error) {
	if pg.IsLeaf() {
		return -1, 0, ErrNotInnerPage
	}

	idx, found := pg.find(key)
	if !found {
		idx--
	}

	e := pg.innerPageEntryAt(idx)
	return idx, PageID(binary.BigEndian.Uint64(pg.data[e.Offset:])), nil
}

func (pg *Page) ChildPageAt(idx int) PageID {
	assert(!pg.IsLeaf(), "not an inner page")

	e := pg.innerPageEntryAt(idx)
	return PageID(binary.BigEndian.Uint64(pg.data[e.Offset:]))
}

func (pg *Page) SetPageID(idx int, id PageID) {
	assert(idx < int(pg.NumEntries), "SetPageID(): idx >= num entries")

	e := pg.innerPageEntryAt(idx)
	binary.BigEndian.PutUint64(pg.data[e.Offset:e.Offset+8], uint64(id))
}

func (pg *Page) UpdateHistory(key []byte, hoff uint64) {
	idx, found := pg.find(key)
	assert(found, "SetHOffset: node not found")

	e := pg.leafPageEntryAt(idx)
	binary.BigEndian.PutUint64(pg.data[e.Offset+8:], hoff)
	hc := binary.BigEndian.Uint64(pg.data[e.Offset+16:])
	binary.BigEndian.PutUint64(pg.data[e.Offset+16:], hc+1)
}

func (pg *Page) insertLeafEntryAt(e int, entry LeafPageEntryData) {
	for i := int(pg.NumEntries) - 1; i >= e; i-- {
		start := i * int(LeafPageEntryDataSize)
		copy(pg.data[start+int(LeafPageEntryDataSize):start+2*int(LeafPageEntryDataSize)], pg.data[start:start+int(LeafPageEntryDataSize)])
	}

	pg.FreeSpaceStart += uint16(LeafPageEntryDataSize)
	newEntry := pg.leafPageEntryAt(e)
	*newEntry = entry
}

func (pg *Page) removeLeafEntryAt(e int) {
	for i := e; i < int(pg.NumEntries)-1; i++ {
		start := i * int(LeafPageEntryDataSize)
		copy(pg.data[start:start+LeafPageEntryDataSize], pg.data[start+LeafPageEntryDataSize:start+2*LeafPageEntryDataSize])
	}

	pg.FreeSpaceStart -= uint16(LeafPageEntryDataSize)
}

func (pg *Page) insertInnerEntryAt(e int, entry InnerPageEntryData) {
	for i := int(pg.NumEntries) - 1; i >= e; i-- {
		start := i * int(InnerPageEntryDataSize)
		copy(pg.data[start+int(InnerPageEntryDataSize):start+2*int(InnerPageEntryDataSize)], pg.data[start:start+int(InnerPageEntryDataSize)])
	}

	pg.FreeSpaceStart += uint16(InnerPageEntryDataSize)
	newEntry := pg.innerPageEntryAt(e)
	*newEntry = entry
}

func (pg *Page) Bytes() []byte {
	return (*(*[PageSize]byte)(unsafe.Pointer(pg)))[:]
}

func (pg *Page) IsRoot() bool {
	return pg.getFlag(RootFlag)
}

func (pg *Page) IsLeaf() bool {
	return pg.getFlag(LeafNodeFlag)
}

func (pg *Page) SetAsRoot() {
	pg.setFlags(RootFlag)
}

func (pg *Page) SetAsLeaf() {
	pg.setFlags(LeafNodeFlag)
}

func (pg *Page) SetAsCopied() {
	pg.setFlags(CopiedFlag)
}

func (pg *Page) IsCopied() bool {
	return pg.getFlag(CopiedFlag)
}

func (pg *Page) FreeSpace() int {
	return int(pg.FreeSpaceEnd) - int(pg.FreeSpaceStart)
}

func (pg *Page) UsedSpace() int {
	return len(pg.data) - pg.FreeSpace()
}

func (pg *Page) getFlag(mask uint16) bool {
	return pg.Flags&(mask) != 0
}

func (pg *Page) setFlags(mask uint16) {
	pg.Flags |= mask
}

func (pg *Page) leafPageEntryAt(idx int) *LeafPageEntryData {
	start := idx * int(LeafPageEntryDataSize)
	buf := pg.data[start : start+int(LeafPageEntryDataSize)]
	return (*LeafPageEntryData)(unsafe.Pointer(&buf[0]))
}

func (pg *Page) innerPageEntryAt(idx int) *InnerPageEntryData {
	start := idx * int(InnerPageEntryDataSize)
	buf := pg.data[start : start+int(InnerPageEntryDataSize)]
	return (*InnerPageEntryData)(unsafe.Pointer(&buf[0]))
}

func (pg *Page) keyAtEntry(idx int) []byte {
	if pg.IsLeaf() {
		e := pg.leafPageEntryAt(idx)
		return pg.data[e.Offset+24 : e.Offset+24+e.KeySize]
	}

	e := pg.innerPageEntryAt(idx)
	return pg.data[e.Offset+8 : e.Offset+8+e.KeySize]
}

func (pg *Page) find(key []byte) (int, bool) {
	found := false

	idx := sort.Search(int(pg.NumEntries), func(i int) bool {
		res := bytes.Compare(pg.keyAtEntry(i), key)
		found = found || res == 0
		return res >= 0
	})
	return idx, found
}

const PageFooterSize = 2 // size

func (pg *Page) Put(buf []byte) int {
	pg.PageHeaderData.Put(buf[:])

	off := PageHeaderDataSize - PageFooterSize
	off += copy(buf[off:], pg.data[:pg.FreeSpaceStart])
	off += copy(buf[off:], pg.data[pg.FreeSpaceEnd:])

	binary.BigEndian.PutUint16(buf[off:], uint16(off+PageFooterSize))
	off += 2

	return off
}

func expandToFixedPage(buf []byte) (*Page, int, error) {
	pgSize := binary.BigEndian.Uint16(buf[len(buf)-PageFooterSize:])

	pgStart := len(buf) - int(pgSize)
	var hdr PageHeaderData
	hdr.Read(buf[pgStart:])

	shiftLeft(
		buf,
		pgStart,
		pgStart+(PageHeaderDataSize-PageFooterSize)-1,
		pgStart,
	)

	isLeaf := hdr.Flags&LeafNodeFlag != 0
	entrySize := InnerPageEntryDataSize
	if isLeaf {
		entrySize = LeafPageEntryDataSize
	}

	entriesOff := pgStart + PageHeaderDataSize - PageFooterSize
	entriesSize := int(hdr.NumEntries) * entrySize
	dataOff := entriesOff + entriesSize

	shiftRight(
		buf,
		dataOff,
		len(buf)-PageFooterSize-1,
		PageFooterSize,
	)

	shift := entriesOff - PageHeaderDataSize
	if shift > 0 {
		shiftLeft(
			buf,
			entriesOff,
			dataOff-1,
			shift,
		)
	} else if shift < 0 {
		shiftRight(
			buf,
			entriesOff,
			dataOff-1,
			-shift,
		)
	}

	pg := PageFromBytes(buf)
	pg.PageHeaderData = hdr // NOTE: needed because of machine endianess
	pg.Padding = 0

	return pg, int(pgSize), nil
}

func shiftLeft(buf []byte, start, end, n int) {
	for i := start; i <= end; i++ {
		buf[i-n] = buf[i]
	}
}

func shiftRight(buf []byte, start, end, n int) {
	for i := end; i >= start; i-- {
		buf[i+n] = buf[i]
	}
}

func requiredPageItemSize(keySize int, valueSize int) int {
	return LeafPageEntryDataSize +
		8 + // Ts field
		8 + // HOff
		8 + // HC
		keySize +
		valueSize
}

func requiredInnerPageItemSize(keySize int) int {
	return InnerPageEntryDataSize + keySize + 8 // PageID
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func assertNoErr(err error) {
	if err != nil {
		panic(err)
	}
}

const HistoryPageHeaderSize = 10

type HistoryPage [PageSize]byte

func (pg *HistoryPage) init() {
	pg.SetNextPageID(PageNone)
	pg.setOffset(HistoryPageHeaderSize)
}

func HistoryPageFromBytes(buf []byte) *HistoryPage {
	return (*HistoryPage)(unsafe.Pointer(&buf[0]))
}

type HistoryPageHeader struct {
	NextPageID PageID
	Offset     uint16
}

func (pg *HistoryPage) Append(e *HistoryEntry) (int, error) {
	offset := pg.Offset()

	space := e.requiredSpace()
	if space > pg.freeSpace() {
		return -1, ErrPageFull
	}

	binary.BigEndian.PutUint64(pg[offset:], e.PrevOffset)
	offset += 8

	binary.BigEndian.PutUint64(pg[offset:], e.Ts)
	offset += 8

	binary.BigEndian.PutUint16(pg[offset:], uint16(len(e.Value)))
	offset += 2

	copy(pg[offset:], e.Value)
	offset += uint16(len(e.Value))

	pg.setOffset(offset)
	return space, nil
}

func (pg *HistoryPage) Data() []byte {
	return pg[HistoryPageHeaderSize:pg.Offset()]
}

func (pg *HistoryPage) freeSpace() int {
	return PageSize - int(pg.Offset())
}

func (pg *HistoryPage) Offset() uint16 {
	return binary.BigEndian.Uint16(pg[8:10])
}

func (pg *HistoryPage) setOffset(offset uint16) {
	binary.BigEndian.PutUint16(pg[8:10], offset)
}

func (pg *HistoryPage) NextPageID() PageID {
	return PageID(binary.BigEndian.Uint64(pg[:]))
}

func (pg *HistoryPage) SetNextPageID(id PageID) {
	binary.BigEndian.PutUint64(pg[:], uint64(id))
}

func (pg *HistoryPage) Header() HistoryPageHeader {
	return HistoryPageHeader{
		NextPageID: pg.NextPageID(),
		Offset:     pg.Offset(),
	}
}
