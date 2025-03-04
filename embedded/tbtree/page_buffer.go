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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/pkg/latch"
)

var (
	ErrCannotEvictPage   = errors.New("no page can be evicted")
	ErrPageNotFound      = errors.New("page not found")
	ErrDescriptorChanged = errors.New("descriptor changed")
)

type PageLoader func(dst []byte, id PageID) error

type PageDescriptor struct {
	allocated atomic.Bool
	tp        TreePage
	lock      latch.LWLock
}

type PageBuffer struct {
	mtx sync.RWMutex

	descTable   map[TreePage]uint32
	descriptors []PageDescriptor

	buf []byte
}

func NewPageBuffer(size int) *PageBuffer {
	nPages := roundUpPages(size)

	desc := make([]PageDescriptor, nPages)
	for i := range desc {
		desc[i].tp = math.MaxUint64
	}

	return &PageBuffer{
		descTable:   make(map[TreePage]uint32, nPages),
		descriptors: desc,
		buf:         make([]byte, PageSize*nPages),
	}
}

func (buf *PageBuffer) Get(tid TreeID, id PageID, loader PageLoader) (*Page, error) {
	pg, err := buf.get(indexPage(tid, id), loader)
	return pg, err
}

func (buf *PageBuffer) UsePage(tid TreeID, id PageID, loader PageLoader, onPage func(pg *Page) error) error {
	pg, err := buf.Get(tid, id, loader)
	if err != nil {
		return err
	}

	err = onPage(pg)
	buf.Release(tid, id)
	return err
}

func (buf *PageBuffer) get(ip TreePage, loader PageLoader) (*Page, error) {
	var err error = ErrDescriptorChanged
	var pg *Page
	for errors.Is(err, ErrDescriptorChanged) {
		descID := buf.pageDesc(ip)
		if descID >= 0 {
			pg, _, err = buf.pinDescriptor(uint32(descID), ip, false)
		} else {
			pg, _, err = buf.loadPage(ip, loader)
		}
	}
	return pg, err
}

func (buf *PageBuffer) loadPage(ip TreePage, loader PageLoader) (*Page, uint32, error) {
	newDescID, newSlotAllocated, err := buf.allocDescriptor()
	if err != nil {
		return nil, 0, err
	}

	buf.mtx.Lock()

	descID, has := buf.descTable[ip]
	if has {
		buf.mtx.Unlock()

		buf.descriptors[newDescID].allocated.Store(!newSlotAllocated)
		buf.descriptors[newDescID].lock.Unlock()

		return buf.pinDescriptor(uint32(descID), ip, false)
	}

	desc := &buf.descriptors[newDescID]
	delete(buf.descTable, desc.tp)

	desc.tp = ip

	buf.descTable[ip] = uint32(newDescID)

	buf.mtx.Unlock()

	pageData := buf.buf[newDescID*PageSize : (newDescID+1)*PageSize]
	err = loader(pageData, ip.PageID())
	if err != nil {
		desc.lock.Unlock()
		return nil, 0, err
	}
	return buf.pinDescriptor(uint32(newDescID), ip, true)
}

func (buf *PageBuffer) pinDescriptor(
	descID uint32,
	ip TreePage,
	downgrade bool,
) (*Page, uint32, error) {
	desc := &buf.descriptors[descID]
	if downgrade {
		desc.lock.Downgrade()
	} else {
		desc.lock.RLock()
	}

	if desc.tp != ip {
		desc.lock.RUnlock()
		return nil, 0, ErrDescriptorChanged
	}

	pageData := buf.buf[descID*PageSize : (descID+1)*PageSize]
	return PageFromBytes(pageData), descID, nil
}

func (buf *PageBuffer) allocDescriptor() (int, bool, error) {
	for i := range buf.descriptors {
		desc := &buf.descriptors[i]
		if !desc.allocated.Load() && desc.lock.TryLock() {
			desc.allocated.Store(true)
			return i, true, nil
		}
	}

	for {
		for i := range buf.descriptors {
			desc := &buf.descriptors[i]
			if desc.lock.TryLock() {
				return i, false, nil
			}
		}
		// TODO: optimize this section for lower contemption
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func (buf *PageBuffer) Release(tid TreeID, pgID PageID) {
	descID := buf.pageDesc(indexPage(tid, pgID))
	assert(descID >= 0, "Release(): no descriptor found")

	desc := &buf.descriptors[descID]
	desc.lock.RUnlock()
}

func (buf *PageBuffer) pageDesc(ip TreePage) int {
	buf.mtx.RLock()
	desc, ok := buf.descTable[ip]
	buf.mtx.RUnlock()
	if !ok {
		return -1
	}
	return int(desc)
}

func (buf *PageBuffer) InvalidatePages(id TreeID) {
	buf.mtx.Lock()
	defer buf.mtx.Unlock()

	for pgID := range buf.descTable {
		if pgID.TreeID() == id {
			delete(buf.descTable, pgID)
		}
	}
}

type TreePage uint64

func indexPage(id TreeID, pgID PageID) TreePage {
	return TreePage(uint64(id)<<48 | uint64(pgID))
}

func (tp TreePage) PageID() PageID {
	return PageID(tp & TreePage(PageNone))
}

func (tp TreePage) TreeID() TreeID {
	return TreeID(tp >> 48)
}
