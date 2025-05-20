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

	"github.com/codenotary/immudb/v2/embedded/metrics"
	"github.com/codenotary/immudb/v2/pkg/latch"
)

var (
	ErrCannotEvictPage   = errors.New("no page can be evicted")
	ErrPageNotFound      = errors.New("page not found")
	ErrDescriptorChanged = errors.New("descriptor changed")
)

type PageLoader func(dst []byte, id PageID) error

type PageDescriptor struct {
	allocated uint32
	tp        TreePage
	lock      latch.LWLock
}

type PageCache struct {
	mtx sync.RWMutex

	metrics metrics.PageCacheMetrics

	descTable   map[TreePage]uint32
	descriptors []PageDescriptor

	buf []byte
}

func NewPageCache(
	size int,
	metrics metrics.PageCacheMetrics,
) *PageCache {
	nPages := roundUpPages(size)

	desc := make([]PageDescriptor, nPages)
	for i := range desc {
		desc[i].tp = math.MaxUint64
	}

	metrics.SetCacheSize(PageSize * nPages)

	return &PageCache{
		metrics:     metrics,
		descTable:   make(map[TreePage]uint32, nPages),
		descriptors: desc,
		buf:         make([]byte, PageSize*nPages),
	}
}

func (c *PageCache) Get(tid TreeID, id PageID, loader PageLoader) (*Page, error) {
	pg, err := c.get(indexPage(tid, id), loader)
	return pg, err
}

func (c *PageCache) UsePage(tid TreeID, id PageID, loader PageLoader, onPage func(pg *Page) error) error {
	pg, err := c.Get(tid, id, loader)
	if err != nil {
		return err
	}

	err = onPage(pg)
	c.Release(tid, id)
	return err
}

func (c *PageCache) get(ip TreePage, loader PageLoader) (*Page, error) {
	var err error = ErrDescriptorChanged
	var pg *Page
	for errors.Is(err, ErrDescriptorChanged) {
		descID := c.pageDesc(ip)
		if descID >= 0 {
			pg, _, err = c.pinDescriptor(uint32(descID), ip, false)
			if err == nil {
				c.metrics.IncHits()
			}
		} else {
			pg, _, err = c.loadPage(ip, loader)
		}
	}
	return pg, err
}

func (c *PageCache) loadPage(ip TreePage, loader PageLoader) (*Page, uint32, error) {
	newDescID, newSlotAllocated, err := c.allocDescriptor()
	if err != nil {
		return nil, 0, err
	}

	c.mtx.Lock()

	descID, has := c.descTable[ip]
	if has {
		c.mtx.Unlock()

		atomic.StoreUint32(&c.descriptors[newDescID].allocated, boolToUInt32(!newSlotAllocated))
		c.descriptors[newDescID].lock.Unlock()

		pg, desc, err := c.pinDescriptor(uint32(descID), ip, false)
		if err == nil {
			c.metrics.IncHits()
		}
		return pg, desc, err
	}

	desc := &c.descriptors[newDescID]
	delete(c.descTable, desc.tp)

	desc.tp = ip

	c.descTable[ip] = uint32(newDescID)

	c.mtx.Unlock()

	c.metrics.IncMisses()

	pageData := c.buf[newDescID*PageSize : (newDescID+1)*PageSize]
	err = loader(pageData, ip.PageID())
	if err != nil {
		desc.lock.Unlock()
		return nil, 0, err
	}
	return c.pinDescriptor(uint32(newDescID), ip, true)
}

func (c *PageCache) pinDescriptor(
	descID uint32,
	ip TreePage,
	downgrade bool,
) (*Page, uint32, error) {
	desc := &c.descriptors[descID]
	if downgrade {
		desc.lock.Downgrade()
	} else {
		desc.lock.RLock()
	}

	if desc.tp != ip {
		desc.lock.RUnlock()
		return nil, 0, ErrDescriptorChanged
	}

	pageData := c.buf[descID*PageSize : (descID+1)*PageSize]
	return PageFromBytes(pageData), descID, nil
}

func (c *PageCache) allocDescriptor() (int, bool, error) {
	for i := range c.descriptors {
		desc := &c.descriptors[i]
		if atomic.LoadUint32(&desc.allocated) == 0 && desc.lock.TryLock() {
			atomic.StoreUint32(&desc.allocated, 1)
			return i, true, nil
		}
	}

	c.metrics.IncEvictions()

	for {
		for i := range c.descriptors {
			desc := &c.descriptors[i]
			if desc.lock.TryLock() {
				return i, false, nil
			}
		}
		// TODO: optimize this section for lower contemption
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func (c *PageCache) Release(tid TreeID, pgID PageID) {
	descID := c.pageDesc(indexPage(tid, pgID))
	assert(descID >= 0, "Release(): no descriptor found")

	desc := &c.descriptors[descID]
	desc.lock.RUnlock()
}

func (c *PageCache) pageDesc(ip TreePage) int {
	c.mtx.RLock()
	desc, ok := c.descTable[ip]
	c.mtx.RUnlock()
	if !ok {
		return -1
	}
	return int(desc)
}

func (c *PageCache) InvalidatePages(id TreeID) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for pgID := range c.descTable {
		if pgID.TreeID() == id {
			delete(c.descTable, pgID)
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

func boolToUInt32(v bool) uint32 {
	if v {
		return 1
	}
	return 0
}
