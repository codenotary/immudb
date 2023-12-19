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

package cache

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrKeyNotFound = errors.New("key not found")
var ErrIllegalState = errors.New("illegal state")

type LRUCache struct {
	data    map[interface{}]*entry
	lruList *list.List
	size    int

	mutex sync.Mutex
}

type entry struct {
	value interface{}
	order *list.Element
}

func NewLRUCache(size int) (*LRUCache, error) {
	if size < 1 {
		return nil, ErrIllegalArguments
	}

	return &LRUCache{
		data:    make(map[interface{}]*entry, size),
		lruList: list.New(),
		size:    size,
	}, nil
}

func (c *LRUCache) Resize(size int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for size < c.lruList.Len() {
		c.evict()
	}

	c.size = size
}

func (c *LRUCache) Put(key interface{}, value interface{}) (rkey interface{}, rvalue interface{}, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if key == nil || value == nil {
		return nil, nil, ErrIllegalArguments
	}

	e, ok := c.data[key]

	if ok {
		e.value = value
		c.lruList.MoveToBack(e.order)
		return
	}

	e = &entry{
		value: value,
		order: c.lruList.PushBack(key),
	}
	c.data[key] = e

	if c.lruList.Len() > c.size {
		return c.evict()
	}

	return nil, nil, nil
}

func (c *LRUCache) evict() (rkey interface{}, rvalue interface{}, err error) {
	if c.lruList.Len() == 0 {
		return nil, nil, fmt.Errorf("%w: evict requested in an empty cache", ErrIllegalState)
	}

	lruEntry := c.lruList.Front()
	rkey = lruEntry.Value

	re := c.data[rkey]
	rvalue = re.value

	delete(c.data, rkey)
	c.lruList.Remove(lruEntry)

	return rkey, rvalue, nil
}

func (c *LRUCache) Get(key interface{}) (interface{}, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if key == nil {
		return nil, ErrIllegalArguments
	}

	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	c.lruList.MoveToBack(e.order)

	return e.value, nil
}

func (c *LRUCache) Pop(key interface{}) (interface{}, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if key == nil {
		return nil, ErrIllegalArguments
	}

	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	c.lruList.Remove(e.order)
	delete(c.data, key)
	return e.value, nil
}

func (c *LRUCache) Replace(k interface{}, v interface{}) (interface{}, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if k == nil {
		return nil, ErrIllegalArguments
	}
	e, ok := c.data[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	oldV := e.value
	e.value = v

	return oldV, nil
}

func (c *LRUCache) Size() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.size
}

func (c *LRUCache) EntriesCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.lruList.Len()
}

func (c *LRUCache) Apply(fun func(k interface{}, v interface{}) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k, e := range c.data {
		err := fun(k, e.value)
		if err != nil {
			return err
		}
	}
	return nil
}
