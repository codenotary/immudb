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
	"sync/atomic"
)

var (
	ErrIllegalArguments = errors.New("illegal arguments")
	ErrKeyNotFound      = errors.New("key not found")
	ErrIllegalState     = errors.New("illegal state")
)

// Cache implements the SIEVE cache replacement policy.
type Cache struct {
	data map[interface{}]*entry

	hand *list.Element
	list *list.List
	size int

	mutex sync.RWMutex
}

type entry struct {
	value   interface{}
	visited uint32
	order   *list.Element
}

func NewCache(size int) (*Cache, error) {
	if size < 1 {
		return nil, ErrIllegalArguments
	}

	return &Cache{
		data: make(map[interface{}]*entry, size),
		list: list.New(),
		size: size,
	}, nil
}

func (c *Cache) Resize(size int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for size < c.list.Len() {
		c.evict()
	}

	c.size = size
}

func (c *Cache) Put(key interface{}, value interface{}) (interface{}, interface{}, error) {
	if key == nil || value == nil {
		return nil, nil, ErrIllegalArguments
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	e, ok := c.data[key]
	if ok {
		e.visited = 1
		e.value = value
		return nil, nil, nil
	}

	var rkey, rvalue interface{}
	if c.list.Len() >= c.size {
		evictedKey, entry, err := c.evict()
		if err != nil {
			return nil, nil, err
		}

		rkey = evictedKey
		rvalue = entry.value
	}

	c.data[key] = &entry{
		value:   value,
		visited: 0,
		order:   c.list.PushFront(key),
	}
	return rkey, rvalue, nil
}

func (c *Cache) evict() (rkey interface{}, e *entry, err error) {
	if c.list.Len() == 0 {
		return nil, nil, fmt.Errorf("%w: evict requested in an empty cache", ErrIllegalState)
	}

	curr := c.hand
	for {
		if curr == nil {
			curr = c.list.Back()
		}

		key := curr.Value

		e := c.data[key]
		if e.visited == 0 {
			c.hand = curr.Prev()

			c.list.Remove(curr)
			delete(c.data, key)

			return key, e, nil
		}

		e.visited = 0
		curr = curr.Prev()
	}
}

func (c *Cache) Get(key interface{}) (interface{}, error) {
	if key == nil {
		return nil, ErrIllegalArguments
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	atomic.StoreUint32(&e.visited, 1)

	return e.value, nil
}

func (c *Cache) Pop(key interface{}) (interface{}, error) {
	if key == nil {
		return nil, ErrIllegalArguments
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	if c.hand == e.order {
		c.hand = c.hand.Prev()
	}

	c.list.Remove(e.order)
	delete(c.data, key)

	return e.value, nil
}

func (c *Cache) Replace(k interface{}, v interface{}) (interface{}, error) {
	if k == nil {
		return nil, ErrIllegalArguments
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	e, ok := c.data[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	oldV := e.value
	e.value = v

	return oldV, nil
}

func (c *Cache) Size() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.size
}

func (c *Cache) EntriesCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.list.Len()
}

func (c *Cache) Apply(fun func(k interface{}, v interface{}) error) error {
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
