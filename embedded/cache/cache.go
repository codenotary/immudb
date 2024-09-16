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
	ErrCannotEvictItem  = errors.New("cannot find an item to evict")
)

type EvictFilterFunc func(key interface{}, value interface{}) bool
type EvictCallbackFunc func(key, value interface{})

// Cache implements the SIEVE cache replacement policy.
type Cache struct {
	data map[interface{}]*entry

	hand      *list.Element
	list      *list.List
	weight    int
	maxWeight int

	mutex sync.RWMutex

	canEvict EvictFilterFunc
	onEvict  EvictCallbackFunc
}

type entry struct {
	value   interface{}
	visited uint32
	weight  int
	order   *list.Element
}

func NewCache(maxWeight int) (*Cache, error) {
	if maxWeight < 1 {
		return nil, ErrIllegalArguments
	}

	return &Cache{
		data:      make(map[interface{}]*entry),
		list:      list.New(),
		weight:    0,
		maxWeight: maxWeight,
		onEvict:   nil,
		canEvict:  nil,
	}, nil
}

func (c *Cache) SetCanEvict(canEvict EvictFilterFunc) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.canEvict = canEvict
}

func (c *Cache) SetOnEvict(onEvict EvictCallbackFunc) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.onEvict = onEvict
}

func (c *Cache) Resize(newWeight int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for c.weight > newWeight {
		key, entry, _ := c.evict()
		if c.onEvict != nil {
			c.onEvict(key, entry.value)
		}
		c.weight -= entry.weight
	}

	c.maxWeight = newWeight
}

func (c *Cache) Put(key interface{}, value interface{}) (interface{}, interface{}, error) {
	return c.PutWeighted(key, value, 1)
}

func (c *Cache) PutWeighted(key interface{}, value interface{}, weight int) (interface{}, interface{}, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.put(key, value, weight, 0)
}

func (c *Cache) put(key interface{}, value interface{}, weight int, visited uint32) (interface{}, interface{}, error) {
	if key == nil || value == nil || weight == 0 || weight > c.maxWeight {
		return nil, nil, ErrIllegalArguments
	}

	e, ok := c.data[key]
	if ok {
		if c.weight-e.weight+weight > c.maxWeight {
			c.pop(key)
			return c.put(key, value, weight, 1)
		}

		c.weight = c.weight - e.weight + weight

		e.visited = 1
		e.value = value
		e.weight = weight

		return nil, nil, nil
	}

	evictedKey, evictedValue, err := c.evictWhileFull(weight)
	if err != nil {
		return nil, nil, err
	}

	c.weight += weight

	c.data[key] = &entry{
		value:   value,
		visited: visited,
		weight:  weight,
		order:   c.list.PushFront(key),
	}
	return evictedKey, evictedValue, nil
}

func (c *Cache) evictWhileFull(weight int) (interface{}, interface{}, error) {
	var rkey, rvalue interface{}
	for c.weight+weight > c.maxWeight {
		evictedKey, entry, err := c.evict()
		if err != nil {
			return nil, nil, err
		}

		rkey = evictedKey
		rvalue = entry.value

		if c.onEvict != nil {
			c.onEvict(rkey, rvalue)
		}
		c.weight -= entry.weight
	}
	return rkey, rvalue, nil
}

func (c *Cache) evict() (rkey interface{}, e *entry, err error) {
	if c.list.Len() == 0 {
		return nil, nil, fmt.Errorf("%w: evict requested in an empty cache", ErrIllegalState)
	}

	curr := c.hand
	for i := 0; i < 2*c.list.Len(); i++ {
		if curr == nil {
			curr = c.list.Back()
		}

		key := curr.Value

		e := c.data[key]
		if e.visited == 0 && c.shouldEvict(key, e.value) {
			c.hand = curr.Prev()

			c.list.Remove(curr)
			delete(c.data, key)

			return key, e, nil
		}

		e.visited = 0
		curr = curr.Prev()
	}
	return nil, nil, ErrCannotEvictItem
}

func (c *Cache) shouldEvict(key, value interface{}) bool {
	return c.canEvict == nil || c.canEvict(key, value)
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

	return c.pop(key)
}

func (c *Cache) pop(key interface{}) (interface{}, error) {
	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	if c.hand == e.order {
		c.hand = c.hand.Prev()
	}

	c.list.Remove(e.order)
	delete(c.data, key)

	c.weight -= e.weight

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

func (c *Cache) Weight() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.weight
}

func (c *Cache) Available() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.maxWeight - c.weight
}

func (c *Cache) MaxWeight() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.maxWeight
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
