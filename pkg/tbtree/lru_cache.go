/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tbtree

import "container/list"

type LRUCache struct {
	data    map[interface{}]*entry
	lruList *list.List
	size    int
}

type entry struct {
	value interface{}
	order *list.Element
}

func NewLRUCache(size int) (*LRUCache, error) {
	if size < 1 {
		return nil, ErrIllegalArgument
	}

	return &LRUCache{
		data:    make(map[interface{}]*entry, size),
		lruList: list.New(),
		size:    size,
	}, nil
}

func (c *LRUCache) Put(key interface{}, value interface{}) error {
	if key == nil || value == nil {
		return ErrIllegalArgument
	}

	e, ok := c.data[key]

	if ok {
		e.value = value
		c.lruList.MoveToBack(e.order)
	} else {
		e = &entry{
			value: value,
			order: c.lruList.PushBack(key),
		}
		c.data[key] = e

		if c.lruList.Len() > c.size {
			lruEntry := c.lruList.Front()
			delete(c.data, lruEntry.Value)
			c.lruList.Remove(lruEntry)
		}
	}
	return nil
}

func (c *LRUCache) Get(key interface{}) (interface{}, error) {
	if key == nil {
		return nil, ErrIllegalArgument
	}

	e, ok := c.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	c.lruList.MoveToBack(e.order)

	return e.value, nil
}

func (c *LRUCache) Size() int {
	return c.size
}
