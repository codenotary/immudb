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

package multiapp

import (
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/cache"
)

type appendableLRUCache struct {
	cache *cache.LRUCache
}

func (c appendableLRUCache) Put(key int64, value appendable.Appendable) (int64, appendable.Appendable, error) {
	k, v, err := c.cache.Put(key, value)
	rkey, _ := k.(int64)
	rvalue, _ := v.(appendable.Appendable)
	return rkey, rvalue, err
}

func (c appendableLRUCache) Get(key int64) (appendable.Appendable, error) {
	v, err := c.cache.Get(key)
	rvalue, _ := v.(appendable.Appendable)
	return rvalue, err
}

func (c appendableLRUCache) Pop(key int64) (appendable.Appendable, error) {
	v, err := c.cache.Pop(key)
	rvalue, _ := v.(appendable.Appendable)
	return rvalue, err
}

func (c appendableLRUCache) Replace(key int64, value appendable.Appendable) (appendable.Appendable, error) {
	v, err := c.cache.Replace(key, value)
	rvalue, _ := v.(appendable.Appendable)
	return rvalue, err
}

func (c appendableLRUCache) Apply(fun func(k int64, v appendable.Appendable) error) error {
	return c.cache.Apply(func(k, v interface{}) error {
		return fun(k.(int64), v.(appendable.Appendable))
	})
}
