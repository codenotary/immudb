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
package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheCreation(t *testing.T) {
	cacheSize := 10
	cache, err := NewLRUCache(cacheSize)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
	assert.Equal(t, cacheSize, cache.Size())

	_, _, err = cache.Put(nil, nil)
	assert.Equal(t, ErrIllegalArgument, err)

	for i := 0; i < cacheSize; i++ {
		_, _, err = cache.Put(i, 10*i)
		assert.NoError(t, err)
	}

	for i := cacheSize; i > 0; i-- {
		v, err := cache.Get(i - 1)
		assert.NoError(t, err)
		assert.Equal(t, v, 10*(i-1))
	}

	for i := cacheSize; i < cacheSize+cacheSize/2; i++ {
		_, _, err = cache.Put(i, 10*i)
		assert.NoError(t, err)
	}

	for i := 0; i < cacheSize/2; i++ {
		v, err := cache.Get(i)
		assert.NoError(t, err)
		assert.Equal(t, v, 10*i)
	}

	for i := cacheSize / 2; i < cacheSize; i++ {
		_, err = cache.Get(i)
		assert.Equal(t, ErrKeyNotFound, err)
	}

	for i := cacheSize; i < cacheSize+cacheSize/2; i++ {
		v, err := cache.Get(i)
		assert.NoError(t, err)
		assert.Equal(t, v, 10*i)
	}
}
