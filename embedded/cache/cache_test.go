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
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupCache(t *testing.T) *Cache {
	rand.Seed(time.Now().UnixNano())

	size := 10 + rand.Intn(100)

	cache, err := NewCache(size)
	require.NoError(t, err)
	return cache
}

func TestCacheCreation(t *testing.T) {
	_, err := NewCache(0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	cacheSize := 10
	cache, err := NewCache(cacheSize)
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, cacheSize, cache.MaxWeight())

	_, _, err = cache.evict()
	require.Error(t, err)

	_, err = cache.Get(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = cache.Put(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	for i := 0; i < cacheSize; i++ {
		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)
	}

	for i := cacheSize; i > 0; i-- {
		v, err := cache.Get(i - 1)
		require.NoError(t, err)
		require.Equal(t, v, 10*(i-1))
	}

	for i := cacheSize; i < cacheSize+cacheSize/2; i++ {
		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)

		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)
	}

	for i := 0; i < cacheSize/2; i++ {
		_, err := cache.Get(i)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}

	for i := cacheSize / 2; i < cacheSize; i++ {
		v, err := cache.Get(i)
		require.NoError(t, err, ErrKeyNotFound)
		require.Equal(t, v, 10*i)
	}

	for i := cacheSize; i < cacheSize+cacheSize/2; i++ {
		v, err := cache.Get(i)
		require.NoError(t, err)
		require.Equal(t, v, 10*i)
	}
}

func TestEvictionPolicy(t *testing.T) {
	fillCache := func(cache *Cache) {
		for i := 0; i < cache.MaxWeight(); i++ {
			key, value, err := cache.Put(i, i+1)
			require.NoError(t, err)
			require.Nil(t, key)
			require.Nil(t, value)
		}
	}

	t.Run("should evict multiple items", func(t *testing.T) {
		cache := setupCache(t)
		fillCache(cache)

		el := rand.Intn(cache.MaxWeight())

		_, _, err := cache.PutWeighted(cache.MaxWeight(), cache.MaxWeight()+1, el+1)
		require.NoError(t, err)

		require.Equal(t, cache.Weight(), cache.EntriesCount()+el)
		require.Equal(t, cache.MaxWeight()-el, cache.EntriesCount())
	})

	t.Run("should evict non visited items", func(t *testing.T) {
		t.Run("starting from element at middle", func(t *testing.T) {
			cache := setupCache(t)
			fillCache(cache)

			el := rand.Intn(cache.MaxWeight())
			for i := 0; i < el; i++ {
				_, err := cache.Get(i)
				require.NoError(t, err)
			}

			for i := el; i < cache.MaxWeight(); i++ {
				key, _, err := cache.Put(cache.MaxWeight()+i, cache.MaxWeight()+i+1)
				require.NoError(t, err)
				require.Equal(t, i%cache.maxWeight, key)
			}
		})

		t.Run("at even positions", func(t *testing.T) {
			cache := setupCache(t)

			fillCache(cache)

			for i := 0; i < (cache.MaxWeight()+1)/2; i++ {
				_, err := cache.Get(2 * i)
				require.NoError(t, err)
			}

			for i := 0; i < cache.MaxWeight()/2; i++ {
				key, _, err := cache.Put(cache.MaxWeight()+i, cache.MaxWeight()+i+1)
				require.NoError(t, err)
				require.Equal(t, key, 2*i+1)
			}
		})

		t.Run("starting from back", func(t *testing.T) {
			cache := setupCache(t)

			fillCache(cache)

			n := 1 + rand.Intn(cache.MaxWeight()-1)
			for i := 0; i < n; i++ {
				key, _, err := cache.Put(cache.MaxWeight()+i, cache.MaxWeight()+i+1)
				require.NoError(t, err)
				require.Equal(t, key, i)
			}
		})
	})

	t.Run("should evict visited items", func(t *testing.T) {
		cache := setupCache(t)

		fillCache(cache)

		for i := 0; i < cache.MaxWeight(); i++ {
			_, err := cache.Get(i)
			require.NoError(t, err)
		}

		n := 1 + rand.Intn(cache.MaxWeight()-1)
		for i := 0; i < n; i++ {
			key, _, err := cache.Put(cache.MaxWeight()+i, cache.MaxWeight()+i+1)
			require.NoError(t, err)
			require.Equal(t, key, i)
		}
	})
}

func TestApply(t *testing.T) {
	cacheSize := 10
	cache, err := NewCache(cacheSize)
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, cacheSize, cache.MaxWeight())

	for i := 0; i < cacheSize; i++ {
		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)
	}

	c := 0
	err = cache.Apply(func(k, v interface{}) error {
		c++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, cacheSize, c)

	err = cache.Apply(func(k, v interface{}) error {
		return errors.New("expected error")
	})
	require.Error(t, err)
}

func TestPop(t *testing.T) {
	cacheSize := 10
	cache, err := NewCache(cacheSize)
	require.NoError(t, err)

	for i := 0; i < cacheSize; i++ {
		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)
	}

	poppedKey := 5
	val, err := cache.Pop(poppedKey)
	require.NoError(t, err)
	require.Equal(t, 10*poppedKey, val)

	c := 0
	err = cache.Apply(func(k, v interface{}) error {
		require.NotEqual(t, 10*poppedKey, v)
		c++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, cacheSize-1, c)

	val, err = cache.Pop(-1)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Nil(t, val)

	val, err = cache.Pop(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, val)
}

func TestReplace(t *testing.T) {
	cacheSize := 10
	cache, err := NewCache(cacheSize)
	require.NoError(t, err)

	for i := 0; i < cacheSize; i++ {
		_, _, err = cache.Put(i, 10*i)
		require.NoError(t, err)
	}

	replacedKey := 5
	val, err := cache.Replace(replacedKey, 9999)
	require.NoError(t, err)
	require.Equal(t, 10*replacedKey, val)

	c := 0
	err = cache.Apply(func(k, v interface{}) error {
		if k == replacedKey {
			require.Equal(t, 9999, v)
		}
		c++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, cacheSize, c)

	val, err = cache.Replace(-1, 9998)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Nil(t, val)

	val, err = cache.Replace(nil, 9997)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, val)
}

func TestCacheResizing(t *testing.T) {
	initialCacheSize := 10
	cache, err := NewCache(initialCacheSize)
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, initialCacheSize, cache.MaxWeight())

	for i := 0; i < initialCacheSize; i++ {
		rkey, _, err := cache.Put(i, i)
		require.NoError(t, err)
		require.Nil(t, rkey)
	}

	// cache growing
	largerCacheSize := 20
	cache.Resize(largerCacheSize)
	require.Equal(t, largerCacheSize, cache.MaxWeight())

	for i := 0; i < initialCacheSize; i++ {
		v, err := cache.Get(i)
		require.NoError(t, err)
		require.Equal(t, i, v)
	}

	for i := initialCacheSize; i < largerCacheSize; i++ {
		rkey, _, err := cache.Put(i, i)
		require.NoError(t, err)
		require.Nil(t, rkey)
	}

	// cache shrinking
	cache.Resize(initialCacheSize)
	require.Equal(t, initialCacheSize, cache.MaxWeight())

	for i := 0; i < initialCacheSize; i++ {
		_, err = cache.Get(i)
		require.NoError(t, err)
	}

	for i := initialCacheSize; i < largerCacheSize; i++ {
		_, err = cache.Get(i)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

func TestPutWeighted(t *testing.T) {
	t.Run("should evict entries according to weight", func(t *testing.T) {
		cache, err := NewCache(1024 * 1024) // 1MB
		require.NoError(t, err)

		weights := make([]int, 0, 1000)

		expectedWeight := 0

		n := 0
		currWeight := 100 + rand.Intn(1024)
		for cache.Weight()+currWeight <= cache.MaxWeight() {
			k, v, err := cache.PutWeighted(n, n, currWeight)
			require.NoError(t, err)
			require.Nil(t, k)
			require.Nil(t, v)

			expectedWeight += currWeight
			weights = append(weights, currWeight)
			currWeight = 100 + rand.Intn(1024)
			n++
		}
		require.Equal(t, expectedWeight, cache.Weight())
		require.Equal(t, n, cache.EntriesCount())

		weight := currWeight + rand.Intn(cache.Weight()-currWeight+1)

		expectedEvictedWeight := 0
		expectedEntriesCount := 0
		for i, w := range weights {
			expectedEvictedWeight += w

			if expectedEvictedWeight+cache.Available() >= weight {
				expectedEntriesCount = n - i
				break
			}
		}

		_, _, err = cache.PutWeighted(n+1, n+1, weight)
		require.NoError(t, err)

		require.Equal(t, expectedEntriesCount, cache.EntriesCount())
		require.Equal(t, expectedWeight-expectedEvictedWeight+weight, cache.Weight())
	})

	t.Run("update existing item weight", func(t *testing.T) {
		cache, err := NewCache(5)
		require.NoError(t, err)

		_, _, err = cache.PutWeighted(1, 1, 0)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, _, err = cache.PutWeighted(1, 1, 10)
		require.ErrorIs(t, err, ErrIllegalArguments)

		cache.PutWeighted(1, 1, 2)
		cache.PutWeighted(2, 2, 3)

		require.Equal(t, 5, cache.Weight())

		key, _, err := cache.PutWeighted(2, 2, 1)
		require.NoError(t, err)
		require.Nil(t, key)
		require.Equal(t, 3, cache.Weight())
		require.Equal(t, 2, cache.EntriesCount())

		key, _, err = cache.PutWeighted(2, 2, 4)
		require.NoError(t, err)
		require.Equal(t, key, 1)

		require.Equal(t, 4, cache.Weight())
		require.Equal(t, 1, cache.EntriesCount())
	})
}

func TestOnEvict(t *testing.T) {
	cache, err := NewCache(5)
	require.NoError(t, err)

	var onEvictCalled int
	cache.SetOnEvict(func(_, value interface{}) {
		onEvictCalled++
	})

	for i := 0; i < 5; i++ {
		cache.Put(i, i)
	}
	require.Zero(t, onEvictCalled)

	_, _, err = cache.PutWeighted(6, 6, 3)
	require.NoError(t, err)

	require.Equal(t, onEvictCalled, 3)

	_, _, err = cache.PutWeighted(7, 7, 2)
	require.NoError(t, err)
	require.Equal(t, onEvictCalled, 5)

	cache.Resize(0)
	require.Equal(t, onEvictCalled, 7)
}

func TestCanEvict(t *testing.T) {
	cache, err := NewCache(5)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, _, err := cache.Put(i, i)
		require.NoError(t, err)
	}

	t.Run("cannot evict any item", func(t *testing.T) {
		cache.SetCanEvict(func(_, _ interface{}) bool {
			return false
		})

		_, _, err := cache.Put(6, 6)
		require.ErrorIs(t, err, ErrCannotEvictItem)
	})

	t.Run("cannot evict any item", func(t *testing.T) {
		keyToEvict := rand.Intn(5)
		cache.SetCanEvict(func(key, _ interface{}) bool {
			return key == keyToEvict
		})

		evictedKey, _, err := cache.Put(6, 6)
		require.NoError(t, err)
		require.Equal(t, evictedKey, keyToEvict)
	})
}
