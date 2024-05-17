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
	require.Equal(t, cacheSize, cache.Size())

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
		for i := 0; i < cache.Size(); i++ {
			key, value, err := cache.Put(i, i+1)
			require.NoError(t, err)
			require.Nil(t, key)
			require.Nil(t, value)
		}
	}

	t.Run("should evict non visited items", func(t *testing.T) {
		t.Run("starting from element at middle", func(t *testing.T) {
			cache := setupCache(t)

			fillCache(cache)

			el := rand.Intn(cache.Size())
			for i := 0; i < el; i++ {
				_, err := cache.Get(i)
				require.NoError(t, err)
			}

			for i := el; i < cache.Size(); i++ {
				key, _, err := cache.Put(cache.Size()+i, cache.Size()+i+1)
				require.NoError(t, err)
				require.Equal(t, i%cache.size, key)
			}
		})

		t.Run("at even positions", func(t *testing.T) {
			cache := setupCache(t)

			fillCache(cache)

			for i := 0; i < (cache.Size()+1)/2; i++ {
				_, err := cache.Get(2 * i)
				require.NoError(t, err)
			}

			for i := 0; i < cache.Size()/2; i++ {
				key, _, err := cache.Put(cache.Size()+i, cache.Size()+i+1)
				require.NoError(t, err)
				require.Equal(t, key, 2*i+1)
			}
		})

		t.Run("starting from back", func(t *testing.T) {
			cache := setupCache(t)

			fillCache(cache)

			n := 1 + rand.Intn(cache.Size()-1)
			for i := 0; i < n; i++ {
				key, _, err := cache.Put(cache.Size()+i, cache.Size()+i+1)
				require.NoError(t, err)
				require.Equal(t, key, i)
			}
		})
	})

	t.Run("should evict visited items", func(t *testing.T) {
		cache := setupCache(t)

		fillCache(cache)

		for i := 0; i < cache.Size(); i++ {
			_, err := cache.Get(i)
			require.NoError(t, err)
		}

		n := 1 + rand.Intn(cache.Size()-1)
		for i := 0; i < n; i++ {
			key, _, err := cache.Put(cache.Size()+i, cache.Size()+i+1)
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
	require.Equal(t, cacheSize, cache.Size())

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
	require.Equal(t, initialCacheSize, cache.Size())

	for i := 0; i < initialCacheSize; i++ {
		rkey, _, err := cache.Put(i, i)
		require.NoError(t, err)
		require.Nil(t, rkey)
	}

	// cache growing
	largerCacheSize := 20
	cache.Resize(largerCacheSize)
	require.Equal(t, largerCacheSize, cache.Size())

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
	require.Equal(t, initialCacheSize, cache.Size())

	for i := 0; i < initialCacheSize; i++ {
		_, err = cache.Get(i)
		require.NoError(t, err)
	}

	for i := initialCacheSize; i < largerCacheSize; i++ {
		_, err = cache.Get(i)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}
