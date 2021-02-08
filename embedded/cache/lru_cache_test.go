/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheCreation(t *testing.T) {
	_, err := NewLRUCache(0)
	require.Equal(t, ErrIllegalArguments, err)

	cacheSize := 10
	cache, err := NewLRUCache(cacheSize)
	require.NoError(t, err)
	require.NotNil(t, cache)
	require.Equal(t, cacheSize, cache.Size())

	_, err = cache.Get(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = cache.Put(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

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
		v, err := cache.Get(i)
		require.NoError(t, err)
		require.Equal(t, v, 10*i)
	}

	for i := cacheSize / 2; i < cacheSize; i++ {
		_, err = cache.Get(i)
		require.Equal(t, ErrKeyNotFound, err)
	}

	for i := cacheSize; i < cacheSize+cacheSize/2; i++ {
		v, err := cache.Get(i)
		require.NoError(t, err)
		require.Equal(t, v, 10*i)
	}
}

func TestApply(t *testing.T) {
	cacheSize := 10
	cache, err := NewLRUCache(cacheSize)
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
