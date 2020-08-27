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

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func monotonicInsertions(t *testing.T, tbtree *TBtree, itCount int, kCount int, ascMode bool) {
	for i := 0; i < itCount; i++ {
		for j := 0; j < kCount; j++ {
			k := make([]byte, 4)
			if ascMode {
				binary.BigEndian.PutUint32(k, uint32(j))
			} else {
				binary.BigEndian.PutUint32(k, uint32(kCount-j))
			}

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+j))

			ts := uint64(i*kCount+j) + 1

			root, err := tbtree.Root()
			assert.NoError(t, err)
			assert.Equal(t, ts-1, root.Ts())

			v1, ts1, err := root.Get(k)

			if i == 0 {
				assert.Equal(t, ErrKeyNotFound, err)
			} else {
				assert.NoError(t, err)

				expectedV := make([]byte, 8)
				binary.BigEndian.PutUint64(expectedV, uint64((i-1)<<4+j))
				assert.Equal(t, expectedV, v1)

				expectedTs := uint64((i-1)*kCount+j) + 1
				assert.Equal(t, expectedTs, ts1)
			}

			err = tbtree.Insert(k, v, uint64(ts))
			assert.NoError(t, err)

			root, err = tbtree.Root()
			assert.NoError(t, err)
			assert.Equal(t, ts, root.Ts())

			v1, ts1, err = root.Get(k)
			assert.NoError(t, err)
			assert.Equal(t, v, v1)
			assert.Equal(t, ts, ts1)
		}
	}
}

func randomInsertions(t *testing.T, tbtree *TBtree, kCount int) {
	seed := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(seed)

	for i := 0; i < kCount; i++ {
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k, rnd.Uint32())

		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i))

		ts := uint64(i + 1)

		err := tbtree.Insert(k, v, uint64(ts))
		assert.NoError(t, err)

		root, err := tbtree.Root()
		assert.NoError(t, err)
		assert.Equal(t, ts, root.Ts())

		v1, ts1, err := root.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, v, v1)
		assert.Equal(t, ts, ts1)
	}
}

func TestTBTreeInsertionInAscendingOrder(t *testing.T) {
	tbtree, _ := NewWith(DefaultOptions().setMaxNodeSize(256))

	monotonicInsertions(t, tbtree, 10, 1000, true)
}

func TestTBTreeInsertionInDescendingOrder(t *testing.T) {
	tbtree, _ := NewWith(DefaultOptions().setMaxNodeSize(256))

	monotonicInsertions(t, tbtree, 10, 1000, false)
}

func TestTBTreeInsertionInRandomOrder(t *testing.T) {
	tbtree, _ := NewWith(DefaultOptions().setMaxNodeSize(DefaultMaxNodeSize))

	randomInsertions(t, tbtree, 1000)
}
