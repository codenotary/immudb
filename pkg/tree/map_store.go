/*
Copyright 2019 vChain, Inc.

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

package tree

import (
	"crypto/sha256"
	"fmt"
	"math/bits"
)

type mapStore struct {
	data map[uint8]map[uint64]*[sha256.Size]byte
	w    uint64
}

func NewMapStore() Storer {
	m := make(map[uint8]map[uint64]*[sha256.Size]byte, 64)
	for l := uint8(0); l < 64; l++ {
		m[l] = make(map[uint64]*[sha256.Size]byte)
	}
	return &mapStore{
		data: m,
	}
}

func (m *mapStore) Width() uint64 {
	return m.w
}

func (m *mapStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {

	// if _, ok := m.data[layer]; !ok {
	// 	m.data[layer] = make(map[uint64]*[sha256.Size]byte, 256*256)
	// }

	m.data[layer][index] = &value

	if layer == 0 && index >= m.w {
		m.w = index + 1
	}
}

func (m *mapStore) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return m.data[layer][index]
}

func (m *mapStore) Print() {
	if m.w == 0 {
		return
	}
	for l := uint8(bits.Len64(uint64(m.w - 1))); true; l-- {
		if row, ok := m.data[l]; ok {
			for i := uint64(0); true; i++ {
				if v, ok := row[i]; ok {
					fmt.Printf("%x ", v[0])
				} else {
					break
				}
			}
		}
		fmt.Println()
		if l == 0 {
			return
		}
	}
}
