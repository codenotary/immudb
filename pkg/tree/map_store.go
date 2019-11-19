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
	"encoding/binary"
)

type mapStore struct {
	data map[[9]byte]*[sha256.Size]byte
	w    uint64
}

func mapStoreKey(layer uint8, index uint64) [9]byte {
	k := [9]byte{layer}
	k[0] = layer
	binary.BigEndian.PutUint64(k[1:], index)
	return k
}

func NewMapStore() Storer {
	m := make(map[[9]byte]*[sha256.Size]byte, 256*256)
	return &mapStore{
		data: m,
	}
}

func (m *mapStore) Width() uint64 {
	return m.w
}

func (m *mapStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	m.data[mapStoreKey(layer, index)] = &value

	if layer == 0 && index >= m.w {
		m.w = index + 1
	}
}

func (m *mapStore) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return m.data[mapStoreKey(layer, index)]
}
