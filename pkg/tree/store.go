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
)

type Storer interface {
	Width() uint64
	Set(layer uint8, index uint64, value [sha256.Size]byte)
	Get(layer uint8, index uint64) *[sha256.Size]byte
}

type memStore struct {
	data [][][sha256.Size]byte
}

func NewMemStore() Storer {
	return &memStore{
		data: make([][][sha256.Size]byte, 1),
	}
}

func (m *memStore) Width() uint64 {
	return uint64(len(m.data[0]))
}

func (m *memStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {

	for uint8(len(m.data)) <= layer {
		m.data = append(m.data, make([][sha256.Size]byte, 0, 256*256))
	}

	l := uint64(len(m.data[layer]))
	if l < index {
		m.data[layer] = append(m.data[layer], make([][sha256.Size]byte, l-index)...)
	}

	if l == index {
		m.data[layer] = append(m.data[layer], value)
	} else {
		m.data[layer][index] = value
	}
}

func (m *memStore) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return &m.data[layer][index]
}
