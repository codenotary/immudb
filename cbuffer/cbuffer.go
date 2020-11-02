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
package chbuffer

import (
	"crypto/sha256"
	"errors"
	"sync"
)

var ErrBufferIsFull = errors.New("Buffer is full")
var ErrBufferIsEmpty = errors.New("Buffer is empty")

type CHBuffer struct {
	buf  [][sha256.Size]byte
	rpos int
	wpos int
	full bool

	mux sync.Mutex
}

func New(size int) *CHBuffer {
	return &CHBuffer{
		buf:  make([][sha256.Size]byte, size),
		rpos: 0,
		wpos: 0,
	}
}

func (b *CHBuffer) freeSlots() int {
	if b.full {
		return 0
	}

	if b.rpos <= b.wpos {
		return len(b.buf) - (b.wpos - b.rpos)
	}

	return b.rpos - b.wpos
}

func (b *CHBuffer) Put(h [sha256.Size]byte) error {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.freeSlots() == 0 {
		return ErrBufferIsFull
	}

	b.wpos = (b.wpos + 1) % len(b.buf)
	b.buf[b.wpos] = h
	b.full = b.rpos == b.wpos

	return nil
}

func (b *CHBuffer) Get() (h [sha256.Size]byte, err error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.freeSlots() == len(b.buf) {
		err = ErrBufferIsEmpty
		return
	}

	b.rpos = (b.rpos + 1) % len(b.buf)
	h = b.buf[b.rpos]
	b.full = false

	return
}
