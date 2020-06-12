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

package ring

// Buffer ...
type Buffer interface {
	Head() uint64
	Tail() uint64
	Get(uint64) interface{}
	Set(uint64, interface{})
}

type ringBuffer struct {
	head uint64
	tail uint64
	size uint64
	data []interface{}
}

// NewRingBuffer ...
func NewRingBuffer(size uint64) Buffer {
	return &ringBuffer{
		size: size,
		data: make([]interface{}, size),
	}
}

func (r *ringBuffer) Head() uint64 {
	return r.head
}

func (r *ringBuffer) Tail() uint64 {
	return r.tail
}

func (r *ringBuffer) Get(i uint64) interface{} {
	if i < r.head {
		return nil
	}
	if i >= r.tail {
		return nil
	}
	if r.tail-i >= r.size {
		return nil
	}
	return r.data[i%r.size]
}

func (r *ringBuffer) Set(i uint64, value interface{}) {
	if i > (r.size + r.head) {
		r.head = i - r.size
	}
	if i >= r.tail {
		r.tail = i + 1
	}

	r.data[i%r.size] = value
}
