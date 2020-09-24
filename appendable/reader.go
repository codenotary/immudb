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
package appendable

import "io"

type Reader struct {
	rAt       io.ReaderAt
	data      []byte
	dataIndex int
	eof       bool
	readIndex int
	offset    int64
}

func NewReaderFrom(rAt io.ReaderAt, off int64, size int) *Reader {
	return &Reader{
		rAt:       rAt,
		data:      make([]byte, size),
		dataIndex: 0,
		eof:       false,
		readIndex: 0,
		offset:    off,
	}
}

func (r *Reader) Reset(off int64) {
	//TODO: data might be reused
	r.dataIndex = 0
	r.eof = false
	r.readIndex = 0
	r.offset = off
}

func (r *Reader) Read(bs []byte) (n int, err error) {
	l := 0

	for {
		nl := min(r.dataIndex-r.readIndex, len(bs)-l)

		copy(bs[l:], r.data[r.readIndex:r.readIndex+nl])
		r.readIndex += nl

		l += nl

		if l == len(bs) {
			break
		}

		if r.eof {
			return 0, io.EOF
		}

		n, err := r.rAt.ReadAt(r.data, r.offset)

		r.dataIndex = n
		r.readIndex = 0
		r.offset += int64(n)

		if err == io.EOF {
			r.eof = true
			continue
		}

		if err != nil {
			return l + n, err
		}
	}

	return l, nil
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
