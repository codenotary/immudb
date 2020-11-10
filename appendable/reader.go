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

import (
	"encoding/binary"
	"io"
)

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

func (r *Reader) Offset() int64 {
	return r.offset
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

func (r *Reader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := r.Read(b[:])
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *Reader) ReadUint64() (uint64, error) {
	var b [8]byte
	_, err := r.Read(b[:8])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	var b [4]byte
	_, err := r.Read(b[:4])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
