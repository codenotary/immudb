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
	readCount int64 // total number of read bytes
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

func (r *Reader) Reset() {
	r.dataIndex = 0
	r.eof = false
	r.readIndex = 0
	r.offset = 0
	r.readCount = 0
}

func (r *Reader) Offset() int64 {
	return r.offset
}

func (r *Reader) ReadCount() int64 {
	return r.readCount
}

func (r *Reader) Read(bs []byte) (n int, err error) {
	defer func() {
		r.readCount += int64(n)
	}()

	for {
		bn := min(r.dataIndex-r.readIndex, len(bs)-n)

		copy(bs[n:], r.data[r.readIndex:r.readIndex+bn])
		r.readIndex += bn

		n += bn

		if n == len(bs) {
			break
		}

		if r.eof {
			return n, io.EOF
		}

		rn, err := r.rAt.ReadAt(r.data, r.offset)

		r.dataIndex = rn
		r.readIndex = 0
		r.offset += int64(rn)

		if err == io.EOF {
			r.eof = true
			continue
		}

		if err != nil {
			return n, err
		}
	}

	return n, nil
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

func (r *Reader) ReadUint16() (uint16, error) {
	var b [2]byte
	_, err := r.Read(b[:2])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
