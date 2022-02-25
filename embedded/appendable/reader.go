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
package appendable

import (
	"encoding/binary"
	"io"
)

type Reader struct {
	rAt    io.ReaderAt
	buf    []byte
	woff   int
	eof    bool
	roff   int
	offset int64
}

func NewReaderFrom(rAt io.ReaderAt, off int64, blockSize int) (*Reader, error) {
	if blockSize <= 0 {
		return nil, ErrIllegalArguments
	}

	return &Reader{
		rAt:    rAt,
		buf:    make([]byte, blockSize),
		woff:   0,
		eof:    false,
		roff:   0,
		offset: off,
	}, nil
}

func (r *Reader) Reset() {
	r.woff = 0
	r.eof = false
	r.roff = 0
	r.offset = 0
}

func (r *Reader) Offset() int64 {
	return r.offset
}

func (r *Reader) Read(bs []byte) (n int, err error) {
	read := 0

	for {
		availableInBuffer := r.woff - r.roff
		remainsToRead := len(bs) - read

		toReadInIteration := min(availableInBuffer, remainsToRead)

		copy(bs[read:], r.buf[r.roff:r.roff+toReadInIteration])
		r.roff += toReadInIteration

		read += toReadInIteration

		if read == len(bs) {
			break
		}

		// buffer was consumed and more data needs to be read

		if r.eof {
			return 0, io.EOF
		}

		// ensure block-aligned read
		remainsToReadInCurrentBlock := len(r.buf) - int(r.offset%int64(len(r.buf)))

		n, err := r.rAt.ReadAt(r.buf[:remainsToReadInCurrentBlock], r.offset)

		r.woff = n
		r.roff = 0
		r.offset += int64(n)

		if err == io.EOF {
			r.eof = true
			continue
		}

		if err != nil {
			return read + n, err
		}
	}

	return read, nil
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
