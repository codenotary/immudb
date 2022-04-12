/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package stream

import (
	"bytes"
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/errors"
	"io"
)

type MsgSender interface {
	Send(reader io.Reader, payloadSize int) (err error)
	RecvMsg(m interface{}) error
}

type msgSender struct {
	stream     ImmuServiceSender_Stream
	b          *bytes.Buffer
	BufferSize int
}

// NewMsgSender returns a NewMsgSender. It can be used on server side or client side to send a message on a stream.
func NewMsgSender(s ImmuServiceSender_Stream, bs int) *msgSender {
	buffer := new(bytes.Buffer)
	return &msgSender{
		stream:     s,
		b:          buffer,
		BufferSize: bs,
	}
}

// Send reads from a reader until it reach payloadSize. It fill an internal buffer from what it read from reader and, when there is enough data, it sends a chunk on stream.
// It continues until it reach the payloadsize. At that point it sends the last content of the buffer.
func (st *msgSender) Send(reader io.Reader, payloadSize int) (err error) {
	if payloadSize == 0 {
		return errors.New(ErrMessageLengthIsZero)
	}
	var read = 0
	var run = true
	for run {
		// if read is 0 here trailer is created
		if read == 0 {
			ml := make([]byte, 8)
			binary.BigEndian.PutUint64(ml, uint64(payloadSize))
			st.b.Write(ml)
		}
		// read data from reader and append it to the buffer
		//  todo @Michele reader need to be dynamic, not of chunk size
		data := make([]byte, st.BufferSize)
		r, err := reader.Read(data)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if read == 0 && err == io.EOF {
			return errors.New(ErrReaderIsEmpty)
		}
		read += r
		if read < payloadSize && err == io.EOF {
			return errors.New(ErrNotEnoughDataOnStream)
		}

		// append read data in the buffer
		st.b.Write(data[:r])

		// chunk processing
		var chunk []byte
		// last chunk creation
		if read == payloadSize {
			chunk = make([]byte, st.b.Len())
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// enough data to send a chunk
		if st.b.Len() > st.BufferSize {
			chunk = make([]byte, st.BufferSize)
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// sending ...
		if len(chunk) > 0 {
			err = st.stream.Send(&schema.Chunk{
				Content: chunk,
			})
			if err != nil {
				return err
			}
			// is last chunk
			if read == payloadSize {
				run = false
			}
		}
	}
	return nil
}

// RecvMsg block until it receives a message from the receiver (here we are on the sender). It's used mainly to retrieve an error message after sending data from a client(SDK) perspective.
func (st *msgSender) RecvMsg(m interface{}) error {
	return st.stream.RecvMsg(m)
}
