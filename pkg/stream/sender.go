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

package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
	"io"
)

type MsgSender interface {
	Send(reader *bufio.Reader, payloadSize int) (err error)
	RecvMsg(m interface{}) error
}

type msgSender struct {
	s ImmuServiceSender_Stream
	b *bytes.Buffer
}

func NewMsgSender(s ImmuServiceSender_Stream) *msgSender {
	buffer := bytes.NewBuffer([]byte{})
	return &msgSender{
		s: s,
		b: buffer,
	}
}

func (st *msgSender) Send(reader *bufio.Reader, payloadSize int) (err error) {
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
		data := make([]byte, reader.Size())
		r, err := reader.Read(data)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}

		read += r
		// no more data to read in the reader, exit
		if read == 0 {
			return nil
		}

		// append read data in the buffer
		st.b.Write(data[:r])

		// chunk processing
		var chunk []byte
		// last chunk creation
		if read == payloadSize {
			chunk = make([]byte, st.b.Len())
			if err != nil {
				return nil
			}
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// enough data to send a chunk
		if st.b.Len() > ChunkSize {
			chunk = make([]byte, ChunkSize)
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// sending ...
		if len(chunk) > 0 {
			err = st.s.Send(&schema.Chunk{
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

func (st *msgSender) RecvMsg(m interface{}) error {
	return st.s.RecvMsg(m)
}
