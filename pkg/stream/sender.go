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
	stream          ImmuServiceSender_Stream
	b               *bytes.Buffer
	StreamChunkSize int
}

func NewMsgSender(s ImmuServiceSender_Stream, chunkSize int) *msgSender {
	buffer := new(bytes.Buffer)
	return &msgSender{
		stream:          s,
		b:               buffer,
		StreamChunkSize: chunkSize,
	}
}

func (st *msgSender) Send(reader *bufio.Reader, payloadSize int) (err error) {
	if payloadSize == 0 {
		return ErrMessageLengthIsZero
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
		data := make([]byte, st.StreamChunkSize)
		r, err := reader.Read(data)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if read == 0 && err == io.EOF {
			return  ErrReaderIsEmpty
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
		if st.b.Len() > st.StreamChunkSize {
			chunk = make([]byte, st.StreamChunkSize)
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

func (st *msgSender) RecvMsg(m interface{}) error {
	return st.stream.RecvMsg(m)
}
