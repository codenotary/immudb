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

package stream

import (
	"encoding/binary"
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
)

type MsgSender interface {
	Send(reader io.Reader, chunkSize int, metadata map[string][]byte) (err error)
	RecvMsg(m interface{}) error
}

type msgSender struct {
	stream ImmuServiceSender_Stream
	buf    []byte
	chunk  *schema.Chunk
}

// NewMsgSender returns a NewMsgSender. It can be used on server side or client side to send a message on a stream.
func NewMsgSender(s ImmuServiceSender_Stream, buf []byte) *msgSender {
	return &msgSender{
		stream: s,
		buf:    buf,
		chunk:  &schema.Chunk{},
	}
}

// Send reads from a reader until it reach msgSize. It fill an internal buffer from what it read from reader and, when there is enough data, it sends a chunk on stream.
// It continues until it reach the msgSize. At that point it sends the last content of the buffer.
func (st *msgSender) Send(reader io.Reader, msgSize int, metadata map[string][]byte) error {
	available := len(st.buf)

	// first chunk begins with the message size and including metadata
	binary.BigEndian.PutUint64(st.buf, uint64(msgSize))
	available -= 8

	st.chunk.Metadata = metadata

	read := 0

	for read < msgSize {
		n, err := reader.Read(st.buf[len(st.buf)-available:])
		if err != nil {
			return err
		}

		available -= n
		read += n

		if available == 0 {
			// send chunk when it's full
			st.chunk.Content = st.buf[:len(st.buf)-available]

			err = st.stream.Send(st.chunk)
			if err != nil {
				return err
			}

			available = len(st.buf)

			// metadata is only included into the first chunk
			st.chunk.Metadata = nil
		}
	}

	if available < len(st.buf) {
		// send last partially written chunk
		st.chunk.Content = st.buf[:len(st.buf)-available]

		err := st.stream.Send(st.chunk)
		if err != nil {
			return err
		}

		// just to avoid keeping a useless reference
		st.chunk.Metadata = nil
	}

	return nil
}

// RecvMsg block until it receives a message from the receiver (here we are on the sender). It's used mainly to retrieve an error message after sending data from a client(SDK) perspective.
func (st *msgSender) RecvMsg(m interface{}) error {
	return st.stream.RecvMsg(m)
}
