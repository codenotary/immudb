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
	"io"

	"github.com/codenotary/immudb/pkg/errors"
)

// NewMsgReceiver returns a NewMsgReceiver reader
func NewMsgReceiver(stream ImmuServiceReceiver_Stream) *msgReceiver {
	return &msgReceiver{stream: stream,
		b: new(bytes.Buffer),
	}
}

type MsgReceiver interface {
	Read(data []byte) (n int, err error)
	ReadFully() ([]byte, error)
}

type msgReceiver struct {
	stream  ImmuServiceReceiver_Stream
	b       *bytes.Buffer
	eof     bool
	tl      int
	s       int
	msgSend bool
}

// ReadFully reads the entire message that could be transmitted in several chunks
func (r *msgReceiver) ReadFully() ([]byte, error) {
	firstChunk, err := r.stream.Recv()
	if err != nil {
		return nil, err
	}
	if len(firstChunk.Content) < 8 {
		return nil, errors.New(ErrChunkTooSmall)
	}

	messageLen := int(binary.BigEndian.Uint64(firstChunk.Content))

	b := make([]byte, messageLen)
	i := 0

	copy(b, firstChunk.Content[8:])
	i += len(firstChunk.Content) - 8

	for {
		chunk, err := r.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		copy(b[i:], chunk.Content)
		i += len(chunk.Content)
	}

	if messageLen > i {
		return nil, errors.New(ErrNotEnoughDataOnStream)
	}

	return b, nil
}

// Read read fill message with received data and return the number of read bytes or error. If no message is present it returns 0 and io.EOF. If the message is complete it returns 0 and nil, in that case successive calls to Read will returns a new message.
func (r *msgReceiver) Read(data []byte) (n int, err error) {
	if r.msgSend {
		r.msgSend = false
		return 0, nil
	}
	// if message is fully received and there is no more data in stream 0 and EOF is returned
	if r.eof && r.b.Len() == 0 {
		return 0, io.EOF
	}

	for {
		// buffer until reach the capacity of the message
	bufferLoad:
		for r.b.Len() <= len(data) {
			chunk, err := r.stream.Recv()
			if chunk != nil {
				r.b.Write(chunk.Content)
			}
			if err != nil {
				// no more data in stream
				if err == io.EOF {
					r.eof = true
					break bufferLoad
				}
				return 0, err
			}
		}

		// trailer (message length) initialization
		if r.tl == 0 {
			trailer := make([]byte, 8)
			_, err = r.b.Read(trailer)
			if err != nil {
				return 0, err
			}
			r.tl = int(binary.BigEndian.Uint64(trailer))
		}

		// no more data in stream but buffer is not enough large to contains the expected value
		if r.eof && r.b.Len() < r.tl-r.s {
			return 0, errors.New(ErrNotEnoughDataOnStream)
		}

		// message send edge cases
		msgInFirstChunk := r.b.Len() >= r.tl
		lastRead := r.tl-r.s <= len(data)
		lastMessageSizeTooBig := r.tl-r.s > len(data)
		if (msgInFirstChunk || lastRead) && !lastMessageSizeTooBig {
			lastMessageSize := r.tl - r.s
			lmsg := make([]byte, lastMessageSize)
			_, err := r.b.Read(lmsg)
			if err != nil {
				return 0, err
			}
			n := copy(data, lmsg)
			r.tl = 0
			r.msgSend = true
			r.s = 0
			return n, nil
		}
		// message send
		if r.b.Len() > len(data) {
			n, err := r.b.Read(data)
			if err != nil {
				return 0, err
			}
			r.s += n
			return n, nil
		}
	}
}
