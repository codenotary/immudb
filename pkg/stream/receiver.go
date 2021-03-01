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
	"bytes"
	"encoding/binary"
	"io"
)

func NewMsgReceiver(stream ImmuServiceReceiver_Stream) *msgReceiver {
	return &msgReceiver{stream: stream,
		b: new(bytes.Buffer),
	}
}

type MsgReceiver interface {
	Read(message []byte) (n int, err error)
}

type msgReceiver struct {
	stream  ImmuServiceReceiver_Stream
	b       *bytes.Buffer
	eof     bool
	tl      int
	s       int
	msgSend bool
}

func (r *msgReceiver) Read(message []byte) (n int, err error) {
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
		for r.b.Len() <= len(message) {
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
		if r.eof && r.b.Len() < r.tl {
			return 0, ErrNotEnoughDataOnStream
		}

		// message send edge case
		msgInFirstChunk := r.b.Len() >= r.tl
		lastRead := r.tl-r.s <= len(message)
		lastMessageSizeTooBig := r.tl-r.s > len(message)
		if (msgInFirstChunk || lastRead) && !lastMessageSizeTooBig {
			lastMessageSize := r.tl - r.s
			lmsg := make([]byte, lastMessageSize)
			_, err := r.b.Read(lmsg)
			if err != nil {
				return 0, err
			}
			n := copy(message, lmsg)
			r.tl = 0
			r.msgSend = true
			r.s = 0
			return n, nil
		}
		// message send
		if r.b.Len() > len(message) {
			n, err := r.b.Read(message)
			if err != nil {
				return 0, err
			}
			r.s += n
			return n, nil
		}
	}
}
