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
	return &msgReceiver{stream: stream, b: bytes.NewBuffer([]byte{})}
}

type MsgReceiver interface {
	Read(message []byte) (n int, err error)
}

type msgReceiver struct {
	stream ImmuServiceReceiver_Stream
	b      *bytes.Buffer
	l      uint64
	r      int
	msgRcv bool
	eof    bool
}

func (r *msgReceiver) Read(message []byte) (n int, err error) {
	for {
		// if message is fully received but there is more data in stream 0 and error nil is returned
		if r.msgRcv {
			r.msgRcv = false
			return 0, nil
		}
		// if message is fully received and there is no more data in stream 0 and EOF is returned
		if r.eof {
			return 0, io.EOF
		}
		// read data in stream
		chunk, err := r.stream.Recv()
		if err != nil {
			// no more data in stream
			if err == io.EOF {
				// no more data present in stream, but if buffer contains data it need to be return
				if r.b.Len() > 0 {
					lmsg := make([]byte, int(r.l)-r.r)
					read, err := r.b.Read(lmsg)
					if err != nil {
						return 0, err
					}
					r.eof = true
					copy(message, lmsg)
					return read, nil
				}
				// no more data in stream, buffer empty
				return 0, err
			}
			// shouldn't happen
			return -1, err
		}
		if chunk != nil {
			r.b.Write(chunk.Content)
		}
		// trailer creation, read counter and trailer length are 0
		if r.r == 0 && r.l == 0 {
			trailer := make([]byte, 8)
			_, err = r.b.Read(trailer)
			if err != nil {
				return 0, err
			}
			r.l = binary.BigEndian.Uint64(trailer)
		}
		// there is enough data in buffer to return the message. This happen when a single chunk contains both the trailer and the message
		if r.b.Len() >= int(r.l) {
			lmsg := make([]byte, int(r.l))
			read, err := r.b.Read(lmsg)
			if err != nil {
				return 0, err
			}
			r.r = 0
			r.l = 0
			r.msgRcv = true
			copy(message, lmsg)
			return read, nil
		}
		// buffer loading
		if r.b.Len() >= len(message) {
			read, err := r.b.Read(message)
			if err != nil {
				return 0, err
			}
			r.r += read
			return read, err
		}
		// last message. Buffer contains enough data to return the last message
		if r.b.Len() >= int(r.l)-r.r {
			lmsg := make([]byte, int(r.l)-r.r)
			read, err := r.b.Read(lmsg)
			if err != nil {
				return 0, err
			}
			r.r = 0
			r.l = 0
			r.msgRcv = true
			copy(message, lmsg)
			return read, nil
		}
	}
}
