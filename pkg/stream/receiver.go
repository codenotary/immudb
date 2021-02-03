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

package stream

import (
	"bytes"
	"encoding/binary"
)

func NewMsgReceiver(stream ImmuServiceReceiver_Stream) *msgReceiver {
	return &msgReceiver{stream: stream, b: bytes.NewBuffer([]byte{})}
}

type MsgReceiver interface {
	Recv() ([]byte, error)
}

type msgReceiver struct {
	stream ImmuServiceReceiver_Stream
	b      *bytes.Buffer
}

func (r *msgReceiver) Recv() ([]byte, error) {
	var l uint64 = 0
	var message []byte
	for {
		chunk, err := r.stream.Recv()
		if err != nil {
			return nil, err
		}
		r.b.Write(chunk.Content)
		// if l is zero need to read the trailer to get the message length
		if l == 0 {
			trailer := make([]byte, 8)
			_, err = r.b.Read(trailer)
			if err != nil {
				return nil, err
			}
			l = binary.BigEndian.Uint64(trailer)
			message = make([]byte, l)
		}
		// if message is contained in the first chunk is returned
		if r.b.Len() >= int(l) {
			_, err = r.b.Read(message)
			if err != nil {
				return nil, err
			}
			return message, nil
		}
	}
}
