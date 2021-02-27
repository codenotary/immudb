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
	"io"
)

type zStreamReceiver struct {
	s               MsgReceiver
	StreamChunkSize int
}

// NewZStreamReceiver ...
func NewZStreamReceiver(s MsgReceiver, chunkSize int) *zStreamReceiver {
	return &zStreamReceiver{
		s:               s,
		StreamChunkSize: chunkSize,
	}
}

func (zr *zStreamReceiver) Next() ([]byte, []byte, float64, *bufio.Reader, error) {
	set, err := readSmallMsg(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	key, err := readSmallMsg(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	scoreBs, err := readSmallMsg(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	var score float64
	if err := Float64FromBytes(scoreBs, &score); err != nil {
		return nil, nil, 0, nil, err
	}

	// for the value, (which can be large), return a Reader and let the caller read it
	valueReader := bufio.NewReaderSize(zr.s, zr.StreamChunkSize)
	return set, key, score, valueReader, nil
}

func readSmallMsg(msgReceiver MsgReceiver, streamChunkSize int) ([]byte, error) {
	b := bytes.NewBuffer([]byte{})
	chunk := make([]byte, streamChunkSize)
	msgLen := 0
	for {
		l, err := msgReceiver.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			return nil, err
		}
		msgLen += l
		b.Write(chunk)
		if l == 0 {
			break
		}
	}
	msg := make([]byte, msgLen)
	_, err := b.Read(msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
