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

func NewZStreamReceiver(s MsgReceiver, chunkSize int) *zStreamReceiver {
	return &zStreamReceiver{
		s:               s,
		StreamChunkSize: chunkSize,
	}
}

func (zr *zStreamReceiver) Next() ([]byte, *bufio.Reader, error) {
	b := bytes.NewBuffer([]byte{})
	chunk := make([]byte, zr.StreamChunkSize)
	setl := 0
	for {
		l, err := zr.s.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if err == io.EOF {
			return nil, nil, err
		}
		setl += l
		b.Write(chunk)
		if l == 0 {
			break
		}
	}
	set := make([]byte, setl)
	_, err := b.Read(set)
	if err != nil {
		return nil, nil, err
	}

	return set, bufio.NewReaderSize(zr.s, zr.StreamChunkSize), nil
}
