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
	"io"
)

type kvStreamReceiver struct {
	s               MsgReceiver
	StreamChunkSize int
}

// NewKvStreamReceiver returns a new kvStreamReceiver
func NewKvStreamReceiver(s MsgReceiver, chunkSize int) *kvStreamReceiver {
	return &kvStreamReceiver{
		s:               s,
		StreamChunkSize: chunkSize,
	}
}

// Next returns the following key and value reader pair found on stream. If no more key values are presents on stream it returns io.EOF
func (kvr *kvStreamReceiver) Next() ([]byte, io.Reader, error) {
	b := bytes.NewBuffer([]byte{})
	chunk := make([]byte, kvr.StreamChunkSize)
	keyl := 0
	for {
		l, err := kvr.s.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if err == io.EOF {
			return nil, nil, err
		}
		keyl += l
		b.Write(chunk)
		if l == 0 {
			break
		}
	}
	key := make([]byte, keyl)
	_, err := b.Read(key)
	if err != nil {
		return nil, nil, err
	}

	return key, kvr.s, nil
}
