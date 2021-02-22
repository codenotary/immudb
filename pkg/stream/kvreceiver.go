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
	"fmt"
	"io"
)

type kvStreamReceiver struct {
	c int
	s MsgReceiver
}

func NewKvStreamReceiver(s MsgReceiver) *kvStreamReceiver {
	return &kvStreamReceiver{
		s: s,
	}
}

func (kvr *kvStreamReceiver) NextKey() ([]byte, error) {
	if kvr.c%2 == 0 {
		b := bytes.NewBuffer([]byte{})
		chunk := make([]byte, ChunkSize)
		keyl := 0
		//kr := bufio.NewReader(kvr.s)
		for {
			l, err := kvr.s.Read(chunk)
			if err != nil && err != io.EOF {
				return nil, err
			}
			keyl += l
			b.Write(chunk)
			if err == io.EOF || l == 0 {
				kvr.c++
				break
			}
		}
		key := make([]byte, keyl)
		_, err := b.Read(key)
		if err != nil {
			return nil, err
		}
		return key, nil
	} else {
		return nil, fmt.Errorf("key not available, use NextValueReader first")
	}
}
func (kvr *kvStreamReceiver) NextValueReader() (*bufio.Reader, error) {
	if kvr.c%2 != 0 {
		kvr.c++
		return bufio.NewReader(kvr.s), nil
	} else {
		return nil, fmt.Errorf("key not available, use NextKey first")
	}
}
