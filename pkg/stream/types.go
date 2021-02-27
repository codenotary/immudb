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
)

type KeyValue struct {
	Key   *ValueSize
	Value *ValueSize
}

type ValueSize struct {
	Content *bufio.Reader
	Size    int
}

type ZEntry struct {
	Set   *ValueSize
	Key   *ValueSize
	Score *ValueSize
	Value *ValueSize
}

// Float64ToBytes ...
func Float64ToBytes(f float64) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, f)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// Float64FromBytes ...
func Float64FromBytes(bs []byte, f *float64) error {
	buf := bytes.NewReader(bs)
	return binary.Read(buf, binary.BigEndian, f)
}
