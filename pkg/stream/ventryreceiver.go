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
)

type vEntryStreamReceiver struct {
	s               MsgReceiver
	StreamChunkSize int
}

// NewVEntryStreamReceiver ...
func NewVEntryStreamReceiver(s MsgReceiver, chunkSize int) *vEntryStreamReceiver {
	return &vEntryStreamReceiver{
		s:               s,
		StreamChunkSize: chunkSize,
	}
}

func (ver *vEntryStreamReceiver) Next() ([]byte, []byte, []byte, *bufio.Reader, error) {
	key, err := ParseValue(bufio.NewReader(ver.s), ver.StreamChunkSize)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	verifiableTx, err := ParseValue(bufio.NewReader(ver.s), ver.StreamChunkSize)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	inclusionProof, err := ParseValue(bufio.NewReader(ver.s), ver.StreamChunkSize)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// for the value, (which can be large), return a Reader and let the caller read it
	valueReader := bufio.NewReaderSize(ver.s, ver.StreamChunkSize)
	return key, verifiableTx, inclusionProof, valueReader, nil
}
