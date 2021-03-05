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

func (zr *zStreamReceiver) Next() ([]byte, []byte, float64, uint64, io.Reader, error) {
	set, err := ReadValue(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}

	key, err := ReadValue(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}

	scoreBs, err := ReadValue(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}
	var score float64
	if err := NumberFromBytes(scoreBs, &score); err != nil {
		return nil, nil, 0, 0, nil, err
	}

	atTxBs, err := ReadValue(zr.s, zr.StreamChunkSize)
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}
	var atTx uint64
	if err := NumberFromBytes(atTxBs, &atTx); err != nil {
		return nil, nil, 0, 0, nil, err
	}

	// for the value, (which can be large), return a Reader and let the caller read it
	return set, key, score, atTx, zr.s, nil
}
