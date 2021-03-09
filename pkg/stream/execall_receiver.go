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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

type execAllStreamReceiver struct {
	s                MsgReceiver
	kvStreamReceiver KvStreamReceiver
	StreamChunkSize  int
}

// NewExecAllStreamReceiver returns a new execAllStreamReceiver
func NewExecAllStreamReceiver(s MsgReceiver, chunkSize int) ExecAllStreamReceiver {
	return &execAllStreamReceiver{
		s:                s,
		kvStreamReceiver: NewKvStreamReceiver(s, chunkSize),
		StreamChunkSize:  chunkSize,
	}
}

// Next returns the following exec all operation found on the wire. If no more operations are presents on stream it returns io.EOF
func (eas *execAllStreamReceiver) Next() (IsOp_Operation, error) {
	for {
		t, err := ReadValue(eas.s, eas.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		switch t[0] {
		case TOp_Kv:
			key, vr, err := eas.kvStreamReceiver.Next()
			if err != nil {
				return nil, err
			}
			return &Op_KeyValue{
				KeyValue: &KeyValue{
					Key: &ValueSize{
						Content: bytes.NewBuffer(key),
						Size:    len(key),
					},
					Value: &ValueSize{
						Content: vr,
					},
				},
			}, nil
		case TOp_ZAdd:
			zr := &schema.ZAddRequest{}
			zaddm, err := ReadValue(eas.s, eas.StreamChunkSize)
			err = proto.Unmarshal(zaddm, zr)
			if err != nil {
				return nil, ErrUnableToReassembleExecAllMessage
			}
			return &Op_ZAdd{
				ZAdd: zr,
			}, nil
		case TOp_Ref:
			return nil, ErrRefOptNotImplemented
		}
	}
}
