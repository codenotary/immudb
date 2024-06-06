/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/golang/protobuf/proto"
)

type execAllStreamReceiver struct {
	s                io.Reader
	kvStreamReceiver KvStreamReceiver
	BufferSize       int
}

// NewExecAllStreamReceiver returns a new execAllStreamReceiver
func NewExecAllStreamReceiver(s io.Reader, bs int) ExecAllStreamReceiver {
	return &execAllStreamReceiver{
		s:                s,
		kvStreamReceiver: NewKvStreamReceiver(s, bs),
		BufferSize:       bs,
	}
}

// Next returns the following exec all operation found on the wire. If no more operations are presents on stream it returns io.EOF
func (eas *execAllStreamReceiver) Next() (IsOp_Operation, error) {
	for {
		t, err := ReadValue(eas.s, eas.BufferSize)
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
			zaddm, err := ReadValue(eas.s, eas.BufferSize)
			err = proto.Unmarshal(zaddm, zr)
			if err != nil {
				return nil, errors.New(ErrUnableToReassembleExecAllMessage)
			}
			return &Op_ZAdd{
				ZAdd: zr,
			}, nil
		case TOp_Ref:
			return nil, errors.New(ErrRefOptNotImplemented)
		}
	}
}
