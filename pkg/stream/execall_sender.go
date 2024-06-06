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

	"github.com/codenotary/immudb/pkg/errors"
	"github.com/golang/protobuf/proto"
)

type execAllStreamSender struct {
	s              MsgSender
	kvStreamSender KvStreamSender
}

// NewExecAllStreamSender returns a new ExecAllStreamSender
func NewExecAllStreamSender(s MsgSender) ExecAllStreamSender {
	return &execAllStreamSender{
		s:              s,
		kvStreamSender: NewKvStreamSender(s),
	}
}

// Send send an ExecAllRequest on stream
func (st *execAllStreamSender) Send(req *ExecAllRequest) error {
	for _, op := range req.Operations {
		switch x := op.Operation.(type) {
		case *Op_KeyValue:
			st.s.Send(bytes.NewBuffer([]byte{TOp_Kv}), 1, nil)
			err := st.kvStreamSender.Send(x.KeyValue)
			if err != nil {
				return err
			}
		case *Op_ZAdd:
			err := st.s.Send(bytes.NewBuffer([]byte{TOp_ZAdd}), 1, nil)
			if err != nil {
				return err
			}
			zAddRequest, err := proto.Marshal(x.ZAdd)
			if err != nil {
				return err
			}
			err = st.s.Send(bytes.NewBuffer(zAddRequest), len(zAddRequest), nil)
			if err != nil {
				return err
			}
		case *Op_Ref:
			return errors.New(ErrRefOptNotImplemented)
		}
	}
	return nil
}
