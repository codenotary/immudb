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
	"errors"
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestNewExecAllStreamSender(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	eas := NewExecAllStreamSender(s)
	require.IsType(t, new(execAllStreamSender), eas)
}

func TestExecAllStreamSender_Send(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`exec-all-set`),
						Score:    85.4,
						Key:      []byte(`exec-all-key`),
						AtTx:     0,
						BoundRef: true,
					},
				},
			},
			{
				Operation: &Op_KeyValue{
					KeyValue: &KeyValue{
						Key: &ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-key2`)),
							Size:    len([]byte(`exec-all-key2`)),
						},
						Value: &ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-val2`)),
							Size:    len([]byte(`exec-all-val2`)),
						},
					},
				},
			},
		},
	}
	err := eas.Send(aOps)
	require.NoError(t, err)
}

func TestExecAllStreamSender_SendZAddError(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return errCustom
	}
	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`exec-all-set`),
						Score:    85.4,
						Key:      []byte(`exec-all-key`),
						AtTx:     0,
						BoundRef: true,
					},
				},
			},
		},
	}
	err := eas.Send(aOps)
	require.ErrorIs(t, err, errCustom)
}

func TestExecAllStreamSender_SendZAddError2(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)

	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_ZAdd{
					ZAdd: nil,
				},
			},
		},
	}
	err := eas.Send(aOps)
	require.ErrorContains(t, err, proto.ErrNil.Error())
}

func TestExecAllStreamSender_SendZAddError3(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	sec := false
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		if sec {
			return errCustom
		}
		sec = true
		return nil
	}

	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`exec-all-set`),
						Score:    85.4,
						Key:      []byte(`exec-all-key`),
						AtTx:     0,
						BoundRef: true,
					},
				},
			},
		},
	}
	err := eas.Send(aOps)
	require.ErrorIs(t, err, errCustom)
}

func TestExecAllStreamSender_SendKVError(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return errCustom
	}
	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_KeyValue{
					KeyValue: &KeyValue{
						Key: &ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-key2`)),
							Size:    len([]byte(`exec-all-key2`)),
						},
						Value: &ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-val2`)),
							Size:    len([]byte(`exec-all-val2`)),
						},
					},
				},
			},
		},
	}
	err := eas.Send(aOps)
	require.ErrorIs(t, err, errCustom)
}

func TestExecAllStreamSender_SendRefError(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return errors.New("custom one")
	}
	eas := NewExecAllStreamSender(s)

	aOps := &ExecAllRequest{
		Operations: []*Op{
			{
				Operation: &Op_Ref{},
			},
		},
	}
	err := eas.Send(aOps)
	require.ErrorContains(t, err, ErrRefOptNotImplemented)
}
