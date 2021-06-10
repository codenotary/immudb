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
	"errors"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
	s.SendF = func(reader io.Reader, payloadSize int) (err error) {
		return errors.New("custom one")
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
	require.Error(t, err)
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
	require.Error(t, err)
}

func TestExecAllStreamSender_SendZAddError3(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	sec := false
	s.SendF = func(reader io.Reader, payloadSize int) (err error) {
		if sec {
			return errors.New("custom one")
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
	require.Error(t, err)
}

func TestExecAllStreamSender_SendKVError(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int) (err error) {
		return errors.New("custom one")
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
	require.Error(t, err)
}

func TestExecAllStreamSender_SendRefError(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int) (err error) {
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
	require.Equal(t, ErrRefOptNotImplemented, err.Error())
}
