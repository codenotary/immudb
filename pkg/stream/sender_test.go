/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"testing"

	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
)

func TestNewMsgSender(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	require.IsType(t, new(msgSender), s)
}

func TestMsgSender_Send(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))

	content := []byte(`mycontent`)
	message := bytes.Join([][]byte{streamtest.GetTrailer(len(content)), content}, nil)
	b := bytes.NewBuffer(message)
	err := s.Send(b, b.Len(), nil)
	require.NoError(t, err)
}

func TestMsgSender_SendPayloadSizeZero(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	b := bytes.NewBuffer(nil)
	err := s.Send(b, 0, nil)
	require.NoError(t, err)
}

func TestMsgSender_SendErrReader(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	r := &streamtest.ErrReader{
		ReadF: func([]byte) (int, error) {
			return 0, errCustom
		},
	}
	err := s.Send(r, 5000, nil)
	require.ErrorIs(t, err, errCustom)
}

func TestMsgSender_SendEmptyReader(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	r := &streamtest.ErrReader{
		ReadF: func([]byte) (int, error) {
			return 0, io.EOF
		},
	}
	err := s.Send(r, 5000, nil)
	require.ErrorIs(t, err, io.EOF)
}

func TestMsgSender_SendEErrNotEnoughDataOnStream(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))

	content := []byte(`mycontent`)
	message := streamtest.GetTrailer(len(content))
	b := bytes.NewBuffer(message)
	err := s.Send(b, 5000, nil)
	require.ErrorIs(t, err, io.EOF)
}

func TestMsgSender_SendLastChunk(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))

	content := []byte(`mycontent`)
	b := bytes.NewBuffer(content)
	err := s.Send(b, len(content), nil)
	require.NoError(t, err)
}

func TestMsgSender_SendMultipleChunks(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 8))

	content := []byte(`mycontent`)
	b := bytes.NewBuffer(content)
	err := s.Send(b, len(content), nil)
	require.NoError(t, err)
}

func TestMsgSender_RecvMsg(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	err := s.RecvMsg(nil)
	require.NoError(t, err)
}
