/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestNewMsgSender(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)
	assert.IsType(t, new(msgSender), s)
}

func TestMsgSender_Send(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)

	content := []byte(`mycontent`)
	message := bytes.Join([][]byte{streamtest.GetTrailer(len(content)), content}, nil)
	b := bytes.NewBuffer(message)
	err := s.Send(b, b.Len())
	assert.NoError(t, err)
}

func TestMsgSender_SendPayloadSizeZero(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)
	b := bytes.NewBuffer(nil)
	err := s.Send(b, 0)
	assert.Equal(t, ErrMessageLengthIsZero, err.Error())
}

func TestMsgSender_SendErrReader(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)
	r := &streamtest.ErrReader{
		ReadF: func([]byte) (int, error) {
			return 0, errors.New("custom one")
		},
	}
	err := s.Send(r, 5000)
	assert.Error(t, err)
}

func TestMsgSender_SendEmptyReader(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)
	r := &streamtest.ErrReader{
		ReadF: func([]byte) (int, error) {
			return 0, io.EOF
		},
	}
	err := s.Send(r, 5000)
	assert.Equal(t, ErrReaderIsEmpty, err.Error())
}

func TestMsgSender_SendEErrNotEnoughDataOnStream(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)

	content := []byte(`mycontent`)
	message := streamtest.GetTrailer(len(content))
	b := bytes.NewBuffer(message)
	err := s.Send(b, 5000)
	assert.Equal(t, ErrNotEnoughDataOnStream, err.Error())
}

func TestMsgSender_SendLastChunk(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)

	content := []byte(`mycontent`)
	b := bytes.NewBuffer(content)
	err := s.Send(b, len(content))
	assert.NoError(t, err)
}

func TestMsgSender_SendMultipleChunks(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 8)

	content := []byte(`mycontent`)
	b := bytes.NewBuffer(content)
	err := s.Send(b, len(content))
	assert.NoError(t, err)
}

func TestMsgSender_RecvMsg(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, 4096)
	err := s.RecvMsg(nil)
	assert.NoError(t, err)
}
