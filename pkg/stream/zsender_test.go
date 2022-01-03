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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type msgSenderMock struct {
	SendF    func(io.Reader, int, map[string][]byte) error
	RecvMsgF func(interface{}) error
}

func (msm *msgSenderMock) Send(reader io.Reader, payloadSize int, metadata map[string][]byte) error {
	return msm.SendF(reader, payloadSize, metadata)
}
func (msm *msgSenderMock) RecvMsg(m interface{}) error {
	return msm.RecvMsgF(m)
}

func TestZSender(t *testing.T) {
	// EOF error
	msm := msgSenderMock{
		SendF:    func(io.Reader, int, map[string][]byte) error { return io.EOF },
		RecvMsgF: func(interface{}) error { return errors.New("receive msg error") },
	}
	zss := NewZStreamSender(&msm)

	set := []byte("SomeSet")
	key := []byte("SomeKey")
	var score float64 = 11
	scoreBs, err := NumberToBytes(score)
	require.NoError(t, err)
	var atTx uint64 = 22
	atTxBs, err := NumberToBytes(atTx)
	require.NoError(t, err)
	value := []byte("SomeValue")

	zEntry := ZEntry{
		Set:   &ValueSize{Content: bytes.NewReader(set), Size: len(set)},
		Key:   &ValueSize{Content: bytes.NewReader(key), Size: len(key)},
		Score: &ValueSize{Content: bytes.NewReader(scoreBs), Size: len(scoreBs)},
		AtTx:  &ValueSize{Content: bytes.NewReader(atTxBs), Size: len(atTxBs)},
		Value: &ValueSize{Content: bytes.NewReader(value), Size: len(value)},
	}

	err = zss.Send(&zEntry)
	require.Error(t, err)
	require.Equal(t, errors.New("receive msg error"), err)

	// other error
	msm.SendF = func(io.Reader, int, map[string][]byte) error { return errors.New("send error") }
	msm.RecvMsgF = func(interface{}) error { return nil }
	zss = NewZStreamSender(&msm)
	err = zss.Send(&zEntry)
	require.Error(t, err)
	require.Equal(t, errors.New("send error"), err)

	// no error
	msm.SendF = func(io.Reader, int, map[string][]byte) error { return nil }
	zss = NewZStreamSender(&msm)
	err = zss.Send(&zEntry)
	require.NoError(t, err)
}
