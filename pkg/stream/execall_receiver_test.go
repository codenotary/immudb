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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewExecAllStreamReceiver(t *testing.T) {
	r := bytes.NewBuffer([]byte{})
	esr := NewExecAllStreamReceiver(r, 4096)
	require.IsType(t, new(execAllStreamReceiver), esr)
}

func TestExecAllStreamReceiver_Next(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Kv}, E: io.EOF},
		{M: []byte{1, 1, 1}, E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.NoError(t, err)
	require.NotNil(t, op)
}

func TestExecAllStreamReceiver_NextZAdd(t *testing.T) {
	zadd := &schema.ZAddRequest{}
	zaddb, _ := proto.Marshal(zadd)
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Kv}, E: io.EOF},
		{M: []byte{1, 1, 1}, E: io.EOF},
		{M: []byte{TOp_ZAdd}, E: io.EOF},
		{M: zaddb, E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.NoError(t, err)
	require.NotNil(t, op)
	op, err = esr.Next()
	require.NoError(t, err)
	require.NotNil(t, op)
}

func TestExecAllStreamReceiver_NextZAddUnmarshalError(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Kv}, E: io.EOF},
		{M: []byte{1, 1, 1}, E: io.EOF},
		{M: []byte{TOp_ZAdd}, E: io.EOF},
		{M: []byte{1, 1, 1}, E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.NoError(t, err)
	require.NotNil(t, op)
	op, err = esr.Next()
	require.Equal(t, ErrUnableToReassembleExecAllMessage, err.Error())
	require.Nil(t, op)
}

func TestExecAllStreamReceiver_NextRefError(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Ref}, E: io.EOF},
		{M: []byte{1, 1, 1}, E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.Equal(t, ErrRefOptNotImplemented, err.Error())
	require.Nil(t, op)
}

func TestExecAllStreamReceiver_NextKvStreamerError(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Kv}, E: errors.New("custom one")},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.Error(t, err, err)
	require.Nil(t, op)
}

func TestExecAllStreamReceiver_NextKvStreamerNextError(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte{TOp_Kv}, E: io.EOF},
		{M: []byte{4}, E: errors.New("custom one")},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	esr := NewExecAllStreamReceiver(r, 4096)
	op, err := esr.Next()
	require.Error(t, err, err)
	require.Nil(t, op)
}
