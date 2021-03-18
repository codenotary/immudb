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
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewVEntryStreamReceiver(t *testing.T) {
	r := bytes.NewBuffer([]byte{})
	vsr := NewVEntryStreamReceiver(r, 4096)
	require.NotNil(t, vsr)
}

func TestVEntryStreamReceiver_Next(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte(`first`), E: io.EOF},
		{M: []byte(`second`), E: io.EOF},
		{M: []byte(`third`), E: io.EOF},
		{M: []byte(`fourth`), E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	vsr := NewVEntryStreamReceiver(r, 4096)
	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := vsr.Next()
	require.NoError(t, err)
	require.Equal(t, []byte(`first`), entryWithoutValueProto)
	require.Equal(t, []byte(`second`), verifiableTxProto)
	require.Equal(t, []byte(`third`), inclusionProofProto)
	require.NotNil(t, vr)
}

func TestVEntryStreamReceiver_NextErr0(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte(`first`), E: errors.New("custom")},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	vsr := NewVEntryStreamReceiver(r, 4096)
	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := vsr.Next()
	require.Error(t, err)
	require.Nil(t, entryWithoutValueProto)
	require.Nil(t, verifiableTxProto)
	require.Nil(t, inclusionProofProto)
	require.Nil(t, vr)
}

func TestVEntryStreamReceiver_NextErr1(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte(`first`), E: io.EOF},
		{M: []byte(`second`), E: errors.New("custom")},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	vsr := NewVEntryStreamReceiver(r, 4096)
	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := vsr.Next()
	require.Error(t, err)
	require.Nil(t, entryWithoutValueProto)
	require.Nil(t, verifiableTxProto)
	require.Nil(t, inclusionProofProto)
	require.Nil(t, vr)
}

func TestVEntryStreamReceiver_NextErr2(t *testing.T) {
	me := []*streamtest.MsgError{
		{M: []byte(`first`), E: io.EOF},
		{M: []byte(`second`), E: io.EOF},
		{M: []byte(`third`), E: errors.New("custom")},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	vsr := NewVEntryStreamReceiver(r, 4096)
	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := vsr.Next()
	require.Error(t, err)
	require.Nil(t, entryWithoutValueProto)
	require.Nil(t, verifiableTxProto)
	require.Nil(t, inclusionProofProto)
	require.Nil(t, vr)
}
