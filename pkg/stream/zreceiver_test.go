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
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewZStreamReceiver(t *testing.T) {
	r := bytes.NewBuffer([]byte{})
	vsr := NewZStreamReceiver(r, 4096)
	require.NotNil(t, vsr)
}

func TestNewZStreamReceiver_Next(t *testing.T) {
	atTx, err := NumberToBytes(uint64(67))
	if err != nil {
		t.Error(err)
	}
	score, err := NumberToBytes(float64(33.5))
	if err != nil {
		t.Error(err)
	}
	me := []*streamtest.MsgError{
		{M: []byte(`first`), E: io.EOF},
		{M: []byte(`second`), E: io.EOF},
		{M: score, E: io.EOF},
		{M: atTx, E: io.EOF},
	}
	r := streamtest.DefaultMsgReceiverMock(me)
	zsr := NewZStreamReceiver(r, 4096)
	set, key, s, tx, vr, err := zsr.Next()

	require.NoError(t, err)
	require.Equal(t, []byte(`first`), set)
	require.Equal(t, []byte(`second`), key)
	require.Equal(t, float64(33.5), s)
	require.NotNil(t, uint64(67), tx)
	require.NotNil(t, vr)
}
