/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
)

func TestNewKvStreamSender(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := NewMsgSender(sm, make([]byte, 4096))
	kvss := NewKvStreamSender(s)
	require.IsType(t, &kvStreamSender{}, kvss)
}

func TestKvStreamSender_Send(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	kvss := NewKvStreamSender(s)
	kv := &KeyValue{
		Key: &ValueSize{
			Content: nil,
			Size:    0,
		},
		Value: &ValueSize{
			Content: nil,
			Size:    0,
		},
	}

	err := kvss.Send(kv)

	require.NoError(t, err)
}

func TestKvStreamSender_SendEOF(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()

	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return io.EOF
	}
	s.RecvMsgF = func(m interface{}) error {
		return io.EOF
	}
	kvss := NewKvStreamSender(s)
	kv := &KeyValue{
		Key: &ValueSize{
			Content: nil,
			Size:    0,
		},
		Value: &ValueSize{
			Content: nil,
			Size:    0,
		},
	}

	err := kvss.Send(kv)
	require.ErrorIs(t, err, io.EOF)
}

func TestKvStreamSender_SendErr(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()

	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return errCustom
	}

	kvss := NewKvStreamSender(s)
	kv := &KeyValue{
		Key: &ValueSize{
			Content: nil,
			Size:    0,
		},
		Value: &ValueSize{
			Content: nil,
			Size:    0,
		},
	}

	err := kvss.Send(kv)

	require.ErrorIs(t, err, errCustom)
}
