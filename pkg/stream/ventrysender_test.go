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

func TestNewVEntryStreamSender(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	ves := NewVEntryStreamSender(s)
	require.IsType(t, &vEntryStreamSender{}, ves)
}

func TestVEntryStreamSender_Send(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	kvss := NewVEntryStreamSender(s)
	kv := &VerifiableEntry{
		EntryWithoutValueProto: &ValueSize{
			Content: nil,
			Size:    0,
		},
		VerifiableTxProto: &ValueSize{
			Content: nil,
			Size:    0,
		},
		InclusionProofProto: &ValueSize{
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

func TestVEntryStreamSender_SendErr(t *testing.T) {
	sm := streamtest.DefaultImmuServiceSenderStreamMock()
	s := streamtest.DefaultMsgSenderMock(sm, 4096)
	s.SendF = func(reader io.Reader, payloadSize int, metadata map[string][]byte) (err error) {
		return errCustom
	}
	kvss := NewVEntryStreamSender(s)
	kv := &VerifiableEntry{
		EntryWithoutValueProto: &ValueSize{
			Content: nil,
			Size:    0,
		},
		VerifiableTxProto: &ValueSize{
			Content: nil,
			Size:    0,
		},
		InclusionProofProto: &ValueSize{
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
