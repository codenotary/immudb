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
	"errors"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
	s.SendF = func(reader io.Reader, payloadSize int) (err error) {
		return errors.New("custom")
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

	require.Error(t, err)
}
