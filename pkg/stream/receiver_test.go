/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
)

func TestMsgReceiver_Read(t *testing.T) {

	chunk_size := 5_000
	chunk := make([]byte, chunk_size)
	for i := 0; i < chunk_size-8; i++ {
		chunk[i] = byte(1)
	}
	chunk1 := &schema.Chunk{Content: bytes.Join([][]byte{streamtest.GetTrailer(len(chunk)), chunk}, nil)}

	chunk = make([]byte, 8)
	for i := 0; i < 8; i++ {
		chunk[i] = byte(1)
	}
	chunk2 := &schema.Chunk{Content: chunk}

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: chunk1, E: nil},
		{C: chunk2, E: nil},
		{C: nil, E: io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.NoError(t, err)
	require.Equal(t, 4096, n)

	n, err = mr.Read(message)

	require.NoError(t, err)
	require.Equal(t, 904, n)
}

func TestMsgReceiver_ReadMessInFirstChunk(t *testing.T) {
	content := []byte(`mycontent`)
	chunk := &schema.Chunk{Content: bytes.Join([][]byte{streamtest.GetTrailer(len(content)), content}, nil)}

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: chunk, E: nil},
		{C: nil, E: io.EOF},
	})

	mr := NewMsgReceiver(sm)
	message := make([]byte, 4096)

	n, err := mr.Read(message)
	require.NoError(t, err)
	require.Equal(t, 9, n)
}

func TestMsgReceiver_ReadFully_Edge_Cases(t *testing.T) {
	content := []byte(`mycontent`)
	firstChunk := &schema.Chunk{Content: bytes.Join([][]byte{streamtest.GetTrailer(len(content)*2 + 1), content}, nil)}
	secondChunk := &schema.Chunk{Content: content}

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: firstChunk, E: nil},
		{C: secondChunk, E: nil},
		{C: nil, E: io.EOF},
	})
	mr := NewMsgReceiver(sm)
	_, _, err := mr.ReadFully()
	require.ErrorIs(t, err, io.EOF)

	sm = streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: &schema.Chunk{Content: []byte{1}}, E: nil},
		{C: nil, E: io.EOF},
	})
	mr = NewMsgReceiver(sm)
	_, _, err = mr.ReadFully()
	require.ErrorContains(t, err, ErrChunkTooSmall)

	expectedErr := errors.New("unexpected error")

	sm = streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: nil, E: expectedErr},
	})

	mr = NewMsgReceiver(sm)

	_, _, err = mr.ReadFully()
	require.ErrorIs(t, err, expectedErr)

	sm = streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: firstChunk, E: nil},
		{C: nil, E: expectedErr},
	})

	mr = NewMsgReceiver(sm)
	_, _, err = mr.ReadFully()
	require.ErrorIs(t, err, expectedErr)
}

func TestMsgReceiver_EmptyStream(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: nil, E: io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestMsgReceiver_ErrNotEnoughDataOnStream(t *testing.T) {

	content := []byte(`mycontent`)
	chunk := &schema.Chunk{Content: bytes.Join([][]byte{streamtest.GetTrailer(len(content) + 10), content}, nil)}

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: chunk, E: nil},
		{C: nil, E: io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
}

func TestMsgReceiver_StreamRecvError(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{C: nil, E: errCustom},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.ErrorIs(t, err, errCustom)
}

func TestMsgReceiver_StreamMsgSent(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock(nil)

	mr := NewMsgReceiver(sm)
	mr.msgSend = true
	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.NoError(t, err)
}

func TestMsgReceiver_StreamEOF(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock(nil)

	mr := NewMsgReceiver(sm)
	mr.eof = true
	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
}
