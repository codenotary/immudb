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
		{chunk1, nil},
		{chunk2, nil},
		{nil, io.EOF},
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
		{chunk, nil},
		{nil, io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.NoError(t, err)
	require.Equal(t, 9, n)
}

func TestMsgReceiver_EmptyStream(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{nil, io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestMsgReceiver_ErrNotEnoughDataOnStream(t *testing.T) {

	content := []byte(`mycontent`)
	chunk := &schema.Chunk{Content: bytes.Join([][]byte{streamtest.GetTrailer(len(content) + 10), content}, nil)}

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{chunk, nil},
		{nil, io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.Equal(t, ErrNotEnoughDataOnStream, err)
}

func TestMsgReceiver_StreamRecvError(t *testing.T) {

	sm := streamtest.DefaultImmuServiceReceiverStreamMock([]*streamtest.ChunkError{
		{nil, errors.New("NewError!")},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.Equal(t, 0, n)
	require.Error(t, err)
}
