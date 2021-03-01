package stream

import (
	"bytes"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream/stream_test"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestMsgReceiver_Read(t *testing.T) {
	content := []byte(`mycontent`)
	chunk := &schema.Chunk{Content: bytes.Join([][]byte{stream_test.GetTrailer(len(content)), content}, nil)}

	sm := stream_test.DefaultImmuServiceReceiverStreamMock([]*stream_test.ChunkError{
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

	sm := stream_test.DefaultImmuServiceReceiverStreamMock([]*stream_test.ChunkError{
		{nil, io.EOF},
	})

	mr := NewMsgReceiver(sm)

	message := make([]byte, 4096)

	n, err := mr.Read(message)

	require.NoError(t, err)
	require.Equal(t, 9, n)
}
