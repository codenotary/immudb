package stream_test

import (
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
)

type ChunkError struct {
	C *schema.Chunk
	E error
}
type ImmuServiceReceiver_StreamMock struct {
	cc    int
	ce    []*ChunkError
	RecvF func() (*schema.Chunk, error)
}

func (ism *ImmuServiceReceiver_StreamMock) Recv() (*schema.Chunk, error) {
	return ism.RecvF()
}

func DefaultImmuServiceReceiverStreamMock(ce []*ChunkError) *ImmuServiceReceiver_StreamMock {
	m := &ImmuServiceReceiver_StreamMock{
		ce: ce,
	}
	f := func() (*schema.Chunk, error) {
		if len(m.ce) > 0 {
			c := m.ce[m.cc].C
			e := m.ce[m.cc].E
			m.cc++
			return c, e
		}
		return nil, nil
	}
	m.RecvF = f
	return m
}

func GetTrailer(payloadSize int) []byte {
	ml := make([]byte, 8)
	binary.BigEndian.PutUint64(ml, uint64(payloadSize))
	return ml
}
