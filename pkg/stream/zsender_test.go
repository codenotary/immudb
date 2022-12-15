package stream

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type msgSenderMock struct {
	SendF    func(io.Reader, int) error
	RecvMsgF func(interface{}) error
}

func (msm *msgSenderMock) Send(reader io.Reader, payloadSize int) error {
	return msm.SendF(reader, payloadSize)
}
func (msm *msgSenderMock) RecvMsg(m interface{}) error {
	return msm.RecvMsgF(m)
}

func TestZSender(t *testing.T) {
	errReceiveMsg := errors.New("receive msg error")
	// EOF error
	msm := msgSenderMock{
		SendF:    func(io.Reader, int) error { return io.EOF },
		RecvMsgF: func(interface{}) error { return errReceiveMsg },
	}
	zss := NewZStreamSender(&msm)

	set := []byte("SomeSet")
	key := []byte("SomeKey")
	var score float64 = 11
	scoreBs, err := NumberToBytes(score)
	require.NoError(t, err)
	var atTx uint64 = 22
	atTxBs, err := NumberToBytes(atTx)
	require.NoError(t, err)
	value := []byte("SomeValue")

	zEntry := ZEntry{
		Set:   &ValueSize{Content: bytes.NewReader(set), Size: len(set)},
		Key:   &ValueSize{Content: bytes.NewReader(key), Size: len(key)},
		Score: &ValueSize{Content: bytes.NewReader(scoreBs), Size: len(scoreBs)},
		AtTx:  &ValueSize{Content: bytes.NewReader(atTxBs), Size: len(atTxBs)},
		Value: &ValueSize{Content: bytes.NewReader(value), Size: len(value)},
	}

	err = zss.Send(&zEntry)
	require.ErrorIs(t, err, errReceiveMsg)

	errSend := errors.New("send error")
	// other error
	msm.SendF = func(io.Reader, int) error { return errSend }
	msm.RecvMsgF = func(interface{}) error { return nil }
	zss = NewZStreamSender(&msm)
	err = zss.Send(&zEntry)
	require.ErrorIs(t, err, errSend)

	// no error
	msm.SendF = func(io.Reader, int) error { return nil }
	zss = NewZStreamSender(&msm)
	err = zss.Send(&zEntry)
	require.NoError(t, err)
}
