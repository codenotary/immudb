package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"os"
)

func (s *ImmuServer) Stream(stream schema.ImmuService_StreamServer) (err error) {
	mr := NewMsgReceiver(stream, s.Logger)
	done := false
	var messages = make([][]byte, 2)
	count := 0
	for !done {
		count++
		msg, err := mr.rec()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}
			return err
		}
		messages[count-1] = msg
		if count%2 == 0 {
			fn := string(messages[0]) + "_received"
			f, err := os.Create(fn)
			defer f.Close()
			if err != nil {
				return status.Error(codes.Unknown, err.Error())
			}
			_, err = f.Write(messages[1])
			if err != nil {
				return status.Error(codes.Unknown, err.Error())
			}
			count = 0
		}
		err = stream.Send(&schema.Chunk{Content: []byte(fmt.Sprintf("%s OK", messages[0]))})
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
	}
	return nil
}

func NewMsgReceiver(stream schema.ImmuService_StreamServer, l logger.Logger) *msgReceiver {
	return &msgReceiver{stream: stream, l: l, b: bytes.NewBuffer([]byte{})}
}

type msgReceiver struct {
	stream schema.ImmuService_StreamServer
	l      logger.Logger
	b      *bytes.Buffer
}

func (r *msgReceiver) rec() ([]byte, error) {
	var l uint64 = 0
	var message []byte
	for {
		chunk, err := r.stream.Recv()
		if err != nil {
			return nil, err
		}
		r.b.Write(chunk.Content)
		// if l is zero need to read the trailer to get the message length
		if l == 0 {
			trailer := make([]byte, 8)
			_, err = r.b.Read(trailer)
			if err != nil {
				return nil, err
			}
			l = binary.BigEndian.Uint64(trailer)
			message = make([]byte, l)
		}
		// if message is contained in the first chunk is returned
		if r.b.Len() >= int(l) {
			_, err = r.b.Read(message)
			if err != nil {
				return nil, err
			}
			return message, nil
		}
	}
}
