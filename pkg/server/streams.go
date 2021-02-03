/*
Copyright 2019-2020 vChain, Inc.

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

package server

import (
	"bufio"
	"bytes"
	"github.com/codenotary/immudb/pkg/api/schema"
	stream2 "github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"os"
)

func (s *ImmuServer) Stream(stream schema.ImmuService_StreamServer) (err error) {
	kvsr := NewKvStreamReveiver(stream2.NewMsgReceiver(stream))
	done := false
	for !done {
		kv, err := kvsr.Rec()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}
			return err
		}
		fn := make([]byte, kv.Key.Size)

		if _, err = kv.Key.Content.Read(fn); err != nil {
			return err
		}
		f, err := os.Create(string(fn) + "_received")
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		defer f.Close()
		writer := bufio.NewWriter(f)
		read, err := writer.ReadFrom(kv.Value.Content)
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		err = writer.Flush()
		if err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
		println(read)

	}
	return nil
}

type KvStreamReceiver interface {
	Rec() (*stream2.KeyValue, error)
}

type kvStreamReceiver struct {
	s stream2.MsgReceiver
}

func NewKvStreamReveiver(s stream2.MsgReceiver) *kvStreamReceiver {
	return &kvStreamReceiver{
		s: s,
	}
}

func (kvr *kvStreamReceiver) Rec() (*stream2.KeyValue, error) {
	kv := &stream2.KeyValue{}
	key, err := kvr.s.Recv()
	if err != nil {
		return nil, err
	}
	if err := kvr.s.Send([]byte(`ok`)); err != nil {
		return nil, err
	}
	bk := bytes.NewBuffer(key)
	rk := bufio.NewReader(bk)
	// todo @michele a reader and a len(key) should be returned by kvr.s.Recv()
	kv.Key = &stream2.ValueSize{
		Content: rk,
		Size:    len(key),
	}

	val, err := kvr.s.Recv()
	if err != nil {
		return nil, err
	}
	if err := kvr.s.Send([]byte(`ok`)); err != nil {
		return nil, err
	}
	bv := bytes.NewBuffer(val)
	rv := bufio.NewReader(bv)
	// todo @michele a reader and a len(val) should be returned by kvr.s.Recv()
	kv.Value = &stream2.ValueSize{
		Content: rv,
		Size:    len(val),
	}
	return kv, nil
}
