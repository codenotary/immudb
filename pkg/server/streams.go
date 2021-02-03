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
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/proto"
	"io"
)

func (s *ImmuServer) GetStream(chunk *schema.Chunk, str schema.ImmuService_GetStreamServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "VerifiableGet")
	if err != nil {
		return err
	}

	kvsr := stream.NewKvStreamSender(stream.NewMsgSender(str))

	kr := &schema.KeyRequest{}
	err = proto.Unmarshal(chunk.Content, kr)
	if err != nil {
		return err
	}

	entry, err := s.dbList.GetByIndex(ind).Get(kr)
	if err != nil {
		return err
	}
	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(entry.Key)),
			Size:    len(entry.Key),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(entry.Value)),
			Size:    len(entry.Value),
		},
	}
	return kvsr.Send(kv)
}

func (s *ImmuServer) SetStream(str schema.ImmuService_SetStreamServer) (err error) {
	ind, err := s.getDbIndexFromCtx(str.Context(), "VerifiableGet")
	if err != nil {
		return err
	}

	kvsr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(str))
	done := false
	for !done {
		kv, err := kvsr.Recv()
		if err != nil {
			if err == io.EOF {
				done = true
				break
			}
			return err
		}
		key := make([]byte, kv.Key.Size)
		if _, err = kv.Key.Content.Read(key); err != nil {
			return err
		}

		value := make([]byte, kv.Value.Size)
		if _, err = kv.Value.Content.Read(value); err != nil {
			return err
		}

		txMeta, err := s.dbList.GetByIndex(ind).Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}})
		if err != nil {
			return err
		}
		txmb, err := proto.Marshal(txMeta)
		if err != nil {
			return err
		}
		err = str.SendAndClose(&schema.Chunk{Content: txmb})
		if err != nil {
			return err
		}
		/*f, err := os.Create(string(fn) + "_received")
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
		println(read)*/

	}
	return nil
}
