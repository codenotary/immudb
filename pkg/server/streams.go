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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

func (s *ImmuServer) GetStream(kr *schema.KeyRequest, str schema.ImmuService_GetStreamServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "VerifiableGet")
	if err != nil {
		return err
	}

	kvsr := stream.NewKvStreamSender(stream.NewMsgSender(str))

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
	ind, err := s.getDbIndexFromCtx(str.Context(), "SetStream")
	if err != nil {
		return err
	}

	kvsr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(str))

	key, err := kvsr.NextKey()
	if err != nil {
		return err
	}

	vr, err := kvsr.NextValueReader()
	if err != nil {
		return err
	}
	b := bytes.NewBuffer([]byte{})
	vl := 0
	chunk := make([]byte, stream.ChunkSize)
	for {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			return err
		}
		vl += l

		b.Write(chunk)
		if err == io.EOF || l == 0 {
			break
		}
	}
	value := make([]byte, vl)
	b.Read(value)

	txMeta, err := s.dbList.GetByIndex(ind).Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}})
	if err != nil {
		return status.Errorf(codes.Unknown, "SetStream receives following error: %s", err.Error())
	}
	err = str.SendAndClose(txMeta)
	if err != nil {
		return err
	}

	return nil
}
