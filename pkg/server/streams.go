/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

func (s *ImmuServer) StreamGet(kr *schema.KeyRequest, str schema.ImmuService_StreamGetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamGet")
	if err != nil {
		return err
	}

	kvsr := s.Ssf.NewKvStreamSender(str)

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

func (s *ImmuServer) StreamSet(str schema.ImmuService_StreamSetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamSet")
	if err != nil {
		return err
	}

	kvsr := s.Ssf.NewKvStreamReceiver(str)

	var kvs = make([]*schema.KeyValue, 0)

out:
	for {
		key, vr, err := kvsr.Next()
		if err != nil {
			if err == io.EOF {
				break out
			}
			return err
		}

		b := new(bytes.Buffer)
		vl := 0
		chunk := make([]byte, stream.DefaultChunkSize)
	inner:
		for {
			l, err := vr.Read(chunk)
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				value := make([]byte, vl)
				_, err = b.Read(value)
				if err != nil {
					return err
				}
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break out
			}
			vl += l

			b.Write(chunk)
			if l == 0 {
				value := make([]byte, vl)
				_, err = b.Read(value)
				if err != nil {
					return err
				}
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break inner
			}
		}
	}

	txMeta, err := s.dbList.GetByIndex(ind).Set(&schema.SetRequest{KVs: kvs})
	if err == store.ErrorMaxValueLenExceeded {
		return stream.ErrMaxValueLenExceeded
	}
	if err != nil {
		return status.Errorf(codes.Unknown, "StreamSet receives following error: %s", err.Error())
	}
	err = str.SendAndClose(txMeta)
	if err != nil {
		return err
	}

	return nil
}

func (s *ImmuServer) StreamVerifiableGet(request *schema.VerifiableGetRequest, server schema.ImmuService_StreamVerifiableGetServer) error {
	panic("implement me")
}

func (s *ImmuServer) StreamVerifiableSet(request *schema.VerifiableSetRequest, server schema.ImmuService_StreamVerifiableSetServer) error {
	panic("implement me")
}

func (s *ImmuServer) StreamScan(req *schema.ScanRequest, str schema.ImmuService_StreamScanServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "Scan")
	if err != nil {
		return err
	}

	r, err := s.dbList.GetByIndex(ind).Scan(req)

	kvsr := s.Ssf.NewKvStreamSender(str)

	for _, e := range r.Entries {
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(e.Key)),
				Size:    len(e.Key),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(e.Value)),
				Size:    len(e.Value),
			},
		}
		err = kvsr.Send(kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ImmuServer) StreamZScan(request *schema.ZScanRequest, server schema.ImmuService_StreamZScanServer) error {
	panic("implement me")
}

func (s *ImmuServer) StreamHistory(request *schema.HistoryRequest, server schema.ImmuService_StreamHistoryServer) error {
	panic("implement me")
}
