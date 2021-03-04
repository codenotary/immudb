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
	"fmt"
	"io"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// StreamGet return a stream of key-values to the client
func (s *ImmuServer) StreamGet(kr *schema.KeyRequest, str schema.ImmuService_StreamGetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamGet")
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamSender(str)

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

// StreamSet set a stream of key-values in the internal store
func (s *ImmuServer) StreamSet(str schema.ImmuService_StreamSetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamSet")
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamReceiver(str)

	var kvs = make([]*schema.KeyValue, 0)

	vlength := 0
	for {
		key, vr, err := kvsr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		value, err := stream.ReadValue(vr, s.Options.StreamChunkSize)
		if value != nil {
			vlength += len(value)
			if vlength > stream.MaxTxValueLen {
				return stream.ErrMaxTxValuesLenExceeded
			}
		}
		if err != nil {
			if err == io.EOF {
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break
			}
		}
		kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})

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

// StreamVerifiableGet ...
func (s *ImmuServer) StreamVerifiableGet(req *schema.VerifiableGetRequest, str schema.ImmuService_StreamVerifiableGetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamVerifiableGet")
	if err != nil {
		return err
	}

	vess := s.StreamServiceFactory.NewVEntryStreamSender(str)

	vEntry, err := s.dbList.GetByIndex(ind).VerifiableGet(req)
	if err != nil {
		return err
	}

	value := stream.ValueSize{
		Content: bufio.NewReader(bytes.NewBuffer(vEntry.GetEntry().GetValue())),
		Size:    len(vEntry.GetEntry().GetValue()),
	}

	entryWithoutValue := schema.Entry{
		Tx:           vEntry.GetEntry().GetTx(),
		Key:          vEntry.GetEntry().GetKey(),
		ReferencedBy: vEntry.GetEntry().GetReferencedBy(),
	}

	entryWithoutValueProto, err := proto.Marshal(&entryWithoutValue)
	if err != nil {
		return err
	}

	verifiableTxProto, err := proto.Marshal(vEntry.GetVerifiableTx())
	if err != nil {
		return err
	}

	inclusionProofProto, err := proto.Marshal(vEntry.GetInclusionProof())
	if err != nil {
		return err
	}

	sVEntry := stream.VerifiableEntry{
		EntryWithoutValueProto: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(entryWithoutValueProto)),
			Size:    len(entryWithoutValueProto),
		},
		VerifiableTxProto: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(verifiableTxProto)),
			Size:    len(verifiableTxProto),
		},
		InclusionProofProto: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(inclusionProofProto)),
			Size:    len(inclusionProofProto),
		},
		Value: &value,
	}
	return vess.Send(&sVEntry)
}

// StreamVerifiableSet ...
func (s *ImmuServer) StreamVerifiableSet(str schema.ImmuService_StreamVerifiableSetServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "StreamVerifiableSet")
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamReceiver(str)

	vlength := 0

	//--> 1st message is special: it's a fake key which has the ProveSinceTx as value
	//		proveSinceTxFakeKey := []byte("ProveSinceTx")
	proveSinceTxKey, proveSinceTxVReader, err := kvsr.Next()
	if err != nil {
		return err
	}
	if string(proveSinceTxKey) != string(stream.ProveSinceTxFakeKey) {
		return fmt.Errorf("expected 1st key to be %s, got %s",
			stream.ProveSinceTxFakeKey, proveSinceTxKey)
	}
	proveSinceTxBs, err := stream.ReadValue(proveSinceTxVReader, s.Options.StreamChunkSize)
	if err != nil {
		return err
	}
	var proveSinceTx uint64
	if err := stream.NumberFromBytes(proveSinceTxBs, &proveSinceTx); err != nil {
		return err
	}
	vlength += len(proveSinceTxBs)
	if vlength > stream.MaxTxValueLen {
		return stream.ErrMaxTxValuesLenExceeded
	}
	//<--

	var kvs = make([]*schema.KeyValue, 0)

	for {
		key, vr, err := kvsr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		value, err := stream.ReadValue(vr, s.Options.StreamChunkSize)
		if value != nil {
			vlength += len(value)
			if vlength > stream.MaxTxValueLen {
				return stream.ErrMaxTxValuesLenExceeded
			}
		}
		if err != nil {
			if err == io.EOF {
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break
			}
		}
		kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})

	}

	vSetReq := schema.VerifiableSetRequest{
		SetRequest:   &schema.SetRequest{KVs: kvs},
		ProveSinceTx: proveSinceTx,
	}
	verifiableTx, err := s.dbList.GetByIndex(ind).VerifiableSet(&vSetReq)
	if err == store.ErrorMaxValueLenExceeded {
		return stream.ErrMaxValueLenExceeded
	}
	if err != nil {
		return status.Errorf(
			codes.Unknown,
			"StreamVerifiableSet received the following error: %s", err.Error())
	}
	err = str.SendAndClose(verifiableTx)
	if err != nil {
		return err
	}

	return nil
}

func (s *ImmuServer) StreamScan(req *schema.ScanRequest, str schema.ImmuService_StreamScanServer) error {
	ind, err := s.getDbIndexFromCtx(str.Context(), "Scan")
	if err != nil {
		return err
	}

	r, err := s.dbList.GetByIndex(ind).Scan(req)

	kvsr := s.StreamServiceFactory.NewKvStreamSender(str)

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

// StreamZScan ...
func (s *ImmuServer) StreamZScan(request *schema.ZScanRequest, server schema.ImmuService_StreamZScanServer) error {
	ind, err := s.getDbIndexFromCtx(server.Context(), "ZScan")
	if err != nil {
		return err
	}

	r, err := s.dbList.GetByIndex(ind).ZScan(request)

	zss := s.StreamServiceFactory.NewZStreamSender(server)

	for _, e := range r.Entries {
		scoreBs, err := stream.NumberToBytes(e.Score)
		if err != nil {
			s.Logger.Errorf(
				"StreamZScan error: could not convert score (float64) to bytes: %v", err)
		}
		ze := &stream.ZEntry{
			Set: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(e.Set)),
				Size:    len(e.Set),
			},
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(e.Key)),
				Size:    len(e.Key),
			},
			Score: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(scoreBs)),
				Size:    len(scoreBs),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(e.Entry.Value)),
				Size:    len(e.Entry.Value),
			},
		}
		err = zss.Send(ze)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ImmuServer) StreamHistory(request *schema.HistoryRequest, server schema.ImmuService_StreamHistoryServer) error {
	ind, err := s.getDbIndexFromCtx(server.Context(), "History")
	if err != nil {
		return err
	}

	r, err := s.dbList.GetByIndex(ind).History(request)

	kvsr := s.StreamServiceFactory.NewKvStreamSender(server)

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
