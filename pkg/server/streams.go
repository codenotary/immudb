/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"io"

	"github.com/codenotary/immudb/pkg/errors"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// StreamGet return a stream of key-values to the client
func (s *ImmuServer) StreamGet(kr *schema.KeyRequest, str schema.ImmuService_StreamGetServer) error {
	db, err := s.getDBFromCtx(str.Context(), "StreamGet")
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamSender(s.StreamServiceFactory.NewMsgSender(str))

	entry, err := db.Get(str.Context(), kr)
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
	if s.Options.GetMaintenance() {
		return ErrNotAllowedInMaintenanceMode
	}

	db, err := s.getDBFromCtx(str.Context(), "StreamSet")
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamReceiver(s.StreamServiceFactory.NewMsgReceiver(str))

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
		if err != nil {
			if err == io.EOF {
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break
			}
		}

		vlength += len(value)
		if vlength > stream.MaxTxValueLen {
			return errors.New(stream.ErrMaxTxValuesLenExceeded)
		}

		kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
	}

	txhdr, err := db.Set(str.Context(), &schema.SetRequest{KVs: kvs})
	if err == store.ErrMaxValueLenExceeded {
		return errors.Wrap(err, stream.ErrMaxValueLenExceeded)
	}
	if err != nil {
		return status.Errorf(codes.Unknown, "StreamSet receives following error: %s", err.Error())
	}

	err = str.SendAndClose(txhdr)
	if err != nil {
		return err
	}

	return nil
}

// StreamVerifiableGet ...
func (s *ImmuServer) StreamVerifiableGet(req *schema.VerifiableGetRequest, str schema.ImmuService_StreamVerifiableGetServer) error {
	db, err := s.getDBFromCtx(str.Context(), "StreamVerifiableGet")
	if err != nil {
		return err
	}

	vess := s.StreamServiceFactory.NewVEntryStreamSender(s.StreamServiceFactory.NewMsgSender(str))

	vEntry, err := db.VerifiableGet(str.Context(), req)
	if err != nil {
		return err
	}

	if s.StateSigner != nil {
		hdr := schema.TxHeaderFromProto(vEntry.VerifiableTx.DualProof.TargetTxHeader)
		alh := hdr.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetName(),
			TxId:   hdr.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return err
		}

		vEntry.VerifiableTx.Signature = newState.Signature
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
	if s.Options.GetMaintenance() {
		return ErrNotAllowedInMaintenanceMode
	}

	db, err := s.getDBFromCtx(str.Context(), "StreamVerifiableSet")
	if err != nil {
		return err
	}

	sr := s.StreamServiceFactory.NewMsgReceiver(str)
	kvsr := s.StreamServiceFactory.NewKvStreamReceiver(sr)

	vlength := 0

	proveSinceTxBs, err := stream.ReadValue(sr, s.Options.StreamChunkSize)
	if err != nil {
		return err
	}

	var proveSinceTx uint64
	if err := stream.NumberFromBytes(proveSinceTxBs, &proveSinceTx); err != nil {
		return err
	}

	vlength += len(proveSinceTxBs)
	if vlength > stream.MaxTxValueLen {
		return errors.New(stream.ErrMaxTxValuesLenExceeded).WithCode(errors.CodDataException)
	}

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
		if err != nil {
			if err == io.EOF {
				kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
				break
			}
		}

		vlength += len(value)
		if vlength > stream.MaxTxValueLen {
			return errors.New(stream.ErrMaxTxValuesLenExceeded).WithCode(errors.CodDataException)
		}

		kvs = append(kvs, &schema.KeyValue{Key: key, Value: value})
	}

	vSetReq := schema.VerifiableSetRequest{
		SetRequest:   &schema.SetRequest{KVs: kvs},
		ProveSinceTx: proveSinceTx,
	}
	verifiableTx, err := db.VerifiableSet(str.Context(), &vSetReq)
	if err == store.ErrMaxValueLenExceeded {
		return errors.Wrap(err, stream.ErrMaxValueLenExceeded).WithCode(errors.CodDataException)
	}
	if err != nil {
		return status.Errorf(codes.Unknown, "StreamVerifiableSet received the following error: %s", err.Error())
	}

	if s.StateSigner != nil {
		hdr := schema.TxHeaderFromProto(verifiableTx.DualProof.TargetTxHeader)
		alh := hdr.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetName(),
			TxId:   hdr.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return err
		}

		verifiableTx.Signature = newState.Signature
	}

	err = str.SendAndClose(verifiableTx)
	if err != nil {
		return err
	}

	return nil
}

func (s *ImmuServer) StreamScan(req *schema.ScanRequest, str schema.ImmuService_StreamScanServer) error {
	db, err := s.getDBFromCtx(str.Context(), "Scan")
	if err != nil {
		return err
	}

	r, err := db.Scan(str.Context(), req)
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamSender(s.StreamServiceFactory.NewMsgSender(str))

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
	db, err := s.getDBFromCtx(server.Context(), "ZScan")
	if err != nil {
		return err
	}

	r, err := db.ZScan(server.Context(), request)
	if err != nil {
		return err
	}

	zss := s.StreamServiceFactory.NewZStreamSender(s.StreamServiceFactory.NewMsgSender(server))

	for _, e := range r.Entries {
		scoreBs, err := stream.NumberToBytes(e.Score)
		if err != nil {
			s.Logger.Errorf("StreamZScan error: could not convert score %f to bytes: %v", e.Score, err)
		}

		atTxBs, err := stream.NumberToBytes(e.AtTx)
		if err != nil {
			s.Logger.Errorf("StreamZScan error: could not convert atTx %d to bytes: %v", e.AtTx, err)
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
			AtTx: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(atTxBs)),
				Size:    len(atTxBs),
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
	db, err := s.getDBFromCtx(server.Context(), "History")
	if err != nil {
		return err
	}

	r, err := db.History(server.Context(), request)
	if err != nil {
		return err
	}

	kvsr := s.StreamServiceFactory.NewKvStreamSender(s.StreamServiceFactory.NewMsgSender(server))

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

func (s *ImmuServer) StreamExecAll(str schema.ImmuService_StreamExecAllServer) error {
	if s.Options.GetMaintenance() {
		return ErrNotAllowedInMaintenanceMode
	}

	db, err := s.getDBFromCtx(str.Context(), "StreamSet")
	if err != nil {
		return err
	}

	sops := []*schema.Op{}
	eas := s.StreamServiceFactory.NewExecAllStreamReceiver(s.StreamServiceFactory.NewMsgReceiver(str))
	for {
		op, err := eas.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch x := op.(type) {
		case *stream.Op_KeyValue:
			key, err := stream.ReadValue(x.KeyValue.Key.Content, s.Options.StreamChunkSize)
			if err != nil {
				return err
			}

			value, err := stream.ReadValue(x.KeyValue.Value.Content, s.Options.StreamChunkSize)
			if err != nil {
				return err
			}

			sop := &schema.Op{Operation: &schema.Op_Kv{
				Kv: &schema.KeyValue{
					Key:   key,
					Value: value,
				},
			},
			}
			sops = append(sops, sop)
		case *stream.Op_ZAdd:
			sop := &schema.Op{Operation: &schema.Op_ZAdd{
				ZAdd: x.ZAdd,
			},
			}
			sops = append(sops, sop)
		}
	}

	txhdr, err := db.ExecAll(str.Context(), &schema.ExecAllRequest{Operations: sops})
	if err != nil {
		return err
	}

	err = str.SendAndClose(txhdr)
	if err != nil {
		return err
	}

	return nil
}
