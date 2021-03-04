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

package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/stream"
)

func (c *immuClient) streamSet(ctx context.Context) (schema.ImmuService_StreamSetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamSet(ctx)
}

func (c *immuClient) streamGet(ctx context.Context, in *schema.KeyRequest) (schema.ImmuService_StreamGetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamGet(ctx, in)
}

func (c *immuClient) streamVerifiableSet(ctx context.Context) (schema.ImmuService_StreamVerifiableSetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamVerifiableSet(ctx)
}

func (c *immuClient) streamVerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest) (schema.ImmuService_StreamVerifiableGetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamVerifiableGet(ctx, in)
}

func (c *immuClient) streamScan(ctx context.Context, in *schema.ScanRequest) (schema.ImmuService_StreamScanClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamScan(ctx, in)
}

func (c *immuClient) streamZScan(ctx context.Context, in *schema.ZScanRequest) (schema.ImmuService_StreamZScanClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamZScan(ctx, in)
}

func (c *immuClient) streamHistory(ctx context.Context, in *schema.HistoryRequest) (schema.ImmuService_StreamHistoryClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamHistory(ctx, in)
}

// StreamSet set an array of *stream.KeyValue in immudb streaming contents on a fixed size channel
func (c *immuClient) StreamSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	s, err := c.streamSet(ctx)
	if err != nil {
		return nil, err
	}

	kvss := stream.NewKvStreamSender(stream.NewMsgSender(s, c.Options.StreamChunkSize))

	for _, kv := range kvs {
		err = kvss.Send(kv)
		if err != nil {
			return nil, err
		}
	}

	return s.CloseAndRecv()
}

// StreamGet get an *schema.Entry from immudb with a stream
func (c *immuClient) StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	gs, err := c.streamGet(ctx, k)

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs), c.Options.StreamChunkSize)

	key, vr, err := kvr.Next()
	if err != nil {
		return nil, err
	}

	return stream.ParseKV(key, vr, c.Options.StreamChunkSize)
}

func (c *immuClient) StreamVerifiedSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxMetadata, error) {
	if len(kvs) == 0 {
		return nil, errors.New("no key-values specified")
	}

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	start := time.Now()
	defer c.Logger.Debugf("StreamVerifiedSet finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}
	stateTxID, err := stream.NumberToBytes(state.TxId)
	if err != nil {
		return nil, err
	}

	s, err := c.streamVerifiableSet(ctx)
	if err != nil {
		return nil, err
	}

	kvss := stream.NewKvStreamSender(stream.NewMsgSender(s, c.Options.StreamChunkSize))

	// 1st send the ProveSinceTx (build a "fake" KV with it):
	err = kvss.Send(&stream.KeyValue{
		// this is a fake key, server will ignore it and use only the value
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(stream.ProveSinceTxFakeKey)),
			Size:    len(stream.ProveSinceTxFakeKey),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(stateTxID)),
			Size:    len(stateTxID),
		},
	})
	if err != nil {
		return nil, err
	}

	for _, kv := range kvs {
		err = kvss.Send(kv)
		if err != nil {
			return nil, err
		}
	}

	verifiableTx, err := s.CloseAndRecv()

	if verifiableTx.Tx.Metadata.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFrom(verifiableTx.Tx)

	// TODO OGG: check with @Michele to perform inclusion proof,
	//					 key and value (i.e. the last ones from the input list of KVs?)
	//					 need to be read => if value is huge, this can be problematic
	//-->
	verifies := false

	// inclusionProof, err := tx.Proof(database.EncodeKey(key))
	// if err != nil {
	// 	return nil, err
	// }

	// verifies := store.VerifyInclusion(inclusionProof, database.EncodeKV(key, value), tx.Eh())
	// if !verifies {
	// 	return nil, store.ErrCorruptedData
	// }

	//<--

	if tx.Eh() != schema.DigestFrom(verifiableTx.DualProof.TargetTxMetadata.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFrom(state.TxHash)
	targetID = tx.ID
	targetAlh = tx.Alh

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFrom(verifiableTx.DualProof),
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)

		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Metadata, nil
}

func (c *immuClient) StreamVerifiedGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	gs, err := c.streamVerifiableGet(ctx, req)

	ver := stream.NewVEntryStreamReceiver(stream.NewMsgReceiver(gs), c.Options.StreamChunkSize)

	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := ver.Next()
	if err != nil {
		return nil, err
	}

	vEntry, err := stream.ParseVerifiableEntry(
		entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, c.Options.StreamChunkSize)
	if err != nil {
		return nil, err
	}

	inclusionProof := schema.InclusionProofFrom(vEntry.InclusionProof)
	dualProof := schema.DualProofFrom(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	var vTx uint64
	var kv *store.KV

	if vEntry.Entry.ReferencedBy == nil {
		vTx = vEntry.Entry.Tx
		kv = database.EncodeKV(req.KeyRequest.Key, vEntry.Entry.Value)
	} else {
		vTx = vEntry.Entry.ReferencedBy.Tx
		kv = database.EncodeReference(vEntry.Entry.ReferencedBy.Key, vEntry.Entry.Key, vEntry.Entry.ReferencedBy.AtTx)
	}

	if state.TxId <= vTx {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.TargetTxMetadata.EH)

		sourceID = state.TxId
		sourceAlh = schema.DigestFrom(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxMetadata.Alh()
	} else {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.SourceTxMetadata.EH)

		sourceID = vTx
		sourceAlh = dualProof.SourceTxMetadata.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFrom(state.TxHash)
	}

	verifies := store.VerifyInclusion(
		inclusionProof,
		kv,
		eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vEntry.Entry, nil
}

func (c *immuClient) StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	gs, err := c.streamScan(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.Ssf.NewKvStreamReceiver(gs)
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseKV(key, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}

func (c *immuClient) StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	gs, err := c.streamZScan(ctx, req)
	if err != nil {
		return nil, err
	}
	zr := c.Ssf.NewZStreamReceiver(gs)
	var entries []*schema.ZEntry
	for {
		set, key, score, vr, err := zr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseZEntry(set, key, score, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.ZEntries{Entries: entries}, nil
}

func (c *immuClient) StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	gs, err := c.streamHistory(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.Ssf.NewKvStreamReceiver(gs)
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseKV(key, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}
