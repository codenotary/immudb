/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	"io"
	"time"

	"github.com/codenotary/immudb/pkg/client/errors"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/stream"
)

// StreamSet performs a write operation of a value for a single key retrieving key and value form io.Reader streams.
func (c *immuClient) StreamSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxHeader, error) {
	txhdr, err := c._streamSet(ctx, kvs)
	return txhdr, errors.FromError(err)
}

// StreamGet retrieves a single entry for a key read from an io.Reader stream.
func (c *immuClient) StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error) {
	entry, err := c._streamGet(ctx, k)
	return entry, errors.FromError(err)
}

// StreamVerifiedSet performs a write operation of a value for a single key retrieving key and value form io.Reader streams
// with additional verification of server-provided write proof.
func (c *immuClient) StreamVerifiedSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxHeader, error) {
	txhdr, err := c._streamVerifiedSet(ctx, kvs)
	return txhdr, errors.FromError(err)
}

// StreamVerifiedGet retrieves a single entry for a key read from an io.Reader stream
// with additional verification of server-provided value proof.
func (c *immuClient) StreamVerifiedGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.Entry, error) {
	entry, err := c._streamVerifiedGet(ctx, req)
	return entry, errors.FromError(err)
}

// StreamScan scans for keys with given prefix, using stream API to overcome limits of large keys and values.
func (c *immuClient) StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	entries, err := c._streamScan(ctx, req)
	return entries, errors.FromError(err)
}

// StreamZScan scans entries from given sorted set, using stream API to overcome limits of large keys and values.
func (c *immuClient) StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	entries, err := c._streamZScan(ctx, req)
	return entries, errors.FromError(err)
}

// StreamHistory returns a history of given key, using stream API to overcome limits of large keys and values.
func (c *immuClient) StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	entries, err := c._streamHistory(ctx, req)
	return entries, errors.FromError(err)
}

// StreamExecAll performs an ExecAll operation (write operation for multiple data types in a single transaction)
// using stream API to overcome limits of large keys and values.
func (c *immuClient) StreamExecAll(ctx context.Context, req *stream.ExecAllRequest) (*schema.TxHeader, error) {
	txhdr, err := c._streamExecAll(ctx, req)
	return txhdr, errors.FromError(err)
}

func (c *immuClient) _streamSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxHeader, error) {
	s, err := c.streamSet(ctx)
	if err != nil {
		return nil, err
	}

	kvss := c.StreamServiceFactory.NewKvStreamSender(c.StreamServiceFactory.NewMsgSender(s))

	for _, kv := range kvs {
		err = kvss.Send(kv)
		if err != nil {
			return nil, err
		}
	}

	return s.CloseAndRecv()
}

func (c *immuClient) _streamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error) {
	gs, err := c.streamGet(ctx, k)
	if err != nil {
		return nil, err
	}

	kvr := c.StreamServiceFactory.NewKvStreamReceiver(c.StreamServiceFactory.NewMsgReceiver(gs))

	key, vr, err := kvr.Next()
	if err != nil {
		return nil, err
	}

	value, err := stream.ReadValue(vr, c.Options.StreamChunkSize)
	if err != nil {
		return nil, err
	}

	return &schema.Entry{
		Key:   key,
		Value: value,
	}, nil
}

func (c *immuClient) _streamVerifiedSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxHeader, error) {
	if len(kvs) == 0 {
		return nil, errors.New("no key-values specified")
	}

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	start := time.Now()
	defer c.debugElapsedTime("_streamVerifiedSet", start)

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}
	stateTxID, err := stream.NumberToBytes(state.TxId)
	if err != nil {
		return nil, err
	}

	//--> collect the keys and values as they need to be used for verifications
	stdKVs := make([]*schema.KeyValue, 0, len(kvs))
	for i, kv := range kvs {
		var keyBuffer bytes.Buffer
		keyTeeReader := io.TeeReader(kv.Key.Content, &keyBuffer)
		key := make([]byte, kv.Key.Size)
		if _, err := keyTeeReader.Read(key); err != nil {
			return nil, err
		}
		// put a new Reader back
		kvs[i].Key.Content = bufio.NewReader(&keyBuffer)

		var valueBuffer bytes.Buffer
		valueTeeReader := io.TeeReader(kv.Value.Content, &valueBuffer)
		value := make([]byte, kv.Value.Size)
		if _, err = valueTeeReader.Read(value); err != nil {
			return nil, err
		}
		// put a new Reader back
		kvs[i].Value.Content = bufio.NewReader(&valueBuffer)

		stdKVs = append(stdKVs, &schema.KeyValue{Key: key, Value: value})
	}
	//<--

	s, err := c.streamVerifiableSet(ctx)
	if err != nil {
		return nil, err
	}

	ss := c.StreamServiceFactory.NewMsgSender(s)
	kvss := c.StreamServiceFactory.NewKvStreamSender(ss)

	err = ss.Send(bytes.NewBuffer(stateTxID), len(stateTxID), nil)
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
	if err != nil {
		return nil, err
	}

	if verifiableTx.Tx.Header.Nentries != int32(len(kvs)) || len(verifiableTx.Tx.Entries) != len(kvs) {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFromProto(verifiableTx.Tx)

	entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
	if err != nil {
		return nil, err
	}

	var verifies bool

	for i, kv := range stdKVs {
		inclusionProof, err := tx.Proof(database.EncodeKey(kv.Key))
		if err != nil {
			return nil, err
		}

		md := tx.Entries()[i].Metadata()
		e := database.EncodeEntrySpec(kv.Key, md, kv.Value)

		verifies = store.VerifyInclusion(inclusionProof, entrySpecDigest(e), tx.Header().Eh)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	if tx.Header().Eh != schema.DigestFromProto(verifiableTx.DualProof.TargetTxHeader.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFromProto(state.TxHash)
	targetID = tx.Header().ID
	targetAlh = tx.Header().Alh()

	if state.TxId > 0 {
		dualProof := schema.DualProofFromProto(verifiableTx.DualProof)
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Header, nil
}

func (c *immuClient) _streamVerifiedGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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
	if err != nil {
		return nil, err
	}

	ver := c.StreamServiceFactory.NewVEntryStreamReceiver(c.StreamServiceFactory.NewMsgReceiver(gs))

	entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, err := ver.Next()
	if err != nil {
		return nil, err
	}

	vEntry, err := stream.ParseVerifiableEntry(
		entryWithoutValueProto, verifiableTxProto, inclusionProofProto, vr, c.Options.StreamChunkSize)
	if err != nil {
		return nil, err
	}

	entrySpecDigest, err := store.EntrySpecDigestFor(int(vEntry.VerifiableTx.Tx.Header.Version))
	if err != nil {
		return nil, err
	}

	inclusionProof := schema.InclusionProofFromProto(vEntry.InclusionProof)
	dualProof := schema.DualProofFromProto(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	var vTx uint64
	var e *store.EntrySpec

	if vEntry.Entry.ReferencedBy == nil {
		vTx = vEntry.Entry.Tx
		e = database.EncodeEntrySpec(req.KeyRequest.Key, schema.KVMetadataFromProto(vEntry.Entry.Metadata), vEntry.Entry.Value)
	} else {
		ref := vEntry.Entry.ReferencedBy
		vTx = ref.Tx
		e = database.EncodeReference(ref.Key, schema.KVMetadataFromProto(ref.Metadata), vEntry.Entry.Key, ref.AtTx)
	}

	if state.TxId <= vTx {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.TargetTxHeader.EH)
		sourceID = state.TxId
		sourceAlh = schema.DigestFromProto(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxHeader.Alh()
	} else {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.SourceTxHeader.EH)
		sourceID = vTx
		sourceAlh = dualProof.SourceTxHeader.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFromProto(state.TxHash)
	}

	verifies := store.VerifyInclusion(inclusionProof, entrySpecDigest(e), eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if state.TxId > 0 {
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vEntry.Entry, nil
}

func (c *immuClient) _streamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	gs, err := c.streamScan(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.StreamServiceFactory.NewKvStreamReceiver(c.StreamServiceFactory.NewMsgReceiver(gs))
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		value, err := stream.ReadValue(vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}

		entry := &schema.Entry{
			Key:   key,
			Value: value,
		}

		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}

func (c *immuClient) _streamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	gs, err := c.streamZScan(ctx, req)
	if err != nil {
		return nil, err
	}
	zr := c.StreamServiceFactory.NewZStreamReceiver(c.StreamServiceFactory.NewMsgReceiver(gs))
	var entries []*schema.ZEntry
	for {
		set, key, score, atTx, vr, err := zr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseZEntry(set, key, score, atTx, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.ZEntries{Entries: entries}, nil
}

func (c *immuClient) _streamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	gs, err := c.streamHistory(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.StreamServiceFactory.NewKvStreamReceiver(c.StreamServiceFactory.NewMsgReceiver(gs))
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		value, err := stream.ReadValue(vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}

		entry := &schema.Entry{
			Key:   key,
			Value: value,
		}
		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}

func (c *immuClient) _streamExecAll(ctx context.Context, req *stream.ExecAllRequest) (*schema.TxHeader, error) {
	s, err := c.streamExecAll(ctx)
	if err != nil {
		return nil, err
	}

	eas := c.StreamServiceFactory.NewExecAllStreamSender(c.StreamServiceFactory.NewMsgSender(s))

	err = eas.Send(req)
	if err != nil {
		return nil, err
	}

	return s.CloseAndRecv()
}

func (c *immuClient) streamSet(ctx context.Context) (schema.ImmuService_StreamSetClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamSet(ctx)
}

func (c *immuClient) streamGet(ctx context.Context, in *schema.KeyRequest) (schema.ImmuService_StreamGetClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamGet(ctx, in)
}

func (c *immuClient) streamVerifiableSet(ctx context.Context) (schema.ImmuService_StreamVerifiableSetClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamVerifiableSet(ctx)
}

func (c *immuClient) streamVerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest) (schema.ImmuService_StreamVerifiableGetClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamVerifiableGet(ctx, in)
}

func (c *immuClient) streamScan(ctx context.Context, in *schema.ScanRequest) (schema.ImmuService_StreamScanClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamScan(ctx, in)
}

func (c *immuClient) streamZScan(ctx context.Context, in *schema.ZScanRequest) (schema.ImmuService_StreamZScanClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamZScan(ctx, in)
}

func (c *immuClient) streamHistory(ctx context.Context, in *schema.HistoryRequest) (schema.ImmuService_StreamHistoryClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamHistory(ctx, in)
}

func (c *immuClient) streamExecAll(ctx context.Context) (schema.ImmuService_StreamExecAllClient, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.StreamExecAll(ctx)
}
