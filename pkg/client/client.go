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

package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/store"
)

func (c *ImmuClient) Connect(ctx context.Context) (err error) {
	start := time.Now()
	if c.isConnected() {
		return ErrAlreadyConnected
	}
	if err := c.connectWithRetry(ctx); err != nil {
		return err
	}
	if err := c.waitForHealthCheck(ctx); err != nil {
		return err
	}
	c.Logger.Debugf("connected %v in %s", c.Options, time.Since(start))
	return nil
}

func (c *ImmuClient) Disconnect() error {
	start := time.Now()
	if !c.isConnected() {
		return ErrNotConnected
	}
	if err := c.clientConn.Close(); err != nil {
		return err
	}
	c.serviceClient = nil
	c.clientConn = nil
	c.Logger.Debugf("disconnected %v in %s", c.Options, time.Since(start))
	return nil
}

func (c *ImmuClient) Connected(ctx context.Context, f func() (interface{}, error)) (interface{}, error) {
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	result, err := f()
	if err != nil {
		_ = c.Disconnect()
		return nil, err
	}
	if err := c.Disconnect(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *ImmuClient) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Login(ctx, &schema.LoginRequest{
		User:     user,
		Password: pass,
	})
	c.Logger.Debugf("set finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Get(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	item, err := c.serviceClient.Get(ctx, &schema.Key{Key: key})
	c.Logger.Debugf("get finished in %s", time.Since(start))
	result := item.ToSItem()
	return result, err
}

// VerifiedItem ...
type VerifiedItem struct {
	Key      []byte `json:"key"`
	Value    []byte `json:"value"`
	Index    uint64 `json:"index"`
	Verified bool   `json:"verified"`
}

// Reset ...
func (vi *VerifiedItem) Reset() { *vi = VerifiedItem{} }

func (vi *VerifiedItem) String() string { return proto.CompactTextString(vi) }

// ProtoMessage ...
func (*VerifiedItem) ProtoMessage() {}

// SafeGet ...
func (c *ImmuClient) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*VerifiedItem, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.isConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.rootservice.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	sgOpts := &schema.SafeGetOptions{
		Key: &schema.Key{
			Key: key,
		},
		RootIndex: &schema.Index{
			Index: root.Index,
		},
	}

	safeItem, err := c.serviceClient.SafeGet(ctx, sgOpts, opts...)
	if err != nil {
		return nil, err
	}

	verified := safeItem.Proof.Verify(safeItem.Item.Hash(), *root)
	if verified {
		// saving a fresh root
		tocache := new(schema.Root)
		tocache.Index = safeItem.Proof.At
		tocache.Root = safeItem.Proof.Root
		err := c.rootservice.SetRoot(tocache)
		if err != nil {
			return nil, err
		}
	}

	c.Logger.Debugf("safeget finished in %s", time.Since(start))

	return &VerifiedItem{
			Key:      safeItem.Item.GetKey(),
			Value:    safeItem.Item.GetValue(),
			Index:    safeItem.Item.GetIndex(),
			Verified: verified,
		},
		nil
}

func (c *ImmuClient) Scan(ctx context.Context, prefix []byte) (*schema.ItemList, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	return c.serviceClient.Scan(ctx, &schema.ScanOptions{Prefix: prefix})
}

func (c *ImmuClient) ZScan(ctx context.Context, set []byte) (*schema.ItemList, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	return c.serviceClient.ZScan(ctx, &schema.ZScanOptions{Set: set})
}

func (c *ImmuClient) Count(ctx context.Context, prefix []byte) (*schema.ItemsCount, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	return c.serviceClient.Count(ctx, &schema.KeyPrefix{Prefix: prefix})
}

func (c *ImmuClient) GetBatch(ctx context.Context, keys [][]byte) (*schema.ItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	keyList := &schema.KeyList{}
	for _, key := range keys {
		keyList.Keys = append(keyList.Keys, &schema.Key{Key: key})
	}
	result, err := c.serviceClient.GetBatch(ctx, keyList)
	c.Logger.Debugf("get-batch finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Set(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Set(ctx, &schema.KeyValue{
		Key:   key,
		Value: value,
	})
	c.Logger.Debugf("set finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) verifyAndSetRoot(
	result *schema.Proof,
	root *schema.Root) (bool, error) {

	verified := result.Verify(result.Leaf, *root)
	var err error
	if verified {
		//saving a fresh root
		tocache := new(schema.Root)
		tocache.Index = result.Index
		tocache.Root = result.Root
		err = c.rootservice.SetRoot(tocache)
	}
	return verified, err
}

// VerifiedIndex ...
type VerifiedIndex struct {
	Index    uint64 `json:"index"`
	Verified bool   `json:"verified"`
}

// Reset ...
func (vi *VerifiedIndex) Reset() { *vi = VerifiedIndex{} }

func (vi *VerifiedIndex) String() string { return proto.CompactTextString(vi) }

// ProtoMessage ...
func (*VerifiedIndex) ProtoMessage() {}

// SafeSet ...
func (c *ImmuClient) SafeSet(ctx context.Context, key []byte, value []byte) (*VerifiedIndex, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.isConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.rootservice.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeSetOptions{
		Kv: &schema.KeyValue{
			Key:   key,
			Value: value,
		},
		RootIndex: &schema.Index{
			Index: root.Index,
		},
	}

	var metadata runtime.ServerMetadata

	result, err := c.serviceClient.SafeSet(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed from
	// request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   key,
		Value: value,
		Index: result.Index,
	}
	if !bytes.Equal(item.Hash(), result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safeset finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

func (c *ImmuClient) SetBatch(ctx context.Context, request *BatchRequest) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	list, err := request.toKVList()
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.SetBatch(ctx, list)
	c.Logger.Debugf("set-batch finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Inclusion(ctx context.Context, index uint64) (*schema.InclusionProof, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Inclusion(ctx, &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("inclusion finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Consistency(ctx context.Context, index uint64) (*schema.ConsistencyProof, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Consistency(ctx, &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("consistency finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) ByIndex(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	item, err := c.serviceClient.ByIndex(ctx, &schema.Index{
		Index: index,
	})
	result := item.ToSItem()
	c.Logger.Debugf("by-index finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) History(ctx context.Context, key []byte) (*schema.StructuredItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	list, err := c.serviceClient.History(ctx, &schema.Key{
		Key: key,
	})
	result := list.ToSItemList()
	c.Logger.Debugf("history finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Reference(ctx context.Context, reference []byte, key []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Reference(ctx, &schema.ReferenceOptions{
		Reference: &schema.Key{Key: reference},
		Key:       &schema.Key{Key: key},
	})
	c.Logger.Debugf("reference finished in %s", time.Since(start))
	return result, err
}

// SafeReference ...
func (c *ImmuClient) SafeReference(ctx context.Context, reference []byte, key []byte) (*VerifiedIndex, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.isConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.rootservice.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: &schema.Key{Key: reference},
			Key:       &schema.Key{Key: key},
		},
		RootIndex: &schema.Index{
			Index: root.Index,
		},
	}

	var metadata runtime.ServerMetadata

	result, err := c.serviceClient.SafeReference(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   reference,
		Value: key,
		Index: result.Index,
	}
	if !bytes.Equal(item.Hash(), result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safereference finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

func (c *ImmuClient) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.ZAdd(ctx, &schema.ZAddOptions{
		Set:   set,
		Score: score,
		Key:   key,
	})
	c.Logger.Debugf("zadd finished in %s", time.Since(start))
	return result, err
}

// SafeZAdd ...
func (c *ImmuClient) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*VerifiedIndex, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.isConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.rootservice.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   set,
			Score: score,
			Key:   key,
		},
		RootIndex: &schema.Index{
			Index: root.Index,
		},
	}

	var metadata runtime.ServerMetadata
	result, err := c.serviceClient.SafeZAdd(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	key2, err := store.SetKey(key, set, score)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   key2,
		Value: key,
		Index: result.Index,
	}
	if !bytes.Equal(item.Hash(), result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safezadd finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

func writeSeek(w io.WriteSeeker, msg []byte, offset int64) (int64, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(msg)))
	if _, err := w.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}
	if _, err := w.Write(buf); err != nil {
		return offset, err
	}
	if _, err := w.Seek(offset+int64(len(buf)), io.SeekStart); err != nil {
		return offset, err
	}
	if _, err := w.Write(msg); err != nil {
		return offset, err
	}
	return offset + int64(len(buf)) + int64(len(msg)), nil
}

func readSeek(r io.ReadSeeker, offset int64) ([]byte, int64, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	buf := make([]byte, 4)
	o1 := offset + int64(len(buf))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, o1, err
	}
	if _, err := r.Seek(o1, io.SeekStart); err != nil {
		return nil, o1, err
	}
	size := binary.LittleEndian.Uint32(buf)
	msg := make([]byte, size)
	o2 := o1 + int64(size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, o2, err
	}
	return msg, o2, nil
}

// Backup to be used from Immu CLI
func (c *ImmuClient) Backup(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	start := time.Now()

	var counter int64

	if !c.isConnected() {
		return counter, ErrNotConnected
	}

	bkpClient, err := c.serviceClient.Backup(ctx, &empty.Empty{})
	if err != nil {
		return counter, err
	}
	defer bkpClient.CloseSend()

	var offset int64
	var errs []string
	for {
		kvList, err := bkpClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("error receiving chunk: %v", err))
			continue
		}
		for _, kv := range kvList.Kv {
			kvBytes, err := proto.Marshal(kv)
			if err != nil {
				errs = append(errs, fmt.Sprintf("error marshaling key-value %+v: %v", kv, err))
				continue
			}
			o, err := writeSeek(writer, kvBytes, offset)
			if err != nil {
				errs = append(errs, fmt.Sprintf("error writing as bytes key-value %+v: %v", kv, err))
				continue
			}
			offset = o
			counter++
		}
	}
	var errorsMerged error
	if len(errs) > 0 {
		errorsMerged = fmt.Errorf("Errors:\n\t%s", strings.Join(errs[:], "\n\t- "))
	}

	c.Logger.Debugf("backup finished in %s", time.Since(start))

	return counter, errorsMerged
}

func (c *ImmuClient) restoreChunk(ctx context.Context, kvList *pb.KVList) error {
	kvListLen := len(kvList.Kv)
	kvListStr := fmt.Sprintf("%+v", kvList)
	restoreClient, err := c.serviceClient.Restore(ctx)
	if err != nil {
		return fmt.Errorf("error sending to restore client a chunk of %d KVs in key-value list %s: error getting restore client: %v", kvListLen, kvListStr, err)
	}
	defer restoreClient.CloseAndRecv()
	err = restoreClient.Send(kvList)
	if err != nil {
		return fmt.Errorf("error sending to restore client a chunk of %d KVs in key-value list %s: %v", kvListLen, kvListStr, err)
	}
	return nil
}

// Restore to be used from Immu CLI
func (c *ImmuClient) Restore(ctx context.Context, reader io.ReadSeeker, chunkSize int) (int64, error) {
	start := time.Now()

	var entryCounter int64
	var counter int64

	if !c.isConnected() {
		return counter, ErrNotConnected
	}

	var errs []string
	var offset int64
	kvList := new(pb.KVList)
	for {
		lineBytes, o, err := readSeek(reader, offset)
		if err == io.EOF {
			break
		}
		entryCounter++
		offset = o
		if err != nil {
			errs = append(errs, fmt.Sprintf("error reading file entry %d: %v", entryCounter, err))
			continue
		}
		if len(lineBytes) <= 1 {
			continue
		}
		kv := new(pb.KV)
		err = proto.Unmarshal(lineBytes, kv)
		if err != nil {
			errs = append(errs, fmt.Sprintf("error unmarshaling to key-value the file entry %d: %v", entryCounter, err))
			continue
		}

		kvList.Kv = append(kvList.Kv, kv)
		if len(kvList.Kv) == chunkSize {
			if err := c.restoreChunk(ctx, kvList); err != nil {
				errs = append(errs, err.Error())
			} else {
				counter += int64(len(kvList.Kv))
			}
			kvList.Kv = []*pb.KV{}
		}
	}

	if len(kvList.Kv) > 0 {
		if err := c.restoreChunk(ctx, kvList); err != nil {
			errs = append(errs, err.Error())
		} else {
			counter += int64(len(kvList.Kv))
		}
		kvList.Kv = []*pb.KV{}
	}

	var errorsMerged error
	if len(errs) > 0 {
		errorsMerged = fmt.Errorf("Errors:\n\t%s", strings.Join(errs[:], "\n\t"))
	}

	c.Logger.Infof("restore finished restoring %d of %d entries in %s", counter, entryCounter, time.Since(start))

	return counter, errorsMerged
}

func (c *ImmuClient) HealthCheck(ctx context.Context) error {
	start := time.Now()
	if !c.isConnected() {
		return ErrNotConnected
	}
	response, err := c.serviceClient.Health(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	if !response.Status {
		return ErrHealthCheckFailed
	}
	c.Logger.Debugf("health-check finished in %s", time.Since(start))
	return nil
}

func (c *ImmuClient) isConnected() bool {
	return c.clientConn != nil && c.serviceClient != nil
}

func (c *ImmuClient) connectWithRetry(ctx context.Context) (err error) {
	for i := 0; i < c.Options.DialRetries+1; i++ {
		if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err == nil {
			c.serviceClient = schema.NewImmuServiceClient(c.clientConn)
			c.rootservice = NewRootService(c.serviceClient, cache.NewFileCache())
			if _, err = c.rootservice.GetRoot(ctx); err == nil {
				c.Logger.Debugf("dialed %v", c.Options)
				return nil
			}
		}
		c.Logger.Debugf("dial failed: %v", err)
		if c.Options.DialRetries > 0 {
			time.Sleep(time.Second)
		}
	}
	return err
}

func (c *ImmuClient) waitForHealthCheck(ctx context.Context) (err error) {
	for i := 0; i < c.Options.HealthCheckRetries+1; i++ {
		if err = c.HealthCheck(ctx); err == nil {
			c.Logger.Debugf("health check succeeded %v", c.Options)
			return nil
		}
		c.Logger.Debugf("health check failed: %v", err)
		if c.Options.HealthCheckRetries > 0 {
			time.Sleep(time.Second)
		}
	}
	return err
}
