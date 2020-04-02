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
	"errors"
	"time"

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

func (c *ImmuClient) Get(ctx context.Context, key []byte) (*schema.Item, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Get(ctx, &schema.Key{Key: key})
	c.Logger.Debugf("get finished in %s", time.Since(start))
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
		err
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

// Example on how to load keys at caller site (in immu CLI):
// keys := [][]byte{}
// for _, key := range keyReaders {
// 	key, err := ioutil.ReadAll(keyReader)
// 	if err != nil {
// 		return nil, err
// 	}
// 	keys = append(keys, key)
// }
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

	// This guard ensures that result.Leaf is equal to the item's hash computed from
	// request values. From now on, result.Leaf can be trusted.
	if err == nil {
		item := schema.Item{
			Key:   key,
			Value: value,
			Index: result.Index,
		}
		if !bytes.Equal(item.Hash(), result.Leaf) {
			return nil, errors.New("proof does not match the given item")
		}
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
		err
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

func (c *ImmuClient) ByIndex(ctx context.Context, index uint64) (*schema.Item, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.ByIndex(ctx, &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("by-index finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) History(ctx context.Context, key []byte) (*schema.ItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.History(ctx, &schema.Key{
		Key: key,
	})
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

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	if err == nil {
		item := schema.Item{
			Key:   reference,
			Value: key,
			Index: result.Index,
		}
		if !bytes.Equal(item.Hash(), result.Leaf) {
			return nil, errors.New("proof does not match the given item")
		}
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
		err
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
	result, errza := c.serviceClient.SafeZAdd(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)

	key2, err := store.SetKey(key, set, score)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	if errza == nil {
		item := schema.Item{
			Key:   key2,
			Value: key,
			Index: result.Index,
		}
		if !bytes.Equal(item.Hash(), result.Leaf) {
			return nil, errors.New("proof does not match the given item")
		}
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
		err
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
