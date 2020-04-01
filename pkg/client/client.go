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
	"io"
	"io/ioutil"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/store"
)

func (c *ImmuClient) Connect() (err error) {
	start := time.Now()
	if c.isConnected() {
		return ErrAlreadyConnected
	}
	if err := c.connectWithRetry(); err != nil {
		return err
	}
	if err := c.waitForHealthCheck(); err != nil {
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

func (c *ImmuClient) Connected(f func() (interface{}, error)) (interface{}, error) {
	if err := c.Connect(); err != nil {
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

func (c *ImmuClient) Get(keyReader io.Reader) (*schema.Item, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.Get(context.Background(), &schema.Key{Key: key})
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

// SafeGet ...
func (c *ImmuClient) SafeGet(keyReader io.Reader) (*VerifiedItem, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	root, err := c.rootservice.GetRoot(context.Background())
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeGetOptions{
		Key: &schema.Key{
			Key: key,
		},
		RootIndex: &schema.Index{
			Index: root.Index,
		},
	}

	var metadata runtime.ServerMetadata
	safeItem, err := c.serviceClient.SafeGet(
		context.Background(),
		opts,
		grpc.Header(&metadata.HeaderMD),
		grpc.Trailer(&metadata.TrailerMD))

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

func (c *ImmuClient) Scan(keyReader io.Reader) (*schema.ItemList, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	prefix, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	return c.serviceClient.Scan(context.Background(), &schema.ScanOptions{Prefix: prefix})
}

func (c *ImmuClient) ZScan(setReader io.Reader) (*schema.ItemList, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return nil, err
	}
	return c.serviceClient.ZScan(context.Background(), &schema.ZScanOptions{Set: set})
}

func (c *ImmuClient) Count(keyReader io.Reader) (*schema.ItemsCount, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	prefix, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	return c.serviceClient.Count(context.Background(), &schema.KeyPrefix{Prefix: prefix})
}

func (c *ImmuClient) GetBatch(keyReaders []io.Reader) (*schema.ItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	keys := &schema.KeyList{}
	for _, keyReader := range keyReaders {
		key, err := ioutil.ReadAll(keyReader)
		if err != nil {
			return nil, err
		}
		keys.Keys = append(keys.Keys, &schema.Key{Key: key})
	}
	result, err := c.serviceClient.GetBatch(context.Background(), keys)
	c.Logger.Debugf("get-batch finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Set(keyReader io.Reader, valueReader io.Reader) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	value, err := ioutil.ReadAll(valueReader)
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.Set(context.Background(), &schema.KeyValue{
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

// SafeSet ...
func (c *ImmuClient) SafeSet(keyReader io.Reader, valueReader io.Reader) (*VerifiedIndex, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	value, err := ioutil.ReadAll(valueReader)
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	root, err := c.rootservice.GetRoot(context.Background())
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
		context.Background(),
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

func (c *ImmuClient) SetBatch(request *BatchRequest) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	list, err := request.toKVList()
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.SetBatch(context.Background(), list)
	c.Logger.Debugf("set-batch finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Inclusion(index uint64) (*schema.InclusionProof, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Inclusion(context.Background(), &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("inclusion finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Consistency(index uint64) (*schema.ConsistencyProof, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.Consistency(context.Background(), &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("consistency finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) ByIndex(index uint64) (*schema.Item, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.serviceClient.ByIndex(context.Background(), &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("by-index finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) History(keyReader io.Reader) (*schema.ItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.History(context.Background(), &schema.Key{
		Key: key,
	})
	c.Logger.Debugf("history finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Reference(keyReader io.Reader, valueReader io.Reader) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(valueReader)
	if err != nil {
		return nil, err
	}
	reference, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.Reference(context.Background(), &schema.ReferenceOptions{
		Reference: &schema.Key{Key: reference},
		Key:       &schema.Key{Key: key},
	})
	c.Logger.Debugf("reference finished in %s", time.Since(start))
	return result, err
}

// SafeReference ...
func (c *ImmuClient) SafeReference(keyReader io.Reader, valueReader io.Reader) (*VerifiedIndex, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(valueReader)
	if err != nil {
		return nil, err
	}
	reference, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	root, err := c.rootservice.GetRoot(context.Background())
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
		context.Background(),
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

func (c *ImmuClient) ZAdd(setReader io.Reader, score float64, keyReader io.Reader) (*schema.Index, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	result, err := c.serviceClient.ZAdd(context.Background(), &schema.ZAddOptions{
		Set:   set,
		Score: score,
		Key:   key,
	})
	c.Logger.Debugf("zadd finished in %s", time.Since(start))
	return result, err
}

// SafeZAdd ...
func (c *ImmuClient) SafeZAdd(setReader io.Reader, score float64, keyReader io.Reader) (*VerifiedIndex, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return nil, err
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}

	root, err := c.rootservice.GetRoot(context.Background())
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
		context.Background(),
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

func (c *ImmuClient) HealthCheck() error {
	start := time.Now()
	if !c.isConnected() {
		return ErrNotConnected
	}
	response, err := c.serviceClient.Health(context.Background(), &empty.Empty{})
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

func (c *ImmuClient) connectWithRetry() (err error) {
	for i := 0; i < c.Options.DialRetries+1; i++ {
		if c.clientConn, err = grpc.Dial(c.Options.Bind(), grpc.WithInsecure()); err == nil {
			c.serviceClient = schema.NewImmuServiceClient(c.clientConn)
			c.rootservice = NewRootService(c.serviceClient, cache.NewFileCache())
			if _, err = c.rootservice.GetRoot(context.Background()); err == nil {
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

func (c *ImmuClient) waitForHealthCheck() (err error) {
	for i := 0; i < c.Options.HealthCheckRetries+1; i++ {
		if err = c.HealthCheck(); err == nil {
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
