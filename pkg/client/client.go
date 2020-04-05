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
	"context"
	"io"
	"io/ioutil"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
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

func (c *ImmuClient) Get(keyReader io.Reader) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	item, err := c.serviceClient.Get(context.Background(), &schema.Key{Key: key})
	result := item.ToSItem()
	c.Logger.Debugf("get finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) Scan(keyReader io.Reader) (*schema.StructuredItemList, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	prefix, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	list, err := c.serviceClient.Scan(context.Background(), &schema.ScanOptions{Prefix: prefix})
	return list.ToSItemList(), err
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
	skv := schema.NewSKV(key, value)
	result, err := c.serviceClient.Set(context.Background(), skv.ToKV())
	c.Logger.Debugf("set finished in %s", time.Since(start))
	return result, err
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

func (c *ImmuClient) ByIndex(index uint64) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	item, err := c.serviceClient.ByIndex(context.Background(), &schema.Index{
		Index: index,
	})
	result := item.ToSItem()
	c.Logger.Debugf("by-index finished in %s", time.Since(start))
	return result, err
}

func (c *ImmuClient) History(keyReader io.Reader) (*schema.StructuredItemList, error) {
	start := time.Now()
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	list, err := c.serviceClient.History(context.Background(), &schema.Key{
		Key: key,
	})
	result := list.ToSItemList()
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
			c.Logger.Debugf("dialed %v", c.Options)
			return nil
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
