/*
Copyright 2019 vChain, Inc.

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

	"github.com/codenotary/immudb/pkg/schema"
)

func (c *ImmuClient) Connect() (err error) {
	if c.isConnected() {
		return ErrAlreadyConnected
	}
	if err := c.connectWithRetry(); err != nil {
		return err
	}
	if err := c.waitForHealthCheck(); err != nil {
		return err
	}
	return nil
}

func (c *ImmuClient) Disconnect() error {
	if !c.isConnected() {
		return ErrNotConnected
	}
	if err := c.clientConn.Close(); err != nil {
		return err
	}
	c.serviceClient = nil
	c.clientConn = nil
	c.Logger.Debugf("disconnected %v", c.Options)
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

func (c *ImmuClient) Get(keyReader io.Reader) (*schema.GetResponse, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return nil, err
	}
	return c.serviceClient.Get(context.Background(), &schema.GetRequest{Key: key})
}

func (c *ImmuClient) Set(keyReader io.Reader, valueReader io.Reader) (*schema.SetResponse, error) {
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
	return c.serviceClient.Set(context.Background(), &schema.SetRequest{
		Key:   key,
		Value: value,
	})
}

func (c *ImmuClient) SetBatch(request *BatchRequest) (*schema.SetResponse, error) {
	if !c.isConnected() {
		return nil, ErrNotConnected
	}
	bsr, err := request.toBatchSetRequest()
	if err != nil {
		return nil, err
	}
	return c.serviceClient.SetBatch(context.Background(), bsr)
}

func (c *ImmuClient) HealthCheck() error {
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
	}
	return err
}
