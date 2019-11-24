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
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/schema"
	"github.com/codenotary/immudb/pkg/server"
)

var AlreadyConnectedError = fmt.Errorf("already connected")
var NotConnectedError = fmt.Errorf("not connected")

func (c *ImmuClient) Connect() (err error) {
	if c.isConnected() {
		return AlreadyConnectedError
	}
	if c.clientConn, err = grpc.Dial(c.Options.Bind(), grpc.WithInsecure()); err != nil {
		return err
	}
	c.serviceClient = schema.NewImmuServiceClient(c.clientConn)
	c.Logger.Debugf("connected %v", c.Options)
	return nil
}

func (c *ImmuClient) Disconnect() error {
	if !c.isConnected() {
		return NotConnectedError
	}
	if err := c.clientConn.Close(); err != nil {
		return err
	}
	c.serviceClient = nil
	c.clientConn = nil
	c.Logger.Debugf("disconnected %v", c.Options)
	return nil
}

func (c *ImmuClient) Get(key []byte) ([]byte, error) {
	if !c.isConnected() {
		return nil, NotConnectedError
	}
	response, err := c.serviceClient.Get(context.Background(), &schema.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func (c *ImmuClient) Set(key []byte, reader io.Reader) ([]byte, error) {
	if !c.isConnected() {
		return nil, NotConnectedError
	}
	value, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if _, err := c.serviceClient.Set(context.Background(), &schema.SetRequest{
		Key:   key,
		Value: value,
	}); err != nil {
		return nil, err
	}
	return value, nil
}

func (c *ImmuClient) HealthCheck() (bool, error) {
	if !c.isConnected() {
		return false, NotConnectedError
	}
	client := schema.NewImmuServiceClient(c.clientConn)
	response, err := client.Health(context.Background(), &empty.Empty{})
	if err != nil {
		return false, err
	}
	return response.Status == server.HealthOk, nil
}

func (c *ImmuClient) isConnected() bool {
	return c.clientConn != nil && c.serviceClient != nil
}
