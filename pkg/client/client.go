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

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/schema"
	"github.com/codenotary/immudb/pkg/server"
)

func (c *ImmuClient) Get(key []byte) ([]byte, error) {
	return c.withConnection(func(connection *grpc.ClientConn) (bytes []byte, e error) {
		client := schema.NewImmuServiceClient(connection)
		response, err := client.Get(context.Background(), &schema.GetRequest{Key: key})
		if err != nil {
			return nil, err
		}
		return response.Value, nil
	})
}

func (c *ImmuClient) Set(key []byte, reader io.Reader) ([]byte, error) {
	return c.withConnection(func(connection *grpc.ClientConn) (bytes []byte, e error) {
		client := schema.NewImmuServiceClient(connection)
		value, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		if _, err := client.Set(context.Background(), &schema.SetRequest{
			Key:   key,
			Value: value,
		}); err != nil {
			return nil, err
		}
		return value, nil
	})
}

func (c *ImmuClient) withConnection(callback func(connection *grpc.ClientConn) ([]byte, error)) ([]byte, error) {
	connection, err := grpc.Dial(c.Options.Bind(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return callback(connection)
}

func (c *ImmuClient) HealthCheck() (bool, error) {
	connection, err := grpc.Dial(c.Options.Bind(), grpc.WithInsecure())
	if err != nil {
		return false, err
	}
	defer connection.Close()
	client := schema.NewImmuServiceClient(connection)
	response, err := client.Health(context.Background(), &empty.Empty{})
	if err != nil {
		return false, err
	}
	return response.Status == server.HealthOk, nil
}
