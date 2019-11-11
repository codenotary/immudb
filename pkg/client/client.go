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

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/schema"
)

func Get(address string, key string) ([]byte, error) {
	return withConnection(address, func(connection *grpc.ClientConn) (bytes []byte, e error) {
		client := schema.NewImmuServiceClient(connection)
		response, err := client.Get(context.Background(), &schema.GetRequest{
			Key: key,
		})
		if err != nil {
			return nil, err
		}
		if response.Status != 0 {
			return nil, fmt.Errorf("server error")
		}
		return response.Value, nil
	})
}

func Set(address string, key string, value string) error {
	_, err := withConnection(address, func(connection *grpc.ClientConn) (bytes []byte, e error) {
		client := schema.NewImmuServiceClient(connection)
		response, err := client.Set(context.Background(), &schema.SetRequest{
			Key:   key,
			Value: []byte(value),
		})
		if err != nil {
			return nil, err
		}
		if response.Status != 0 {
			return nil, fmt.Errorf("server error")
		}
		return nil, nil
	})
	return err
}

func withConnection(address string, callback func(connection *grpc.ClientConn) ([]byte, error)) ([]byte, error) {
	connection, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return callback(connection)
}
