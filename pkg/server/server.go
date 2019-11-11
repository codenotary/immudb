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

package server

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/schema"
)

type ImmuServer struct{}

var store []byte

func Run(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	var serverOptions []grpc.ServerOption
	server := grpc.NewServer(serverOptions...)
	schema.RegisterImmuServiceServer(server, &ImmuServer{})
	return server.Serve(lis)
}

func (s ImmuServer) Set(ctx context.Context, sr *schema.SetRequest) (*schema.SetResponse, error) {
	fmt.Println("Set", sr.Key)
	store = sr.Value
	return &schema.SetResponse{
		Status: 0,
	}, nil
}

func (s ImmuServer) Get(ctx context.Context, gr *schema.GetRequest) (*schema.GetResponse, error) {
	fmt.Println("Get", gr.Key)
	return &schema.GetResponse{
		Status: 0,
		Key:    "test",
		Value:  store,
	}, nil
}
