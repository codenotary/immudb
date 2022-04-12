/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package state

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

type StateProvider interface {
	CurrentState(ctx context.Context) (*schema.ImmutableState, error)
}

type stateProvider struct {
	client schema.ImmuServiceClient
}

func NewStateProvider(client schema.ImmuServiceClient) StateProvider {
	return &stateProvider{client}
}

func (r *stateProvider) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	var metadata runtime.ServerMetadata
	var protoReq empty.Empty
	return r.client.CurrentState(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
}
