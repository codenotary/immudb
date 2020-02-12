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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

const ROOT_FN = ".root"

type RootService interface {
	GetRoot(ctx context.Context) (*schema.Root, error)
	SetRoot(root *schema.Root)  error
}

type rootservice struct {
	client schema.ImmuServiceClient
	cache cache.Cache
}

func NewRootService(immuC schema.ImmuServiceClient, cache cache.Cache) RootService {
	return &rootservice{immuC, cache}
}

func (r *rootservice) GetRoot(ctx context.Context) (*schema.Root, error) {
	if root, err := r.cache.Get(); err == nil {
		return root, nil
	}
	var protoReq empty.Empty
	var metadata runtime.ServerMetadata
	if root, err := r.client.CurrentRoot(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		return nil, err
	} else {
		if err := r.cache.Set(root); err != nil {
			return nil, err
		}
		return root, nil
	}
}

func (r *rootservice) SetRoot(root *schema.Root) error {
	return r.cache.Set(root)
}

