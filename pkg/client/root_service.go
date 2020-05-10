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
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

type RootService interface {
	GetRoot(ctx context.Context) (*schema.Root, error)
	SetRoot(root *schema.Root) error
}

type rootservice struct {
	client     schema.ImmuServiceClient
	cache      cache.Cache
	serverUuid string
	logger     logger.Logger
	sync.RWMutex
}

func NewRootService(immuC schema.ImmuServiceClient, cache cache.Cache, logger logger.Logger) RootService {
	var protoReq empty.Empty
	var metadata runtime.ServerMetadata
	var serverUuid string

	if _, err := immuC.Health(context.Background(), &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		return nil
	}
	if len(metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)) > 0 {
		serverUuid = metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)[0]
	}
	if serverUuid == "" {
		logger.Warningf("the immudb-uuid header was not provided. Communication with multiple immudb instances that do not provide the identifier is not allowed")
	}
	return &rootservice{
		client:     immuC,
		cache:      cache,
		logger:     logger,
		serverUuid: serverUuid,
	}
}

func (r *rootservice) GetRoot(ctx context.Context) (*schema.Root, error) {
	defer r.RUnlock()
	r.RLock()
	var metadata runtime.ServerMetadata
	var protoReq empty.Empty
	if root, err := r.cache.Get(r.serverUuid); err == nil {
		return root, nil
	}
	if root, err := r.client.CurrentRoot(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		return nil, err
	} else {
		if err := r.cache.Set(root, r.serverUuid); err != nil {
			return nil, err
		}
		return root, nil
	}
}

func (r *rootservice) SetRoot(root *schema.Root) error {
	defer r.Unlock()
	r.Lock()
	return r.cache.Set(root, r.serverUuid)
}
