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
	"fmt"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

// RootService the root service interface
type RootService interface {
	GetRoot(ctx context.Context, databasename string) (*schema.Root, error)
	SetRoot(root *schema.Root, databasename string) error
}

type rootservice struct {
	client     schema.ImmuServiceClient
	cache      cache.Cache
	serverUuid string
	logger     logger.Logger
	sync.RWMutex
}

// NewRootService ...
func NewRootService(immuC schema.ImmuServiceClient, cache cache.Cache, logger logger.Logger) RootService {
	serverUuid, err := GetServerUuid(context.Background(), immuC)
	if err != nil {
		if err != ErrNoServerUuid {
			return nil // TODO OGG: check with Michele if this was intended or a mistake
		}
		logger.Warningf(err.Error())
	}
	return &rootservice{
		client:     immuC,
		cache:      cache,
		logger:     logger,
		serverUuid: serverUuid,
	}
}

func (r *rootservice) GetRoot(ctx context.Context, databasename string) (*schema.Root, error) {
	defer r.Unlock()
	r.Lock()
	var metadata runtime.ServerMetadata
	var protoReq empty.Empty
	if root, err := r.cache.Get(r.serverUuid, databasename); err == nil {
		return root, nil
	}
	if root, err := r.client.CurrentRoot(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD)); err != nil {
		return nil, err
	} else {
		if err := r.cache.Set(root, r.serverUuid, databasename); err != nil {
			return nil, err
		}
		return root, nil
	}
}

func (r *rootservice) SetRoot(root *schema.Root, databasename string) error {
	defer r.Unlock()
	r.Lock()
	return r.cache.Set(root, r.serverUuid, databasename)
}

// ErrNoServerUuid ...
var ErrNoServerUuid = fmt.Errorf(
	"!IMPORTANT WARNING: %s header is not published by the immudb server; "+
		"this client MUST NOT be used to connect to different immudb servers!",
	server.SERVER_UUID_HEADER)

// GetServerUuid issues a Health command to the server, then parses and returns
// the server UUID from the response metadata
func GetServerUuid(
	ctx context.Context,
	immuClient schema.ImmuServiceClient,
) (string, error) {
	var metadata runtime.ServerMetadata
	if _, err := immuClient.Health(
		ctx,
		new(empty.Empty),
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	); err != nil {
		return "", err
	}
	var serverUuid string
	if len(metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)) > 0 {
		serverUuid = metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)[0]
	}
	if serverUuid == "" {
		return "", ErrNoServerUuid
	}
	return serverUuid, nil
}
