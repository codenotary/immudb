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
	"os"
	"sync"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
)

type ImmuClient struct {
	Logger        logger.Logger
	Options       Options
	clientConn    *grpc.ClientConn
	ServiceClient schema.ImmuServiceClient
	rootservice   RootService
	ts            TimestampService
	sync.RWMutex
}

func DefaultClient() *ImmuClient {
	return &ImmuClient{
		Logger:  logger.NewSimpleLogger("immuclient", os.Stderr),
		Options: DefaultOptions(),
	}
}

func (c *ImmuClient) WithLogger(logger logger.Logger) *ImmuClient {
	c.Logger = logger
	return c
}

func (c *ImmuClient) WithRootService(rs RootService) *ImmuClient {
	c.rootservice = rs
	return c
}

func (c *ImmuClient) WithTimestampService(ts TimestampService) *ImmuClient {
	c.ts = ts
	return c
}

func (c *ImmuClient) WithClientConn(clientConn *grpc.ClientConn) *ImmuClient {
	c.clientConn = clientConn
	return c
}

func (c *ImmuClient) WithServiceClient(serviceClient schema.ImmuServiceClient) *ImmuClient {
	c.ServiceClient = serviceClient
	return c
}

func (c *ImmuClient) WithOptions(options Options) *ImmuClient {
	c.Options = options
	return c
}

func (c *ImmuClient) NewSKV(key []byte, value []byte) *schema.StructuredKeyValue {
	return &schema.StructuredKeyValue{
		Key: key,
		Value: &schema.Content{
			Timestamp: uint64(c.ts.GetTime().Unix()),
			Payload:   value,
		},
	}
}

// VerifiedItem ...
type VerifiedItem struct {
	Key      []byte `json:"key"`
	Value    []byte `json:"value"`
	Index    uint64 `json:"index"`
	Time     uint64 `json:"time"`
	Verified bool   `json:"verified"`
}

// VerifiedIndex ...
type VerifiedIndex struct {
	Index    uint64 `json:"index"`
	Verified bool   `json:"verified"`
}

// Reset ...
func (vi *VerifiedIndex) Reset() { *vi = VerifiedIndex{} }

func (vi *VerifiedIndex) String() string { return proto.CompactTextString(vi) }
