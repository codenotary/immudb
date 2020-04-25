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
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
)

func (c *immuClient) WithLogger(logger logger.Logger) *immuClient {
	c.Logger = logger
	return c
}

func (c *immuClient) WithRootService(rs RootService) *immuClient {
	c.Rootservice = rs
	return c
}

func (c *immuClient) WithTimestampService(ts TimestampService) *immuClient {
	c.ts = ts
	return c
}

func (c *immuClient) WithClientConn(clientConn *grpc.ClientConn) *immuClient {
	c.clientConn = clientConn
	return c
}

func (c *immuClient) WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient {
	c.ServiceClient = serviceClient
	return c
}

func (c *immuClient) WithOptions(options *Options) *immuClient {
	c.Options = options
	return c
}

func (c *immuClient) NewSKV(key []byte, value []byte) *schema.StructuredKeyValue {
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
// ProtoMessage ...
func (*VerifiedIndex) ProtoMessage() {}
// Reset ...
func (vi *VerifiedItem) Reset() { *vi = VerifiedItem{} }

func (vi *VerifiedItem) String() string { return proto.CompactTextString(vi) }

// ProtoMessage ...
func (*VerifiedItem) ProtoMessage() {}
