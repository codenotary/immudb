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

package client

import (
	"crypto/ecdsa"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/logger"
	"google.golang.org/grpc"
)

func (c *immuClient) WithLogger(logger logger.Logger) *immuClient {
	c.Logger = logger
	return c
}

func (c *immuClient) WithStateService(rs state.StateService) *immuClient {
	c.StateService = rs
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

func (c *immuClient) WithTokenService(tokenService tokenservice.TokenService) *immuClient {
	c.Tkns = tokenService
	return c
}

func (c *immuClient) WithServerSigningPubKey(publicKey *ecdsa.PublicKey) *immuClient {
	c.serverSigningPubKey = publicKey
	return c
}

// WithStreamServiceFactory set stream service factory
func (c *immuClient) WithStreamServiceFactory(ssf stream.ServiceFactory) *immuClient {
	c.StreamServiceFactory = ssf
	return c
}

func (c *immuClient) WithOptions(options *Options) *immuClient {
	c.Options = options
	return c
}
