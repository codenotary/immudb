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

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
)

type ImmuClient struct {
	Logger  logger.Logger
	Options Options

	clientConn    *grpc.ClientConn
	serviceClient schema.ImmuServiceClient
}

func DefaultClient() *ImmuClient {
	return &ImmuClient{
		Logger:  logger.New("immu-client", os.Stderr),
		Options: DefaultOptions(),
	}
}

func (c *ImmuClient) WithLogger(logger logger.Logger) *ImmuClient {
	c.Logger = logger
	return c
}

func (c *ImmuClient) WithOptions(options Options) *ImmuClient {
	c.Options = options
	return c
}
