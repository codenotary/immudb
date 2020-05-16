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

package gw

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"os"
)

type ImmuGwServer struct {
	Options Options
	quit    chan struct{}
	Logger  logger.Logger
	Pid     server.PIDFile
	Client  schema.ImmuServiceClient
}

func DefaultServer() *ImmuGwServer {
	return &ImmuGwServer{
		Logger:  logger.NewSimpleLogger("immugw-server", os.Stderr),
		Options: DefaultOptions(),
		quit:    make(chan struct{}),
	}
}

func (c *ImmuGwServer) WithClient(client schema.ImmuServiceClient) *ImmuGwServer {
	c.Client = client
	return c
}

func (c *ImmuGwServer) WithLogger(logger logger.Logger) *ImmuGwServer {
	c.Logger = logger
	return c
}

func (c *ImmuGwServer) WithOptions(options Options) *ImmuGwServer {
	c.Options = options
	return c
}
