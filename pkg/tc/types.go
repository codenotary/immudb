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

package tc

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"net/http"
	"os"
)

type ImmuTcServer struct {
	Options     Options
	Done        chan bool
	Quit        chan os.Signal
	HttpServer  *http.Server
	Checker     ImmuTc
	Logger      logger.Logger
	LogFile     *os.File
	RootService client.RootService
	Pid         server.PIDFile
	Client      schema.ImmuServiceClient
}

func DefaultServer() *ImmuTcServer {
	return &ImmuTcServer{
		Logger:  logger.NewSimpleLogger("immutc", os.Stderr),
		Options: DefaultOptions(),
		Done:    make(chan bool, 1),
		Quit:    make(chan os.Signal, 1),
	}
}

func (c *ImmuTcServer) WithRootService(rootService client.RootService) *ImmuTcServer {
	c.RootService = rootService
	return c
}

func (c *ImmuTcServer) WithClient(client schema.ImmuServiceClient) *ImmuTcServer {
	c.Client = client
	return c
}

func (c *ImmuTcServer) WithLogger(logger logger.Logger) *ImmuTcServer {
	c.Logger = logger
	return c
}

func (c *ImmuTcServer) WithChecker(checker ImmuTc) *ImmuTcServer {
	c.Checker = checker
	return c
}

func (c *ImmuTcServer) WithOptions(options Options) *ImmuTcServer {
	c.Options = options
	return c
}
