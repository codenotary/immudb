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
	"github.com/codenotary/immudb/pkg/client"
	"os"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
)

// ImmuGw ...
type ImmuGw interface {
	Start() error
	Stop() error
	WithClient(schema.ImmuServiceClient) ImmuGw
	WithLogger(logger.Logger) ImmuGw
	WithOptions(Options) ImmuGw
	WithCliOptions(client.Options) ImmuGw
}

// ImmuGwServer ...
type ImmuGwServer struct {
	Options      Options
	CliOptions   client.Options
	quit         chan struct{}
	auditorDone  chan struct{}
	Logger       logger.Logger
	Pid          server.PIDFile
	Client       schema.ImmuServiceClient
	MetricServer *metricServer
}

// DefaultServer returns a default immudb gateway server
func DefaultServer() *ImmuGwServer {
	l := logger.NewSimpleLogger("immugw ", os.Stderr)
	return &ImmuGwServer{
		Logger:       l,
		Options:      DefaultOptions(),
		quit:         make(chan struct{}),
		auditorDone:  make(chan struct{}),
		MetricServer: newMetricsServer(DefaultOptions().MetricsBind(), l, func() float64 { return time.Since(startedAt).Hours() }),
	}
}

// WithClient ...
func (c *ImmuGwServer) WithClient(client schema.ImmuServiceClient) ImmuGw {
	c.Client = client
	return c
}

// WithLogger ...
func (c *ImmuGwServer) WithLogger(logger logger.Logger) ImmuGw {
	c.Logger = logger
	return c
}

// WithOptions ...
func (c *ImmuGwServer) WithOptions(options Options) ImmuGw {
	c.Options = options
	return c
}

// WithCliOptions ...
func (c *ImmuGwServer) WithCliOptions(cliOptions client.Options) ImmuGw {
	c.CliOptions = cliOptions
	return c
}
