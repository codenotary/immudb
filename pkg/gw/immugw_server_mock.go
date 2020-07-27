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
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/logger"
)

// TODO OGG: move this in a subpackage: gwtest
// the problem is that since With... methods return gw.ImmuGw => import cycle

// ImmuGwServerMock ...
type ImmuGwServerMock struct {
	StartF          func() error
	StopF           func() error
	WithClientF     func(schema.ImmuServiceClient) ImmuGw
	WithLoggerF     func(logger.Logger) ImmuGw
	WithOptionsF    func(Options) ImmuGw
	WithCliOptionsF func(client.Options) ImmuGw
}

// Start ...
func (igm *ImmuGwServerMock) Start() error {
	if igm.StartF != nil {
		return igm.StartF()
	}
	return nil
}

// Stop ...
func (igm *ImmuGwServerMock) Stop() error {
	if igm.StopF != nil {
		return igm.StopF()
	}
	return nil
}

// WithClient ...
func (igm *ImmuGwServerMock) WithClient(client schema.ImmuServiceClient) ImmuGw {
	if igm.WithClientF != nil {
		return igm.WithClientF(client)
	}
	return igm
}

// WithLogger ...
func (igm *ImmuGwServerMock) WithLogger(logger logger.Logger) ImmuGw {
	if igm.WithLoggerF != nil {
		return igm.WithLoggerF(logger)
	}
	return igm
}

// WithOptions ...
func (igm *ImmuGwServerMock) WithOptions(options Options) ImmuGw {
	if igm.WithOptionsF != nil {
		return igm.WithOptionsF(options)
	}
	return igm
}

// WithCliOptions ...
func (igm *ImmuGwServerMock) WithCliOptions(options client.Options) ImmuGw {
	if igm.WithCliOptionsF != nil {
		return igm.WithCliOptionsF(options)
	}
	return igm
}
