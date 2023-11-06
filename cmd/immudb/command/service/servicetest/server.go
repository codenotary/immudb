/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package servicetest

import (
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/stream"
)

func NewDefaultImmuServerMock() *ImmuServerMock {
	s := &ImmuServerMock{}
	s.InitializeF = func() error {
		return nil
	}
	s.StartF = func() error {
		return nil
	}
	s.StopF = func() error {
		return nil
	}
	s.WithOptionsF = func(options *server.Options) server.ImmuServerIf {
		return s
	}
	s.WithLoggerF = func(logger.Logger) server.ImmuServerIf {
		return s
	}
	s.WithStateSignerF = func(stateSigner server.StateSigner) server.ImmuServerIf {
		return s
	}
	return s
}

type ImmuServerMock struct {
	server.ImmuServerIf
	InitializeF               func() error
	StartF                    func() error
	StopF                     func() error
	WithOptionsF              func(options *server.Options) server.ImmuServerIf
	WithLoggerF               func(logger.Logger) server.ImmuServerIf
	WithStateSignerF          func(stateSigner server.StateSigner) server.ImmuServerIf
	WithStreamServiceFactoryF func(ssf stream.ServiceFactory) server.ImmuServerIf
}

func (d *ImmuServerMock) Initialize() error {
	return d.InitializeF()
}
func (d *ImmuServerMock) Start() error {
	return d.StartF()
}
func (d *ImmuServerMock) Stop() error {
	return d.StopF()
}
func (d *ImmuServerMock) WithOptions(options *server.Options) server.ImmuServerIf {
	return d.WithOptionsF(options)
}
func (d *ImmuServerMock) WithLogger(l logger.Logger) server.ImmuServerIf {
	return d.WithLoggerF(l)
}
func (d *ImmuServerMock) WithStateSigner(stateSigner server.StateSigner) server.ImmuServerIf {
	return d.WithStateSignerF(stateSigner)
}
func (d *ImmuServerMock) WithStreamServiceFactory(ssf stream.ServiceFactory) server.ImmuServerIf {
	return d.WithStreamServiceFactoryF(ssf)
}
