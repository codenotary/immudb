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

package server

import (
	"os"

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
)

type ImmuServer struct {
	Store      *store.Store
	SysStore   *store.Store
	Logger     logger.Logger
	Options    Options
	GrpcServer *grpc.Server
	Pid        PIDFile
	quit       chan struct{}
}

func DefaultServer() *ImmuServer {
	return &ImmuServer{
		Logger:  logger.NewSimpleLogger("immu-server", os.Stderr),
		Options: DefaultOptions(),
		quit:    make(chan struct{}),
	}
}

func (s *ImmuServer) WithStore(st *store.Store) *ImmuServer {
	s.Store = st
	return s
}

func (s *ImmuServer) WithLogger(logger logger.Logger) *ImmuServer {
	s.Logger = logger
	return s
}

func (s *ImmuServer) WithOptions(options Options) *ImmuServer {
	s.Options = options
	return s
}
