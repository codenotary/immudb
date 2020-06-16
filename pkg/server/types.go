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
	"sync"

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
)

type userDatabasePair struct {
	userUUID string
	auth.User
	//index of the db instance in the Immuserver slice
	index int
}

// userDatabasePairs keeps an associacion of user UUID
//    which is assigned at login and index in the databases array
//    index 0 is always reserved for the system database
type userDatabasePairs struct {
	userDatabaseID map[string]*userDatabasePair
	sync.RWMutex
}

// ImmuServer ...
type ImmuServer struct {
	// Store         *store.Store
	// SysStore      *store.Store
	//SystemAdminDb *Db
	databases     []*Db //TODO gj slice operations protect with mutex
	Logger        logger.Logger
	Options       Options
	GrpcServer    *grpc.Server
	Cc            CorruptionChecker
	Pid           PIDFile
	quit          chan struct{}
	userDatabases *userDatabasePairs
}

// DefaultServer ...
func DefaultServer() *ImmuServer {
	return &ImmuServer{
		Logger:        logger.NewSimpleLogger("immudb ", os.Stderr),
		Options:       DefaultOptions(),
		quit:          make(chan struct{}),
		userDatabases: &userDatabasePairs{userDatabaseID: make(map[string]*userDatabasePair)},
	}
}

// func (s *ImmuServer) WithStore(st *store.Store) *ImmuServer {
// 	s.Store = st
// 	return s
// }

// WithLogger ...
func (s *ImmuServer) WithLogger(logger logger.Logger) *ImmuServer {
	s.Logger = logger
	return s
}

// WithCC ...
func (s *ImmuServer) WithCC(cc CorruptionChecker) *ImmuServer {
	s.Cc = cc
	return s
}

// WithOptions ...
func (s *ImmuServer) WithOptions(options Options) *ImmuServer {
	s.Options = options
	return s
}
