/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"math"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/pkg/follower"
	pgsqlsrv "github.com/codenotary/immudb/pkg/pgsql/server"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/rs/xid"

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/codenotary/immudb/pkg/logger"
)

// userDatabasePairs keeps an associacion of username to userdata
type usernameToUserdataMap struct {
	Userdata map[string]*auth.User
	sync.RWMutex
}

//defaultDbIndex systemdb should always be in index 0
const defaultDbIndex = 0
const sysDBIndex = int64(math.MaxInt64)

// ImmuServer ...
type ImmuServer struct {
	OS immuos.OS

	dbList database.DatabaseList

	followers      map[string]*follower.Follower
	followersMutex sync.Mutex

	Logger      logger.Logger
	Options     *Options
	listener    net.Listener
	GrpcServer  *grpc.Server
	UUID        xid.ID
	Pid         PIDFile
	quit        chan struct{}
	userdata    *usernameToUserdataMap
	multidbmode bool
	//Cc                  CorruptionChecker
	sysDB                database.DB
	metricsServer        *http.Server
	webServer            *http.Server
	mux                  sync.Mutex
	pgsqlMux             sync.Mutex
	StateSigner          StateSigner
	StreamServiceFactory stream.ServiceFactory
	PgsqlSrv             pgsqlsrv.Server

	remoteStorage remotestorage.Storage
}

// DefaultServer ...
func DefaultServer() *ImmuServer {
	return &ImmuServer{
		OS:                   immuos.NewStandardOS(),
		dbList:               database.NewDatabaseList(),
		followers:            make(map[string]*follower.Follower),
		Logger:               logger.NewSimpleLogger("immudb ", os.Stderr),
		Options:              DefaultOptions(),
		quit:                 make(chan struct{}),
		userdata:             &usernameToUserdataMap{Userdata: make(map[string]*auth.User)},
		GrpcServer:           grpc.NewServer(),
		StreamServiceFactory: stream.NewStreamServiceFactory(DefaultOptions().StreamChunkSize),
	}
}

type ImmuServerIf interface {
	Initialize() error
	Start() error
	Stop() error
	WithOptions(options *Options) ImmuServerIf
	WithLogger(logger.Logger) ImmuServerIf
	WithStateSigner(stateSigner StateSigner) ImmuServerIf
	WithStreamServiceFactory(ssf stream.ServiceFactory) ImmuServerIf
	WithPgsqlServer(psrv pgsqlsrv.Server) ImmuServerIf
}

// WithLogger ...
func (s *ImmuServer) WithLogger(logger logger.Logger) ImmuServerIf {
	s.Logger = logger
	return s
}

// WithStateSigner ...
func (s *ImmuServer) WithStateSigner(stateSigner StateSigner) ImmuServerIf {
	s.StateSigner = stateSigner
	return s
}

func (s *ImmuServer) WithStreamServiceFactory(ssf stream.ServiceFactory) ImmuServerIf {
	s.StreamServiceFactory = ssf
	return s
}

// WithOptions ...
func (s *ImmuServer) WithOptions(options *Options) ImmuServerIf {
	s.Options = options
	return s
}

// WithPgsqlServer ...
func (s *ImmuServer) WithPgsqlServer(psrv pgsqlsrv.Server) ImmuServerIf {
	s.PgsqlSrv = psrv
	return s
}
