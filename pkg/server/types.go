/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/codenotary/immudb/pkg/truncator"

	"github.com/codenotary/immudb/embedded/remotestorage"
	pgsqlsrv "github.com/codenotary/immudb/pkg/pgsql/server"
	"github.com/codenotary/immudb/pkg/replication"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/rs/xid"

	"google.golang.org/grpc"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/immuos"
)

// usernameToUserdataMap keeps an associacion of username to userdata
type usernameToUserdataMap struct {
	Userdata map[string]*auth.User
	sync.RWMutex
}

// defaultDbIndex systemdb should always be in index 0
const (
	defaultDbIndex = 0
	sysDBIndex     = math.MaxInt32
)

// ImmuServer ...
type ImmuServer struct {
	OS immuos.OS

	dbListMutex sync.Mutex
	dbList      database.DatabaseList

	replicators      map[string]*replication.TxReplicator
	replicationMutex sync.Mutex

	truncators     map[string]*truncator.Truncator
	truncatorMutex sync.Mutex

	Logger      logger.Logger
	Options     *Options
	Listener    net.Listener
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
	PgsqlSrv             pgsqlsrv.PGSQLServer

	remoteStorage remotestorage.Storage
	SessManager   sessions.Manager
}

// DefaultServer returns a new ImmuServer instance with all configuration options set to their default values.
func DefaultServer() *ImmuServer {
	s := &ImmuServer{
		OS:                   immuos.NewStandardOS(),
		replicators:          make(map[string]*replication.TxReplicator),
		truncators:           make(map[string]*truncator.Truncator),
		Logger:               logger.NewSimpleLogger("immudb ", os.Stderr),
		Options:              DefaultOptions(),
		quit:                 make(chan struct{}),
		userdata:             &usernameToUserdataMap{Userdata: make(map[string]*auth.User)},
		GrpcServer:           grpc.NewServer(),
		StreamServiceFactory: stream.NewStreamServiceFactory(DefaultOptions().StreamChunkSize),
	}

	s.dbList = database.NewDatabaseList(database.NewDBManager(func(name string, opts *database.Options) (database.DB, error) {
		return database.OpenDB(name, s.multidbHandler(), opts, s.Logger)
	}, s.Options.MaxActiveDatabases, s.Logger))
	return s
}

type ImmuServerIf interface {
	Initialize() error
	Start() error
	Stop() error
	WithOptions(options *Options) ImmuServerIf
	WithLogger(logger.Logger) ImmuServerIf
	WithStateSigner(stateSigner StateSigner) ImmuServerIf
	WithStreamServiceFactory(ssf stream.ServiceFactory) ImmuServerIf
	WithPgsqlServer(psrv pgsqlsrv.PGSQLServer) ImmuServerIf
	WithDbList(dbList database.DatabaseList) ImmuServerIf
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
func (s *ImmuServer) WithPgsqlServer(psrv pgsqlsrv.PGSQLServer) ImmuServerIf {
	s.PgsqlSrv = psrv
	return s
}

// WithDbList ...
func (s *ImmuServer) WithDbList(dbList database.DatabaseList) ImmuServerIf {
	s.dbList = dbList
	return s
}
