/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"hash/fnv"
	"math"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/codenotary/immudb/pkg/audit"
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

// userdataShardCount splits the username→userdata association into N
// independent buckets so that concurrent authenticated RPCs do not contend
// on a single shared mutex. 32 was chosen for symmetry with the session
// store (manager.shards) and as a balance between contention reduction
// and per-shard footprint (the shards array is inlined into the struct).
const userdataShardCount = 32

// userdataShard is one bucket of the sharded login state.
type userdataShard struct {
	mu            sync.RWMutex
	Userdata      map[string]*auth.User
	sessionCounts map[string]int
}

// usernameToUserdataMap keeps an associacion of username to userdata.
// Backed by a fixed-size shard array so reads/writes for unrelated
// usernames take independent locks. Every authenticated RPC passes
// through getLoggedInUserDataFromUsername so this map's lock-shape
// dominates auth-path contention under high client concurrency.
type usernameToUserdataMap struct {
	shards [userdataShardCount]userdataShard
}

func newUsernameToUserdataMap() *usernameToUserdataMap {
	m := &usernameToUserdataMap{}
	for i := range m.shards {
		m.shards[i].Userdata = make(map[string]*auth.User)
		m.shards[i].sessionCounts = make(map[string]int)
	}
	return m
}

func (m *usernameToUserdataMap) shardFor(username string) *userdataShard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(username))
	return &m.shards[h.Sum64()%userdataShardCount]
}

// Get returns the userdata for username plus an existence flag, taking only
// the per-shard read lock.
func (m *usernameToUserdataMap) Get(username string) (*auth.User, bool) {
	shard := m.shardFor(username)
	shard.mu.RLock()
	u, ok := shard.Userdata[username]
	shard.mu.RUnlock()
	return u, ok
}

// SetUserdata writes a userdata entry without touching the session counter.
// Intended for tests that need to inject or restore userdata state.
func (m *usernameToUserdataMap) SetUserdata(username string, u *auth.User) {
	shard := m.shardFor(username)
	shard.mu.Lock()
	shard.Userdata[username] = u
	shard.mu.Unlock()
}

// DeleteUserdata removes a userdata entry without touching the session
// counter. Intended for tests that want to simulate ErrNotLoggedIn.
func (m *usernameToUserdataMap) DeleteUserdata(username string) {
	shard := m.shardFor(username)
	shard.mu.Lock()
	delete(shard.Userdata, username)
	shard.mu.Unlock()
}

// AddSession records a new login: stores the userdata and increments the
// session counter for username. Used on every successful Login.
func (m *usernameToUserdataMap) AddSession(u *auth.User) {
	shard := m.shardFor(u.Username)
	shard.mu.Lock()
	shard.Userdata[u.Username] = u
	shard.sessionCounts[u.Username]++
	shard.mu.Unlock()
}

// RemoveSession decrements the session counter for username; when it
// reaches zero the userdata entry is removed. Returns true when the last
// session was removed, mirroring the previous removeUserFromLoginList.
func (m *usernameToUserdataMap) RemoveSession(username string) bool {
	shard := m.shardFor(username)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.sessionCounts[username]--
	if shard.sessionCounts[username] <= 0 {
		delete(shard.sessionCounts, username)
		delete(shard.Userdata, username)
		return true
	}
	return false
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
	auditLogger          *audit.Logger
	metricsServer        *http.Server
	webServer            *http.Server
	webGrpcConn          *grpc.ClientConn
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
		userdata:             newUsernameToUserdataMap(),
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
