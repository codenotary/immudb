/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/codenotary/immudb/pkg/server/sessions"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/replication"

	pgsqlsrv "github.com/codenotary/immudb/pkg/pgsql/server"

	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/pkg/database"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/signer"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	//KeyPrefixUser All user keys in the key/value store are prefixed by this keys to distinguish them from keys that have other purposes
	KeyPrefixUser = iota + 1
	//KeyPrefixDBSettings is used for entries related to database settings
	KeyPrefixDBSettings
)

var startedAt time.Time

var immudbTextLogo = " _                               _ _     \n" +
	"(_)                             | | |    \n" +
	" _ _ __ ___  _ __ ___  _   _  __| | |__  \n" +
	"| | '_ ` _ \\| '_ ` _ \\| | | |/ _` | '_ \\ \n" +
	"| | | | | | | | | | | | |_| | (_| | |_) |\n" +
	"|_|_| |_| |_|_| |_| |_|\\__,_|\\__,_|_.__/ \n"

// Initialize initializes dependencies, set up multi database capabilities and stats
func (s *ImmuServer) Initialize() error {
	_, err := fmt.Fprintf(os.Stdout, "%s\n%s\n%s\n\n", immudbTextLogo, version.VersionStr(), s.Options)
	logErr(s.Logger, "Error printing immudb config: %v", err)

	if s.Options.Logfile != "" {
		s.Logger.Infof("\n%s\n%s\n%s\n\n", immudbTextLogo, version.VersionStr(), s.Options)
	}

	if s.Options.GetMaintenance() && s.Options.GetAuth() {
		return ErrAuthMustBeDisabled
	}

	adminPassword, err := auth.DecodeBase64Password(s.Options.AdminPassword)
	if err != nil {
		return logErr(s.Logger, "%v", err)
	}

	if len(adminPassword) == 0 {
		s.Logger.Errorf(ErrEmptyAdminPassword.Error())
		return ErrEmptyAdminPassword
	}

	dataDir := s.Options.Dir
	err = os.MkdirAll(dataDir, store.DefaultFileMode)
	if err != nil {
		return logErr(s.Logger, "Unable to create data dir: %v", err)
	}

	s.remoteStorage, err = s.createRemoteStorageInstance()
	if err != nil {
		return logErr(s.Logger, "Unable to open remote storage: %v", err)
	}

	err = s.initializeRemoteStorage(s.remoteStorage)
	if err != nil {
		return logErr(s.Logger, "Unable to initialize remote storage: %v", err)
	}

	if err = s.loadSystemDatabase(dataDir, s.remoteStorage, adminPassword); err != nil {
		return logErr(s.Logger, "Unable to load system database: %v", err)
	}

	if err = s.loadDefaultDatabase(dataDir, s.remoteStorage); err != nil {
		return logErr(s.Logger, "Unable to load default database: %v", err)
	}

	defaultDB, _ := s.dbList.GetByIndex(defaultDbIndex)

	dbSize, _ := defaultDB.Size()
	if dbSize <= 1 {
		s.Logger.Infof("Started with an empty default database")
	}

	if s.sysDB.IsReplica() {
		s.Logger.Infof("Recovery mode. Only '%s' and '%s' databases are loaded", SystemDBName, DefaultDBName)
	} else {
		if err = s.loadUserDatabases(dataDir, s.remoteStorage); err != nil {
			return logErr(s.Logger, "Unable load databases: %v", err)
		}
	}

	s.multidbmode = s.mandatoryAuth()
	if !s.Options.GetAuth() && s.multidbmode {
		return ErrAuthMustBeEnabled
	}

	s.SessManager, err = sessions.NewManager(s.Options.SessionsOptions)
	if err != nil {
		return err
	}

	grpcSrvOpts := []grpc.ServerOption{}
	if s.Options.TLSConfig != nil {
		grpcSrvOpts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(s.Options.TLSConfig))}
	}

	if s.Options.SigningKey != "" {
		if signer, err := signer.NewSigner(s.Options.SigningKey); err != nil {
			return logErr(s.Logger, "Unable to configure the cryptographic signer: %v", err)
		} else {
			s.StateSigner = NewStateSigner(signer)
		}
	}

	if s.Options.usingCustomListener {
		s.Logger.Infof("Using custom listener")
		s.Listener = s.Options.listener
	} else {
		s.Listener, err = net.Listen(s.Options.Network, s.Options.Bind())
		if err != nil {
			return logErr(s.Logger, "Immudb unable to listen: %v", err)
		}
	}

	systemDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDBName())

	if s.UUID, err = getOrSetUUID(dataDir, systemDbRootDir); err != nil {
		return logErr(s.Logger, "Unable to get or set uuid: %v", err)
	}

	if s.remoteStorage != nil {
		err := s.updateRemoteUUID(s.remoteStorage)
		if err != nil {
			return logErr(s.Logger, "Unable to persist uuid on the remote storage: %v", err)
		}
	}

	auth.AuthEnabled = s.Options.GetAuth()
	auth.DevMode = s.Options.DevMode
	auth.UpdateMetrics = func(ctx context.Context) { Metrics.UpdateClientMetrics(ctx) }

	if err = s.setupPidFile(); err != nil {
		return err
	}

	if s.Options.StreamChunkSize < stream.MinChunkSize {
		return errors.New(stream.ErrChunkTooSmall).WithCode(errors.CodInvalidParameterValue)
	}

	//===> !NOTE: See Histograms section here:
	// https://github.com/grpc-ecosystem/go-grpc-prometheus
	// TL;DR:
	// Prometheus histograms are a great way to measure latency distributions of
	// your RPCs. However, since it is bad practice to have metrics of high
	// cardinality the latency monitoring metrics are disabled by default. To
	// enable them the following has to be called during initialization code:
	if !s.Options.NoHistograms {
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	//<===

	uuidContext := NewUUIDContext(s.UUID)

	uis := []grpc.UnaryServerInterceptor{
		ErrorMapper, // converts errors in gRPC ones. Need to be the first
		s.KeepAliveSessionInterceptor,
		uuidContext.UUIDContextSetter,
		grpc_prometheus.UnaryServerInterceptor,
		auth.ServerUnaryInterceptor,
		s.SessionAuthInterceptor,
	}
	sss := []grpc.StreamServerInterceptor{
		ErrorMapperStream, // converts errors in gRPC ones. Need to be the first
		s.KeepALiveSessionStreamInterceptor,
		uuidContext.UUIDStreamContextSetter,
		grpc_prometheus.StreamServerInterceptor,
		auth.ServerStreamInterceptor,
	}
	grpcSrvOpts = append(
		grpcSrvOpts,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(uis...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(sss...)),
		grpc.MaxRecvMsgSize(s.Options.MaxRecvMsgSize),
	)

	s.GrpcServer = grpc.NewServer(grpcSrvOpts...)
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
	grpc_prometheus.Register(s.GrpcServer)

	s.PgsqlSrv = pgsqlsrv.New(pgsqlsrv.Address(s.Options.Address), pgsqlsrv.Port(s.Options.PgsqlServerPort), pgsqlsrv.DatabaseList(s.dbList), pgsqlsrv.SysDb(s.sysDB), pgsqlsrv.TlsConfig(s.Options.TLSConfig), pgsqlsrv.Logger(s.Logger))
	if s.Options.PgsqlServer {
		if err = s.PgsqlSrv.Initialize(); err != nil {
			return err
		}
	}

	return err
}

// Start starts the immudb server
// Loads and starts the System DB, default db and user db
func (s *ImmuServer) Start() (err error) {
	s.mux.Lock()
	s.pgsqlMux.Lock()

	startedAt = time.Now()

	if s.Options.MetricsServer {
		if err := s.setUpMetricsServer(); err != nil {
			return err
		}
		defer func() {
			if err := s.metricsServer.Close(); err != nil {
				s.Logger.Errorf("Failed to shutdown metric server: %s", err)
			}
		}()
	}

	s.installShutdownHandler()

	go func() {
		if err := s.GrpcServer.Serve(s.Listener); err != nil {
			s.mux.Unlock()
			log.Fatal(err)
		}
	}()

	if err = s.SessManager.StartSessionsGuard(); err != nil {
		log.Fatal(err)
	}
	s.Logger.Infof("sessions guard started")

	if s.Options.PgsqlServer {
		go func() {
			s.Logger.Infof("pgsql server is running at port %d", s.Options.PgsqlServerPort)
			if err := s.PgsqlSrv.Serve(); err != nil {
				s.pgsqlMux.Unlock()
				log.Fatal(err)
			}
		}()
	}

	if s.Options.WebServer {
		if err := s.setUpWebServer(); err != nil {
			log.Fatal(fmt.Sprintf("Failed to setup web API/console server: %v", err))
		}
		defer func() {
			if err := s.webServer.Close(); err != nil {
				s.Logger.Errorf("Failed to shutdown web API/console server: %s", err)
			}
		}()
	}

	go s.printUsageCallToAction()

	s.mux.Unlock()
	s.pgsqlMux.Unlock()
	<-s.quit

	return err
}

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
	}
	return err
}

func (s *ImmuServer) setupPidFile() error {
	var err error
	if s.Options.Pidfile != "" {
		if s.Pid, err = NewPid(s.Options.Pidfile, s.OS); err != nil {
			return logErr(s.Logger, "Failed to write pidfile: %s", err)
		}
	}
	return err
}

func (s *ImmuServer) setUpMetricsServer() error {
	s.metricsServer = StartMetrics(
		1*time.Minute,
		s.Options.MetricsBind(),
		s.Logger,
		s.metricFuncServerUptimeCounter,
		s.metricFuncComputeDBSizes,
		s.metricFuncComputeDBEntries,
		s.Options.PProf,
	)
	return nil
}

func (s *ImmuServer) setUpWebServer() error {
	server, err := StartWebServer(
		s.Options.WebBind(),
		s.Options.TLSConfig,
		s,
		s.Logger,
	)
	if err != nil {
		return err
	}
	s.webServer = server
	return nil
}

func (s *ImmuServer) printUsageCallToAction() {
	time.Sleep(200 * time.Millisecond)
	immuadminCLI := helper.Blue + "immuadmin" + helper.Green
	immuclientCLI := helper.Blue + "immuclient" + helper.Green
	defaultUsername := helper.Blue + auth.SysAdminUsername + helper.Green

	fmt.Fprintf(os.Stdout,
		"%sYou can now use %s and %s CLIs to login with the %s superadmin user and start using immudb.%s\n",
		helper.Green, immuadminCLI, immuclientCLI, defaultUsername, helper.Reset)

	if s.Options.Logfile != "" {
		s.Logger.Infof(
			"You can now use immuadmin and immuclient CLIs to login with the %s superadmin user and start using immudb.\n",
			auth.SysAdminUsername)
	}
}

func (s *ImmuServer) loadSystemDatabase(dataDir string, remoteStorage remotestorage.Storage, adminPassword string) error {
	if s.dbList.Length() != 0 {
		panic("loadSystemDatabase should be called before any other database loading")
	}

	dbOpts, err := s.loadDBOptions(s.Options.GetSystemAdminDBName(), false)
	if err != nil {
		return fmt.Errorf("%w: while loading '%s' database settings", err, s.Options.GetSystemAdminDBName())
	}

	systemDBRootDir := s.OS.Join(dataDir, s.Options.GetSystemAdminDBName())
	_, err = s.OS.Stat(systemDBRootDir)
	if err == nil {
		s.sysDB, err = database.OpenDB(dbOpts.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
		if err != nil {
			s.Logger.Errorf("Database '%s' was not correctly initialized.\n"+
				"Use replication to recover from external source or start without data folder.", dbOpts.Database)
			return err
		}

		if dbOpts.isReplicatorRequired() {
			err = s.startReplicationFor(s.sysDB, dbOpts)
			if err != nil {
				s.Logger.Errorf("Error starting replication for database '%s'. Reason: %v", s.sysDB.GetName(), err)
			}
		}

		return nil
	}

	if !s.OS.IsNotExist(err) {
		return err
	}

	s.sysDB, err = database.NewDB(dbOpts.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
	if err != nil {
		return err
	}

	//sys admin can have an empty array of databases as it has full access
	if !s.sysDB.IsReplica() {
		adminUsername, _, err := s.insertNewUser([]byte(auth.SysAdminUsername), []byte(adminPassword), auth.PermissionSysAdmin, "*", false, "")
		if err != nil {
			return logErr(s.Logger, "%v", err)
		}

		s.Logger.Infof("Admin user '%s' successfully created", adminUsername)
	}

	if dbOpts.isReplicatorRequired() {
		err = s.startReplicationFor(s.sysDB, dbOpts)
		if err != nil {
			s.Logger.Errorf("Error starting replication for database '%s'. Reason: %v", s.sysDB.GetName(), err)
		}
	}

	return nil
}

//loadDefaultDatabase
func (s *ImmuServer) loadDefaultDatabase(dataDir string, remoteStorage remotestorage.Storage) error {
	if s.dbList.Length() != 0 {
		panic("loadDefaultDatabase should be called right after loading systemDatabase")
	}

	dbOpts, err := s.loadDBOptions(s.Options.GetDefaultDBName(), false)
	if err != nil {
		return fmt.Errorf("%w: while loading '%s' database settings", err, s.Options.GetDefaultDBName())
	}

	defaultDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDBName())

	_, err = s.OS.Stat(defaultDbRootDir)
	if err == nil {
		db, err := database.OpenDB(dbOpts.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
		if err != nil {
			s.Logger.Errorf("Database '%s' was not correctly initialized.\n"+
				"Use replication to recover from external source or start without data folder.", dbOpts.Database)
			return err
		}

		if dbOpts.isReplicatorRequired() {
			err = s.startReplicationFor(db, dbOpts)
			if err != nil {
				s.Logger.Errorf("Error starting replication for database '%s'. Reason: %v", db.GetName(), err)
			}
		}

		s.dbList.Put(db)

		return nil
	}

	if !s.OS.IsNotExist(err) {
		return err
	}

	db, err := database.NewDB(dbOpts.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
	if err != nil {
		return err
	}

	if dbOpts.isReplicatorRequired() {
		err = s.startReplicationFor(db, dbOpts)
		if err != nil {
			s.Logger.Errorf("Error starting replication for database '%s'. Reason: %v", db.GetName(), err)
		}
	}

	s.dbList.Put(db)

	return nil
}

func (s *ImmuServer) loadUserDatabases(dataDir string, remoteStorage remotestorage.Storage) error {
	var dirs []string

	//get first level sub directories of data dir
	files, err := ioutil.ReadDir(s.Options.Dir)
	if err != nil {
		return err
	}

	for _, f := range files {
		if !f.IsDir() ||
			f.Name() == s.Options.GetSystemAdminDBName() ||
			f.Name() == s.Options.GetDefaultDBName() {
			continue
		}

		dirs = append(dirs, f.Name())
	}

	//load databases that are inside each directory
	for _, val := range dirs {
		//dbname is the directory name where it is stored
		//path iteration above stores the directories as data/db_name
		pathparts := strings.Split(val, string(filepath.Separator))
		dbname := pathparts[len(pathparts)-1]

		dbOpts, err := s.loadDBOptions(dbname, true)
		if err != nil {
			return err
		}

		if !dbOpts.Autoload.isEnabled() {
			s.Logger.Infof("Database '%s' is closed (autoload is disabled)", dbname)
			s.dbList.Put(&closedDB{name: dbname, opts: s.databaseOptionsFrom(dbOpts)})
			continue
		}

		s.logDBOptions(dbname, dbOpts)

		db, err := database.OpenDB(dbname, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
		if err != nil {
			s.Logger.Errorf("Database '%s' could not be loaded. Reason: %v", dbname, err)
			s.dbList.Put(&closedDB{name: dbname, opts: s.databaseOptionsFrom(dbOpts)})
			continue
		}

		if dbOpts.isReplicatorRequired() {
			err = s.startReplicationFor(db, dbOpts)
			if err != nil {
				s.Logger.Errorf("Error starting replication for database '%s'. Reason: %v", db.GetName(), err)
			}
		}

		s.dbList.Put(db)
	}

	return nil
}

func (s *ImmuServer) replicationInProgressFor(db string) bool {
	s.replicationMutex.Lock()
	defer s.replicationMutex.Unlock()

	_, ok := s.replicators[db]
	return ok
}

func (s *ImmuServer) startReplicationFor(db database.DB, dbOpts *dbOptions) error {
	if !dbOpts.isReplicatorRequired() {
		return ErrReplicatorNotNeeded
	}

	s.replicationMutex.Lock()
	defer s.replicationMutex.Unlock()

	replicatorOpts := replication.DefaultOptions().
		WithMasterDatabase(dbOpts.MasterDatabase).
		WithMasterAddress(dbOpts.MasterAddress).
		WithMasterPort(dbOpts.MasterPort).
		WithFollowerUsername(dbOpts.FollowerUsername).
		WithFollowerPassword(dbOpts.FollowerPassword).
		WithStreamChunkSize(s.Options.StreamChunkSize)

	f, err := replication.NewTxReplicator(db, replicatorOpts, s.Logger)
	if err != nil {
		return err
	}

	err = f.Start()
	if err != nil {
		return err
	}

	s.replicators[db.GetName()] = f

	return nil
}

func (s *ImmuServer) stopReplicationFor(db string) error {
	s.replicationMutex.Lock()
	defer s.replicationMutex.Unlock()

	replicator, ok := s.replicators[db]
	if !ok {
		return ErrReplicationNotInProgress
	}

	err := replicator.Stop()
	if err != nil {
		return err
	}

	delete(s.replicators, db)

	return nil
}

func (s *ImmuServer) stopReplication() {
	s.replicationMutex.Lock()
	defer s.replicationMutex.Unlock()

	for db, f := range s.replicators {
		err := f.Stop()
		if err != nil {
			s.Logger.Warningf("Error stopping replication for '%s'. Reason: %v", db, err)
		}
	}
}

// Stop stops the immudb server
func (s *ImmuServer) Stop() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.Logger.Infof("Stopping immudb:\n%v", s.Options)

	defer func() { s.quit <- struct{}{} }()

	if !s.Options.usingCustomListener {
		s.GrpcServer.Stop()
		defer func() { s.GrpcServer = nil }()
	}

	s.SessManager.StopSessionsGuard()

	s.stopReplication()

	return s.CloseDatabases()
}

//CloseDatabases closes all opened databases including the consinstency checker
func (s *ImmuServer) CloseDatabases() error {
	for i := 0; i < s.dbList.Length(); i++ {
		val, err := s.dbList.GetByIndex(i)
		if err == nil {
			val.Close()
		}
	}

	if s.sysDB != nil {
		s.sysDB.Close()
	}

	return nil
}

func (s *ImmuServer) updateConfigItem(key string, newOrUpdatedLine string, unchanged func(string) bool) error {
	configFilepath := s.Options.Config

	if strings.TrimSpace(configFilepath) == "" {
		return fmt.Errorf("config file does not exist")
	}

	configBytes, err := s.OS.ReadFile(configFilepath)
	if err != nil {
		return fmt.Errorf("error reading config file '%s'. Reason: %v", configFilepath, err)
	}

	configLines := strings.Split(string(configBytes), "\n")

	write := false
	for i, l := range configLines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, key+"=") || strings.HasPrefix(l, key+" =") {
			kv := strings.Split(l, "=")
			if unchanged(kv[1]) {
				return fmt.Errorf("Server config already has '%s'", newOrUpdatedLine)
			}
			configLines[i] = newOrUpdatedLine
			write = true
			break
		}
	}

	if !write {
		configLines = append(configLines, newOrUpdatedLine)
	}

	if err := s.OS.WriteFile(configFilepath, []byte(strings.Join(configLines, "\n")), 0644); err != nil {
		return err
	}

	return nil
}

// UpdateAuthConfig is DEPRECATED
func (s *ImmuServer) UpdateAuthConfig(ctx context.Context, req *schema.AuthConfig) (*empty.Empty, error) {
	return nil, ErrNotSupported
}

// UpdateMTLSConfig is DEPRECATED
func (s *ImmuServer) UpdateMTLSConfig(ctx context.Context, req *schema.MTLSConfig) (*empty.Empty, error) {
	return nil, ErrNotSupported
}

// ServerInfo returns information about the server instance.
func (s *ImmuServer) ServerInfo(ctx context.Context, req *schema.ServerInfoRequest) (*schema.ServerInfoResponse, error) {
	return &schema.ServerInfoResponse{Version: version.Version}, nil
}

// Health ...
func (s *ImmuServer) Health(ctx context.Context, _ *empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: Version.Version}, nil
}

func (s *ImmuServer) installShutdownHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		s.Logger.Infof("Caught SIGTERM")
		if err := s.Stop(); err != nil {
			s.Logger.Errorf("Shutdown error: %v", err)
		}
		s.Logger.Infof("Shutdown completed")
	}()
}

// CreateDatabase Create a new database instance
func (s *ImmuServer) CreateDatabase(ctx context.Context, req *schema.Database) (*empty.Empty, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	_, err := s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{Name: req.DatabaseName})
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// CreateDatabaseWith Create a new database instance
func (s *ImmuServer) CreateDatabaseWith(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	_, err := s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{
		Name:     req.DatabaseName,
		Settings: dbSettingsToDBNullableSettings(req),
	})
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// CreateDatabaseV2 Create a new database instance
func (s *ImmuServer) CreateDatabaseV2(ctx context.Context, req *schema.CreateDatabaseRequest) (res *schema.CreateDatabaseResponse, err error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	s.Logger.Infof("Creating database '%s'...", req.Name)

	defer func() {
		if err == nil {
			s.Logger.Infof("Database '%s' succesfully created", req.Name)
		} else {
			s.Logger.Infof("Database '%s' could not be created. Reason: %v", req.Name, err)
		}
	}()

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, ErrAuthMustBeEnabled
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	if !user.IsSysAdmin {
		return nil, fmt.Errorf("loggedin user does not have permissions for this operation")
	}

	if req.Name == s.Options.defaultDBName || req.Name == s.Options.systemAdminDBName {
		return nil, ErrReservedDatabase
	}

	req.Name = strings.ToLower(req.Name)
	if err = isValidDBName(req.Name); err != nil {
		return nil, err
	}

	s.dbListMutex.Lock()
	defer s.dbListMutex.Unlock()

	//check if database exists
	if s.dbList.GetId(req.Name) >= 0 {
		if !req.IfNotExists {
			return nil, database.ErrDatabaseAlreadyExists
		}

		dbOpts, err := s.loadDBOptions(req.Name, false)
		if err != nil {
			return nil, fmt.Errorf("%w: while loading database settings", err)
		}

		return &schema.CreateDatabaseResponse{
			Name:           req.Name,
			Settings:       dbOpts.databaseNullableSettings(),
			AlreadyExisted: true,
		}, nil
	}

	dbOpts := s.defaultDBOptions(req.Name)

	if req.Settings != nil {
		err = s.overwriteWith(dbOpts, req.Settings, false)
		if err != nil {
			return nil, err
		}
	}

	err = s.saveDBOptions(dbOpts)
	if err != nil {
		return nil, err
	}

	db, err := database.NewDB(dbOpts.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
	if err != nil {
		return nil, err
	}

	s.dbList.Put(db)
	s.multidbmode = true

	s.logDBOptions(db.GetName(), dbOpts)

	err = s.startReplicationFor(db, dbOpts)
	if err != nil && err != ErrReplicatorNotNeeded {
		return nil, fmt.Errorf("%w: while starting replication", err)
	}

	return &schema.CreateDatabaseResponse{
		Name:     req.Name,
		Settings: dbOpts.databaseNullableSettings(),
	}, nil
}

func (s *ImmuServer) LoadDatabase(ctx context.Context, req *schema.LoadDatabaseRequest) (res *schema.LoadDatabaseResponse, err error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	s.Logger.Infof("Loadinig database '%s'...", req.Database)

	defer func() {
		if err == nil {
			s.Logger.Infof("Database '%s' succesfully loaded", req.Database)
		} else {
			s.Logger.Infof("Database '%s' could not be loaded. Reason: %v", req.Database, err)
		}
	}()

	if req.Database == s.Options.defaultDBName || req.Database == s.Options.systemAdminDBName {
		return nil, ErrReservedDatabase
	}

	if !s.Options.GetAuth() {
		return nil, ErrAuthMustBeEnabled
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	//if the requesting user has admin permission on this database
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.Database, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("the database '%s' does not exist or you do not have admin permission on this database", req.Database)
	}

	s.dbListMutex.Lock()
	defer s.dbListMutex.Unlock()

	db, err := s.dbList.GetByName(req.Database)
	if err != nil {
		return nil, err
	}

	if !db.IsClosed() {
		return nil, ErrDatabaseAlreadyLoaded
	}

	dbOpts, err := s.loadDBOptions(req.Database, false)
	if err == store.ErrKeyNotFound {
		return nil, fmt.Errorf("%w: while opening database '%s'", database.ErrDatabaseNotExists, req.Database)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: while loading database settings", err)
	}

	db, err = database.OpenDB(req.Database, s.multidbHandler(), s.databaseOptionsFrom(dbOpts), s.Logger)
	if err != nil {
		return nil, fmt.Errorf("%w: while opening database", err)
	}

	s.dbList.Put(db)

	if dbOpts.isReplicatorRequired() {
		err = s.startReplicationFor(db, dbOpts)
		if err != nil && err != ErrReplicatorNotNeeded {
			return nil, fmt.Errorf("%w: while starting replication", err)
		}
	}

	return &schema.LoadDatabaseResponse{
		Database: req.Database,
	}, nil
}

func (s *ImmuServer) UnloadDatabase(ctx context.Context, req *schema.UnloadDatabaseRequest) (res *schema.UnloadDatabaseResponse, err error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	s.Logger.Infof("Unloading database '%s'...", req.Database)

	defer func() {
		if err == nil {
			s.Logger.Infof("Database '%s' succesfully unloaded", req.Database)
		} else {
			s.Logger.Infof("Database '%s' could not be unloaded. Reason: %v", req.Database, err)
		}
	}()

	if req.Database == s.Options.defaultDBName || req.Database == s.Options.systemAdminDBName {
		return nil, ErrReservedDatabase
	}

	if !s.Options.GetAuth() {
		return nil, ErrAuthMustBeEnabled
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	//if the requesting user has admin permission on this database
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.Database, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("the database '%s' does not exist or you do not have admin permission on this database", req.Database)
	}

	s.dbListMutex.Lock()
	defer s.dbListMutex.Unlock()

	db, err := s.dbList.GetByName(req.Database)
	if err != nil {
		return nil, err
	}

	if db.IsClosed() {
		return nil, store.ErrAlreadyClosed
	}

	dbOpts, err := s.loadDBOptions(req.Database, false)
	if err != nil {
		return nil, fmt.Errorf("%w: while reading database settings", err)
	}

	if dbOpts.isReplicatorRequired() {
		err = s.stopReplicationFor(req.Database)
		if err != nil && err != ErrReplicationNotInProgress {
			return nil, fmt.Errorf("%w: while stopping replication", err)
		}
	}

	err = db.Close()
	if err != nil {
		return nil, err
	}

	return &schema.UnloadDatabaseResponse{
		Database: req.Database,
	}, nil
}

func (s *ImmuServer) DeleteDatabase(ctx context.Context, req *schema.DeleteDatabaseRequest) (res *schema.DeleteDatabaseResponse, err error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	s.Logger.Infof("Deleting database '%s'...", req.Database)

	defer func() {
		if err == nil {
			s.Logger.Infof("Database '%s' succesfully deleted", req.Database)
		} else {
			s.Logger.Infof("Database '%s' could not be deleted. Reason: %v", req.Database, err)
		}
	}()

	if !s.Options.GetAuth() {
		return nil, ErrAuthMustBeEnabled
	}

	if req.Database == s.Options.defaultDBName || req.Database == s.Options.systemAdminDBName {
		return nil, ErrReservedDatabase
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	//if the requesting user has admin permission on this database
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.Database, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("the database '%s' does not exist or you do not have admin permission on this database", req.Database)
	}

	s.dbListMutex.Lock()
	defer s.dbListMutex.Unlock()

	db, err := s.dbList.Delete(req.Database)
	if err != nil {
		return nil, err
	}

	err = s.deleteDBOptionsFor(req.Database)
	if err != nil {
		return nil, err
	}

	err = os.RemoveAll(db.Path())
	if err != nil {
		return nil, err
	}

	return &schema.DeleteDatabaseResponse{
		Database: req.Database,
	}, nil
}

// UpdateDatabase Updates database settings
func (s *ImmuServer) UpdateDatabase(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	_, err := s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
		Database: req.DatabaseName,
		Settings: dbSettingsToDBNullableSettings(req),
	})
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// UpdateDatabaseV2 Updates database settings
func (s *ImmuServer) UpdateDatabaseV2(ctx context.Context, req *schema.UpdateDatabaseRequest) (res *schema.UpdateDatabaseResponse, err error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	s.Logger.Infof("Updating database settings for '%s'...", req.Database)

	defer func() {
		if err == nil {
			s.Logger.Infof("Database '%s' succesfully updated", req.Database)
		} else {
			s.Logger.Infof("Database '%s' could not be updated. Reason: %v", req.Database, err)
		}
	}()

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, ErrAuthMustBeEnabled
	}

	if req.Database == s.Options.defaultDBName || req.Database == s.Options.systemAdminDBName {
		return nil, ErrReservedDatabase
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	//if the requesting user has admin permission on this database
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.Database, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("the database '%s' does not exist or you do not have admin permission on this database", req.Database)
	}

	s.dbListMutex.Lock()
	defer s.dbListMutex.Unlock()

	dbOpts, err := s.loadDBOptions(req.Database, false)
	if err == store.ErrKeyNotFound {
		return nil, database.ErrDatabaseNotExists
	}
	if err != nil {
		return nil, fmt.Errorf("%w: while loading database settings", err)
	}

	db, err := s.dbList.GetByName(req.Database)
	if err != nil {
		return nil, err
	}

	if req.Settings.ReplicationSettings != nil && !db.IsClosed() {
		err = s.stopReplicationFor(req.Database)
		if err != nil && err != ErrReplicationNotInProgress {
			return nil, fmt.Errorf("%w: while stopping replication", err)
		}
	}

	err = s.overwriteWith(dbOpts, req.Settings, true)
	if err != nil {
		return nil, err
	}

	dbOpts.UpdatedBy = user.Username

	err = s.saveDBOptions(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: while saving updated settings", err)
	}

	s.logDBOptions(db.GetName(), dbOpts)

	if !db.IsClosed() {
		db.AsReplica(dbOpts.Replica)
	}

	if req.Settings.ReplicationSettings != nil && !db.IsClosed() {
		err = s.startReplicationFor(db, dbOpts)
		if err != nil && err != ErrReplicatorNotNeeded {
			return nil, fmt.Errorf("%w: while staring replication", err)
		}
	}

	return &schema.UpdateDatabaseResponse{
		Database: req.Database,
		Settings: dbOpts.databaseNullableSettings(),
	}, nil
}

func (s *ImmuServer) GetDatabaseSettings(ctx context.Context, _ *empty.Empty) (*schema.DatabaseSettings, error) {
	res, err := s.GetDatabaseSettingsV2(ctx, &schema.DatabaseSettingsRequest{})
	if err != nil {
		return nil, err
	}

	ret := &schema.DatabaseSettings{
		DatabaseName: res.Database,
	}

	if res.Settings.ReplicationSettings != nil {
		if res.Settings.ReplicationSettings.Replica != nil {
			ret.Replica = res.Settings.ReplicationSettings.Replica.Value
		}
		if res.Settings.ReplicationSettings.MasterDatabase != nil {
			ret.MasterDatabase = res.Settings.ReplicationSettings.MasterDatabase.Value
		}
		if res.Settings.ReplicationSettings.MasterAddress != nil {
			ret.MasterAddress = res.Settings.ReplicationSettings.MasterAddress.Value
		}
		if res.Settings.ReplicationSettings.MasterPort != nil {
			ret.MasterPort = res.Settings.ReplicationSettings.MasterPort.Value
		}
		if res.Settings.ReplicationSettings.FollowerUsername != nil {
			ret.FollowerUsername = res.Settings.ReplicationSettings.FollowerUsername.Value
		}
		if res.Settings.ReplicationSettings.FollowerPassword != nil {
			ret.FollowerPassword = res.Settings.ReplicationSettings.FollowerPassword.Value
		}
	}

	if res.Settings.FileSize != nil {
		ret.FileSize = res.Settings.FileSize.Value
	}
	if res.Settings.MaxKeyLen != nil {
		ret.MaxKeyLen = res.Settings.MaxKeyLen.Value
	}
	if res.Settings.MaxValueLen != nil {
		ret.MaxValueLen = res.Settings.MaxValueLen.Value
	}
	if res.Settings.MaxTxEntries != nil {
		ret.MaxTxEntries = res.Settings.MaxTxEntries.Value
	}
	if res.Settings.ExcludeCommitTime != nil {
		ret.ExcludeCommitTime = res.Settings.ExcludeCommitTime.Value
	}

	return ret, nil
}

func (s *ImmuServer) GetDatabaseSettingsV2(ctx context.Context, _ *schema.DatabaseSettingsRequest) (*schema.DatabaseSettingsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DatabaseSettings")
	if err != nil {
		return nil, err
	}

	dbOpts, err := s.loadDBOptions(db.GetName(), false)
	if err != nil {
		return nil, err
	}

	return &schema.DatabaseSettingsResponse{
		Database: db.GetName(),
		Settings: dbOpts.databaseNullableSettings(),
	}, nil
}

//DatabaseList returns a list of databases based on the requesting user permissions
func (s *ImmuServer) DatabaseList(ctx context.Context, _ *empty.Empty) (*schema.DatabaseListResponse, error) {
	dbsWithSettings, err := s.DatabaseListV2(ctx, &schema.DatabaseListRequestV2{})
	if err != nil {
		return nil, err
	}

	resp := &schema.DatabaseListResponse{}

	for _, db := range dbsWithSettings.Databases {
		resp.Databases = append(resp.Databases, &schema.Database{DatabaseName: db.Name})
	}

	return resp, nil
}

//DatabaseList returns a list of databases based on the requesting user permissions
func (s *ImmuServer) DatabaseListV2(ctx context.Context, req *schema.DatabaseListRequestV2) (*schema.DatabaseListResponseV2, error) {
	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, loggedInuser, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("please login")
	}

	resp := &schema.DatabaseListResponseV2{}

	if loggedInuser.IsSysAdmin || s.Options.GetMaintenance() {
		for i := 0; i < s.dbList.Length(); i++ {
			db, err := s.dbList.GetByIndex(i)
			if err == database.ErrDatabaseNotExists {
				continue
			}
			if err != nil {
				return nil, err
			}

			dbOpts, err := s.loadDBOptions(db.GetName(), false)
			if err != nil {
				return nil, err
			}

			dbWithSettings := &schema.DatabaseWithSettings{
				Name:     db.GetName(),
				Settings: dbOpts.databaseNullableSettings(),
				Loaded:   !db.IsClosed(),
			}

			resp.Databases = append(resp.Databases, dbWithSettings)
		}
	} else {
		for _, perm := range loggedInuser.Permissions {
			db, err := s.dbList.GetByName(perm.Database)
			if err == database.ErrDatabaseNotExists {
				continue
			}
			if err != nil {
				return nil, err
			}

			dbOpts, err := s.loadDBOptions(perm.Database, false)
			if err != nil {
				return nil, err
			}

			dbWithSettings := &schema.DatabaseWithSettings{
				Name:     perm.Database,
				Settings: dbOpts.databaseNullableSettings(),
				Loaded:   !db.IsClosed(),
			}

			resp.Databases = append(resp.Databases, dbWithSettings)
		}
	}

	return resp, nil
}

// UseDatabase ...
func (s *ImmuServer) UseDatabase(ctx context.Context, req *schema.Database) (*schema.UseDatabaseReply, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	user := &auth.User{}
	var err error

	if s.Options.GetAuth() {
		_, user, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
				return nil, status.Error(codes.PermissionDenied, err.Error())
			}
			return nil, status.Errorf(codes.Unauthenticated, "Please login")
		}
	} else {
		if !s.Options.GetMaintenance() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}

		user.IsSysAdmin = true
		user.Username = ""
		s.addUserToLoginList(user)
	}

	dbid := sysDBIndex
	db := s.sysDB

	if req.DatabaseName != SystemDBName {
		//check if database exists
		dbid = s.dbList.GetId(req.DatabaseName)
		if dbid < 0 {
			return nil, errors.New(fmt.Sprintf("'%s' does not exist", req.DatabaseName)).WithCode(errors.CodInvalidDatabaseName)
		}

		db, err = s.dbList.GetByIndex(dbid)
		if err != nil {
			return nil, err
		}

		if db.IsClosed() {
			return nil, store.ErrAlreadyClosed
		}
	}

	//check if this user has permission on this database
	//if sysadmin allow to continue
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.DatabaseName, auth.PermissionAdmin)) &&
		(!user.HasPermission(req.DatabaseName, auth.PermissionR)) &&
		(!user.HasPermission(req.DatabaseName, auth.PermissionRW)) {

		return nil, status.Errorf(codes.PermissionDenied, "Logged in user does not have permission on this database")
	}

	token, err := auth.GenerateToken(*user, int64(dbid), s.Options.TokenExpiryTimeMin)
	if err != nil {
		return nil, err
	}

	if auth.GetAuthTypeFromContext(ctx) == auth.SessionAuth {
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		sess, err := s.SessManager.GetSession(sessionID)
		if err != nil {
			return nil, err
		}
		sess.SetDatabase(db)
	}

	return &schema.UseDatabaseReply{
		Token: token,
	}, nil
}

// getDBFromCtx checks if user (loggedin from context) has access to methodName.
// returns selected database
func (s *ImmuServer) getDBFromCtx(ctx context.Context, methodName string) (database.DB, error) {
	//if auth is disabled and there is not user created databases returns defaultdb
	if !s.Options.auth && !s.multidbmode && !s.Options.GetMaintenance() {
		db, _ := s.dbList.GetByIndex(defaultDbIndex)
		return db, nil
	}

	if s.Options.GetMaintenance() && !auth.IsMaintenanceMethod(methodName) {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	ind, usr, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if s.Options.GetMaintenance() && !s.Options.auth {
			return nil, fmt.Errorf("please select database first")
		}
		return nil, err
	}

	if ind < 0 {
		return nil, fmt.Errorf("please select a database first")
	}

	// systemdb is always read-only from external access
	if ind == sysDBIndex && !auth.IsMaintenanceMethod(methodName) {
		return nil, ErrPermissionDenied
	}

	var db database.DB

	if ind == sysDBIndex {
		db = s.sysDB
	} else {
		db, err = s.dbList.GetByIndex(ind)
		if err != nil {
			return nil, err
		}
	}

	if usr.IsSysAdmin {
		return db, nil
	}

	if ok := auth.HasPermissionForMethod(usr.WhichPermission(db.GetName()), methodName); !ok {
		return nil, ErrPermissionDenied
	}

	return db, nil
}

// isValidDBName checks if the provided database name meets the requirements
func isValidDBName(dbName string) error {
	if len(dbName) < 1 || len(dbName) > 128 {
		return fmt.Errorf("database name length outside of limits")
	}

	var hasSpecial bool

	for _, ch := range dbName {
		switch {
		case unicode.IsLower(ch):
		case unicode.IsDigit(ch):
		case unicode.IsPunct(ch) || unicode.IsSymbol(ch):
			hasSpecial = true
		default:
			return fmt.Errorf("unrecognized character in database name")
		}
	}

	if hasSpecial {
		return fmt.Errorf("punctuation marks and symbols are not allowed in database name")
	}

	return nil
}

//checkMandatoryAuth checks if auth should be madatory for immudb to start
func (s *ImmuServer) mandatoryAuth() bool {
	if s.Options.GetMaintenance() {
		return false
	}

	//check if there are user created databases, should be zero for auth to be off
	if s.dbList.Length() > 1 {
		return true
	}

	//check if there is only sysadmin on systemdb and no other user
	itemList, err := s.sysDB.Scan(&schema.ScanRequest{
		Prefix: []byte{KeyPrefixUser},
	})

	if err != nil {
		s.Logger.Errorf("error getting users: %v", err)
		return true
	}

	for _, val := range itemList.Entries {
		if len(val.Key) > 2 {
			if auth.SysAdminUsername != string(val.Key[1:]) {
				//another user detected
				return true
			}
		}
	}

	//systemdb exists but there are no other users created
	return false
}
