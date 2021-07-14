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
	"context"
	"encoding/json"
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

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/errors"

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

	adminPassword, err := auth.DecodeBase64Password(s.Options.AdminPassword)
	if err != nil {
		return logErr(s.Logger, "%v", err)
	}

	if len(adminPassword) == 0 {
		s.Logger.Errorf(ErrEmptyAdminPassword.Error())
		return ErrEmptyAdminPassword
	}

	dataDir := s.Options.Dir
	err = os.MkdirAll(dataDir, s.Options.StoreOptions.FileMode)
	if err != nil {
		return logErr(s.Logger, "Unable to create data dir: %v", err)
	}

	remoteStorage, err := s.createRemoteStorageInstance()
	if err != nil {
		return logErr(s.Logger, "Unable to open remote storage: %v", err)
	}
	s.remoteStorage = remoteStorage

	err = s.initializeRemoteStorage(remoteStorage)
	if err != nil {
		return logErr(s.Logger, "Unable to initialize remote storage: %v", err)
	}

	if err = s.loadSystemDatabase(dataDir, remoteStorage, adminPassword); err != nil {
		return logErr(s.Logger, "Unable to load system database: %v", err)
	}

	if err = s.loadDefaultDatabase(dataDir, remoteStorage); err != nil {
		return logErr(s.Logger, "Unable to load default database: %v", err)
	}

	if s.sysDB.IsReplica() {
		s.Logger.Infof("In recovery mode only '%s' and '%s' databases are loaded", SystemdbName, DefaultdbName)
	} else {
		defaultDB := s.dbList.GetByIndex(defaultDbIndex)

		dbSize, _ := defaultDB.Size()
		if dbSize <= 0 {
			s.Logger.Infof("Started with an empty database")
		}

		if err = s.loadUserDatabases(dataDir, remoteStorage); err != nil {
			return logErr(s.Logger, "Unable load databases: %v", err)
		}
	}

	s.multidbmode = s.mandatoryAuth()
	if !s.Options.GetAuth() && s.multidbmode {
		s.Logger.Infof("Authentication must be on.")
		return fmt.Errorf("auth should be on")
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
		s.listener = s.Options.listener
	} else {
		s.listener, err = net.Listen(s.Options.Network, s.Options.Bind())
		if err != nil {
			return logErr(s.Logger, "Immudb unable to listen: %v", err)
		}
	}

	systemDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDbName())
	if s.UUID, err = getOrSetUUID(dataDir, systemDbRootDir); err != nil {
		return logErr(s.Logger, "Unable to get or set uuid: %v", err)
	}
	if remoteStorage != nil {
		err := s.updateRemoteUUID(remoteStorage)
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
		uuidContext.UUIDContextSetter,
		grpc_prometheus.UnaryServerInterceptor,
		auth.ServerUnaryInterceptor,
	}
	sss := []grpc.StreamServerInterceptor{
		ErrorMapperStream, // converts errors in gRPC ones. Need to be the first
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

	s.PgsqlSrv = pgsqlsrv.New(pgsqlsrv.Port(s.Options.PgsqlServerPort), pgsqlsrv.DatabaseList(s.dbList), pgsqlsrv.SysDb(s.sysDB), pgsqlsrv.TlsConfig(s.Options.TLSConfig))
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
		if err := s.GrpcServer.Serve(s.listener); err != nil {
			s.mux.Unlock()
			log.Fatal(err)
		}
	}()

	if s.Options.PgsqlServer {
		go func() {
			s.Logger.Infof("pgsl server is running at port %d", s.Options.PgsqlServerPort)
			if err := s.PgsqlSrv.Serve(); err != nil {
				s.pgsqlMux.Unlock()
				log.Fatal(err)
			}
		}()
	}

	if s.Options.WebServer {
		if err := s.setUpWebServer(); err != nil {
			return err
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

	systemDbRootDir := s.OS.Join(dataDir, s.Options.GetSystemAdminDbName())

	// Do a copy of storeOpts to avoid modification of the original ones
	storeOpts := s.storeOptionsForDb(s.Options.GetSystemAdminDbName(), remoteStorage).
		WithSynced(true)

	op := database.DefaultOption().
		WithDbName(s.Options.GetSystemAdminDbName()).
		WithDbRootPath(dataDir).
		WithStoreOptions(storeOpts)

	_, err := s.OS.Stat(systemDbRootDir)
	if err == nil {
		s.sysDB, err = database.OpenDb(op, nil, s.Logger)
		if err == sql.ErrDatabaseDoesNotExist && s.Options.GetMaintenance() {
			// Handling case where systemdb was in recovery mode but immudb was restarted before initiating replication
			op.GetReplicationOptions().AsReplica(true)
			s.sysDB, err = database.OpenDb(op, nil, s.Logger)
		}
		if err != nil {
			s.Logger.Errorf("Database '%s' was not correctly initialized.\n"+
				"Use maintenance mode to recover from external source or start without data folder.", op.GetDbName())
			return err
		}

		return nil
	}

	if !s.OS.IsNotExist(err) {
		return err
	}

	if s.Options.GetMaintenance() && s.Options.GetAuth() {
		return ErrAuthMustBeDisabled
	}

	op.GetReplicationOptions().AsReplica(s.Options.GetMaintenance())
	s.sysDB, err = database.NewDb(op, nil, s.Logger)
	if err != nil {
		return err
	}

	if !s.Options.GetMaintenance() {
		//sys admin can have an empty array of databases as it has full access
		adminUsername, _, err := s.insertNewUser([]byte(auth.SysAdminUsername), []byte(adminPassword), auth.PermissionSysAdmin, "*", false, "")
		if err != nil {
			return logErr(s.Logger, "%v", err)
		}

		s.Logger.Infof("Admin user %s successfully created", adminUsername)
	}

	return nil
}

//loadDefaultDatabase
func (s *ImmuServer) loadDefaultDatabase(dataDir string, remoteStorage remotestorage.Storage) error {
	if s.dbList.Length() != 0 {
		panic("loadDefaultDatabase should be called right after loading systemDatabase")
	}

	defaultDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDbName())

	op := database.DefaultOption().
		WithDbName(s.Options.GetDefaultDbName()).
		WithDbRootPath(dataDir).
		WithStoreOptions(s.storeOptionsForDb(s.Options.GetDefaultDbName(), remoteStorage))

	_, err := s.OS.Stat(defaultDbRootDir)
	if err == nil {
		db, err := database.OpenDb(op, s.sysDB, s.Logger)
		if err == sql.ErrDatabaseDoesNotExist && s.Options.GetMaintenance() {
			// Handling case where defaultdb was in recovery mode but immudb was restarted before initiating replication
			op.GetReplicationOptions().AsReplica(true)
			db, err = database.OpenDb(op, s.sysDB, s.Logger)
		}
		if err != nil {
			s.Logger.Errorf("Database '%s' was not correctly initialized.\n"+
				"Use maintenance mode to recover from external source or start without data folder.", op.GetDbName())
			return err
		}

		s.dbList.Append(db)
	}

	if !s.OS.IsNotExist(err) {
		return err
	}

	op.GetReplicationOptions().AsReplica(s.Options.GetMaintenance())
	db, err := database.NewDb(op, s.sysDB, s.Logger)
	if err != nil {
		return err
	}

	s.dbList.Append(db)

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
			f.Name() == s.Options.GetSystemAdminDbName() ||
			f.Name() == s.Options.GetDefaultDbName() {
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

		settings, err := s.loadSettings(dbname)
		if err != nil {
			return err
		}

		replicationOpts := &database.ReplicationOptions{
			Replica:     settings.Replica,
			SrcDatabase: settings.Database,
			SrcAddress:  settings.SrcAddress,
			SrcPort:     settings.SrcPort,
			FollowerUsr: settings.FollowerUsr,
			FollowerPwd: settings.FollowerPwd,
		}

		op := database.DefaultOption().
			WithDbName(dbname).
			WithDbRootPath(dataDir).
			WithStoreOptions(s.storeOptionsForDb(dbname, remoteStorage)).
			WithReplicationOptions(replicationOpts)

		db, err := database.OpenDb(op, s.sysDB, s.Logger)
		if err != nil {
			return err
		}

		s.dbList.Append(db)
	}

	return nil
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

	return s.CloseDatabases()
}

//CloseDatabases closes all opened databases including the consinstency checker
func (s *ImmuServer) CloseDatabases() error {
	for i := 0; i < s.dbList.Length(); i++ {
		val := s.dbList.GetByIndex(int64(i))
		val.Close()
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
		return fmt.Errorf("error reading config file %s: %v", configFilepath, err)
	}

	configLines := strings.Split(string(configBytes), "\n")

	write := false
	for i, l := range configLines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, key+"=") || strings.HasPrefix(l, key+" =") {
			kv := strings.Split(l, "=")
			if unchanged(kv[1]) {
				return fmt.Errorf("Server config already has %s", newOrUpdatedLine)
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

// Health ...
func (s *ImmuServer) Health(ctx context.Context, _ *empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: fmt.Sprintf("%s", Version.Version)}, nil
}

func (s *ImmuServer) installShutdownHandler() {
	c := make(chan os.Signal)
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
func (s *ImmuServer) CreateDatabase(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	s.Logger.Debugf("createdatabase")

	if req == nil {
		return nil, ErrIllegalArguments
	}

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	if !user.IsSysAdmin {
		return nil, fmt.Errorf("Logged In user does not have permissions for this operation")
	}

	if req.DatabaseName == SystemdbName {
		return nil, fmt.Errorf("this database name is reserved")
	}

	if strings.ToLower(req.DatabaseName) != req.DatabaseName {
		return nil, fmt.Errorf("provide a lowercase database name")
	}

	req.DatabaseName = strings.ToLower(req.DatabaseName)
	if err = isValidDBName(req.DatabaseName); err != nil {
		return nil, err
	}

	//check if database exists
	if s.dbList.GetId(req.GetDatabaseName()) >= 0 {
		return nil, fmt.Errorf("database %s already exists", req.GetDatabaseName())
	}

	settings := &dbSettings{
		Database:    req.DatabaseName,
		Replica:     req.Replica,
		SrcDatabase: req.SrcDatabase,
		SrcAddress:  req.SrcAddress,
		SrcPort:     int(req.SrcPort),
		FollowerUsr: req.FollowerUsr,
		FollowerPwd: req.FollowerPwd,
		CreatedBy:   user.Username,
		CreatedAt:   time.Now(),
	}

	err = s.saveSettings(settings)
	if err != nil {
		return nil, err
	}

	replicationOpts := &database.ReplicationOptions{
		Replica:     settings.Replica,
		SrcDatabase: settings.Database,
		SrcAddress:  settings.SrcAddress,
		SrcPort:     settings.SrcPort,
		FollowerUsr: settings.FollowerUsr,
		FollowerPwd: settings.FollowerPwd,
	}

	dataDir := s.Options.Dir

	op := database.DefaultOption().
		WithDbName(req.DatabaseName).
		WithDbRootPath(dataDir).
		WithStoreOptions(s.storeOptionsForDb(req.DatabaseName, s.remoteStorage)).
		WithReplicationOptions(replicationOpts)

	db, err := database.NewDb(op, s.sysDB, s.Logger)
	if err != nil {
		s.Logger.Errorf(err.Error())
		return nil, err
	}

	s.dbList.Append(db)
	s.multidbmode = true

	return &empty.Empty{}, nil
}

// UpdateDatabase Updates database settings
func (s *ImmuServer) UpdateDatabase(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	s.Logger.Debugf("updatedatabase")

	if req == nil {
		return nil, ErrIllegalArguments
	}

	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	if req.DatabaseName == DefaultdbName {
		return nil, fmt.Errorf("this database name is reserved")
	}

	db, err := s.dbList.GetByName(req.DatabaseName)
	if err != nil {
		return nil, err
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get loggedin user data")
	}

	//if the requesting user has admin permission on this database
	if (!user.IsSysAdmin) &&
		(!user.HasPermission(req.DatabaseName, auth.PermissionAdmin)) {
		return nil, fmt.Errorf("you do not have permission on this database")
	}

	settings, err := s.loadSettings(req.DatabaseName)
	if err != nil {
		return nil, ErrEmptyAdminPassword
	}

	settings.Replica = req.Replica
	settings.SrcDatabase = req.SrcDatabase
	settings.SrcAddress = req.SrcAddress
	settings.SrcPort = int(req.SrcPort)
	settings.FollowerUsr = req.FollowerUsr
	settings.FollowerPwd = req.FollowerPwd
	settings.UpdatedBy = user.Username
	settings.UpdatedAt = time.Now()

	err = s.saveSettings(settings)
	if err != nil {
		return nil, err
	}

	db.UpdateReplicationOptions(&database.ReplicationOptions{
		Replica:     settings.Replica,
		SrcDatabase: settings.Database,
		SrcAddress:  settings.SrcAddress,
		SrcPort:     settings.SrcPort,
		FollowerUsr: settings.FollowerUsr,
		FollowerPwd: settings.FollowerPwd,
	})

	return &empty.Empty{}, nil
}

//DatabaseList returns a list of databases based on the requesting user permissins
func (s *ImmuServer) DatabaseList(ctx context.Context, _ *empty.Empty) (*schema.DatabaseListResponse, error) {
	s.Logger.Debugf("DatabaseList")
	loggedInuser := &auth.User{}
	var err error

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, loggedInuser, err = s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("please login")
	}

	dbList := &schema.DatabaseListResponse{}

	if loggedInuser.IsSysAdmin || s.Options.GetMaintenance() {
		for i := 0; i < s.dbList.Length(); i++ {
			val := s.dbList.GetByIndex(int64(i))
			if val.GetOptions().GetDbName() == SystemdbName {
				//do not put sysemdb in the list
				continue
			}
			db := &schema.Database{
				DatabaseName: val.GetOptions().GetDbName(),
			}
			dbList.Databases = append(dbList.Databases, db)
		}
	} else {
		for _, val := range loggedInuser.Permissions {
			db := &schema.Database{
				DatabaseName: val.Database,
			}
			dbList.Databases = append(dbList.Databases, db)
		}
	}

	return dbList, nil
}

// UseDatabase ...
func (s *ImmuServer) UseDatabase(ctx context.Context, req *schema.Database) (*schema.UseDatabaseReply, error) {
	s.Logger.Debugf("UseDatabase %+v", req)

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

	if req.DatabaseName != SystemdbName {
		//check if database exists
		dbid = s.dbList.GetId(req.DatabaseName)
		if dbid < 0 {
			return nil, status.Errorf(codes.NotFound, fmt.Sprintf("%s does not exist", req.DatabaseName))
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

	token, err := auth.GenerateToken(*user, dbid, s.Options.TokenExpiryTimeMin)
	if err != nil {
		return nil, err
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
		return s.dbList.GetByIndex(defaultDbIndex), nil
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
		return nil, ErrNotLoggedIn
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
		db = s.dbList.GetByIndex(ind)
	}

	if usr.IsSysAdmin {
		return db, nil
	}

	if ok := auth.HasPermissionForMethod(usr.WhichPermission(s.dbList.GetByIndex(ind).GetOptions().GetDbName()), methodName); !ok {
		return nil, ErrPermissionDenied
	}

	return db, nil
}

type dbSettings struct {
	Database    string    `json:"database"`
	Replica     bool      `json:"replica"`
	SrcDatabase string    `json:"srcDatabase"`
	SrcAddress  string    `json:"srcAddress"`
	SrcPort     int       `json:"srcPort"`
	FollowerUsr string    `json:"followerUsr"`
	FollowerPwd string    `json:"followerPwd"`
	CreatedBy   string    `json:"createdBy"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedBy   string    `json:"updatedBy"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func (s *ImmuServer) loadSettings(database string) (*dbSettings, error) {
	settingsKey := make([]byte, 1+len(database))
	settingsKey[0] = KeyPrefixDBSettings
	copy(settingsKey[1:], []byte(database))

	e, err := s.sysDB.Get(&schema.KeyRequest{Key: settingsKey})
	if err != nil {
		return nil, err
	}

	var settings *dbSettings

	err = json.Unmarshal(e.Value, &settings)
	if err != nil {
		return nil, err
	}

	return settings, nil
}

func (s *ImmuServer) saveSettings(settings *dbSettings) error {
	settingsData, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	settingsKey := make([]byte, 1+len(settings.Database))
	settingsKey[0] = KeyPrefixDBSettings
	copy(settingsKey[1:], []byte(settings.Database))

	settingsKV := &schema.KeyValue{Key: settingsKey, Value: settingsData}
	_, err = s.sysDB.Set(&schema.SetRequest{KVs: []*schema.KeyValue{settingsKV}})

	return err
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
	for i := 0; i < s.dbList.Length(); i++ {
		val := s.dbList.GetByIndex(int64(i))
		if (val.GetOptions().GetDbName() != s.Options.defaultDbName) &&
			(val.GetOptions().GetDbName() != s.Options.systemAdminDbName) {
			return true
		}
	}

	//check if there is only default database
	if (s.dbList.Length() == 1) && (s.dbList.GetByIndex(defaultDbIndex).GetOptions().GetDbName() == s.Options.defaultDbName) {
		return false
	}

	if s.sysDB != nil {
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

	return true
}
