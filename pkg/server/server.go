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
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/codenotary/immudb/embedded/remotestorage"
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

	if err = s.loadUserDatabases(dataDir, remoteStorage); err != nil {
		return logErr(s.Logger, "Unable to load databases: %v", err)
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

	dbSize, _ := s.dbList.GetByIndex(DefaultDbIndex).Size()
	if dbSize <= 0 {
		s.Logger.Infof("Started with an empty database")
	}

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

	s.PgsqlSrv = pgsqlsrv.New(pgsqlsrv.Port(s.Options.PgsqlServerPort), pgsqlsrv.DatabaseList(s.dbList), pgsqlsrv.SysDb(s.sysDb), pgsqlsrv.TlsConfig(s.Options.TLSConfig))
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

	go s.printUsageCallToAction()

	startedAt = time.Now()

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
		WithDbRootPath(s.Options.Dir).
		WithStoreOptions(storeOpts)

	_, sysDbErr := s.OS.Stat(systemDbRootDir)
	if s.OS.IsNotExist(sysDbErr) {
		db, err := database.NewDb(op, nil, s.Logger)
		if err != nil {
			return err
		}

		s.sysDb = db
		//sys admin can have an empty array of databases as it has full access
		adminUsername, _, err := s.insertNewUser([]byte(auth.SysAdminUsername), []byte(adminPassword), auth.PermissionSysAdmin, "*", false, "")
		if err != nil {
			return logErr(s.Logger, "%v", err)
		}

		s.Logger.Infof("Admin user %s successfully created", adminUsername)
	} else {
		db, err := database.OpenDb(op, nil, s.Logger)
		if err != nil {
			return err
		}

		s.sysDb = db
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
		WithDbRootPath(s.Options.Dir).
		WithStoreOptions(s.storeOptionsForDb(s.Options.GetDefaultDbName(), remoteStorage))

	_, defaultDbErr := s.OS.Stat(defaultDbRootDir)
	if s.OS.IsNotExist(defaultDbErr) {
		db, err := database.NewDb(op, s.sysDb, s.Logger)
		if err != nil {
			return err
		}

		s.dbList.Append(db)
	} else {
		db, err := database.OpenDb(op, s.sysDb, s.Logger)
		if err != nil {
			return err
		}

		s.dbList.Append(db)
	}

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

		op := database.DefaultOption().
			WithDbName(dbname).
			WithDbRootPath(dataDir).
			WithDbRootPath(s.Options.Dir).
			WithStoreOptions(s.storeOptionsForDb(dbname, remoteStorage))

		db, err := database.OpenDb(op, s.sysDb, s.Logger)
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

	if s.sysDb != nil {
		s.sysDb.Close()
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

// UpdateAuthConfig ...
func (s *ImmuServer) UpdateAuthConfig(ctx context.Context, req *schema.AuthConfig) (*empty.Empty, error) {
	_, err := s.getDbIndexFromCtx(ctx, "UpdateAuthConfig")
	if err != nil {
		return nil, err
	}

	e := new(empty.Empty)

	s.Options.WithAuth(req.GetKind() > 0)

	auth.AuthEnabled = s.Options.GetAuth()

	if err := s.updateConfigItem(
		"auth",
		fmt.Sprintf("auth = %t", auth.AuthEnabled),
		func(currValue string) bool {
			b, err := strconv.ParseBool(currValue)
			return err == nil && b == auth.AuthEnabled
		},
	); err != nil {
		return e, fmt.Errorf(
			"auth set to %t, but config file could not be updated: %v",
			auth.AuthEnabled, err)
	}

	return e, nil
}

// UpdateMTLSConfig ...
func (s *ImmuServer) UpdateMTLSConfig(ctx context.Context, req *schema.MTLSConfig) (*empty.Empty, error) {
	_, err := s.getDbIndexFromCtx(ctx, "UpdateMTLSConfig")
	if err != nil {
		return nil, err
	}

	e := new(empty.Empty)

	if err := s.updateConfigItem(
		"mtls",
		fmt.Sprintf("mtls = %t", req.GetEnabled()),
		func(currValue string) bool {
			b, err := strconv.ParseBool(currValue)
			return err == nil && b == req.GetEnabled()
		},
	); err != nil {
		return e, fmt.Errorf("MTLS could not be set to %t: %v", req.GetEnabled(), err)
	}

	return e, status.Errorf(
		codes.OK,
		"MTLS set to %t in server config, but server restart is required for it to take effect.",
		req.GetEnabled())
}

// Health ...
func (s *ImmuServer) Health(ctx context.Context, e *empty.Empty) (*schema.HealthResponse, error) {
	ind, _ := s.getDbIndexFromCtx(ctx, "Health")

	if ind < 0 { //probably immuclient hasn't logged in yet
		return s.dbList.GetByIndex(DefaultDbIndex).Health(e)
	}

	return s.dbList.GetByIndex(ind).Health(e)
}

// CurrentState ...
func (s *ImmuServer) CurrentState(ctx context.Context, e *empty.Empty) (*schema.ImmutableState, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "CurrentState")
	if err != nil {
		return nil, err
	}

	state, err := s.dbList.GetByIndex(ind).CurrentState()
	if err != nil {
		return nil, err
	}

	state.Db = s.dbList.GetByIndex(ind).GetOptions().GetDbName()

	if s.Options.SigningKey != "" {
		err = s.StateSigner.Sign(state)
		if err != nil {
			return nil, err
		}
	}

	return state, nil
}

// Set ...
func (s *ImmuServer) Set(ctx context.Context, kv *schema.SetRequest) (*schema.TxMetadata, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Set")

	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Set(kv)
}

// VerifiableSet ...
func (s *ImmuServer) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableSet")
	if err != nil {
		return nil, err
	}

	vtx, err := s.dbList.GetByIndex(ind).VerifiableSet(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     s.dbList.GetByIndex(ind).GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// Get ...
func (s *ImmuServer) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Get")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Get(req)
}

// VerifiableGet ...
func (s *ImmuServer) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableGet")
	if err != nil {
		return nil, err
	}

	vEntry, err := s.dbList.GetByIndex(ind).VerifiableGet(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vEntry.VerifiableTx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     s.dbList.GetByIndex(ind).GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vEntry.VerifiableTx.Signature = newState.Signature
	}

	return vEntry, nil
}

// Scan ...
func (s *ImmuServer) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Scan")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Scan(req)
}

// Count ...
func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	/*s.Logger.Debugf("count %s", prefix.Prefix)
	ind, err := s.getDbIndexFromCtx(ctx, "Count")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Count(prefix)
	*/
	return nil, errors.New("Functionality not yet supported")
}

// CountAll ...
func (s *ImmuServer) CountAll(ctx context.Context, e *empty.Empty) (*schema.EntryCount, error) {
	/*ind, err := s.getDbIndexFromCtx(ctx, "CountAll")
	s.Logger.Debugf("count all for db index %d", ind)
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).CountAll()
	*/
	return nil, errors.New("Functionality not yet supported")
}

// TxByID ...
func (s *ImmuServer) TxById(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "TxByID")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).TxByID(req)
}

// VerifiableTxByID ...
func (s *ImmuServer) VerifiableTxById(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableTxByID")
	if err != nil {
		return nil, err
	}

	vtx, err := s.dbList.GetByIndex(ind).VerifiableTxByID(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     s.dbList.GetByIndex(ind).GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// TxScan ...
func (s *ImmuServer) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "TxScan")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).TxScan(req)
}

// History ...
func (s *ImmuServer) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "History")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).History(req)
}

// SetReference ...
func (s *ImmuServer) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "SetReference")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SetReference(req)
}

// VerifibleSetReference ...
func (s *ImmuServer) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableSetReference")
	if err != nil {
		return nil, err
	}

	vtx, err := s.dbList.GetByIndex(ind).VerifiableSetReference(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     s.dbList.GetByIndex(ind).GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// ZAdd ...
func (s *ImmuServer) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxMetadata, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "ZAdd")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ZAdd(req)
}

// ZScan ...
func (s *ImmuServer) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "ZScan")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ZScan(req)
}

// VerifiableZAdd ...
func (s *ImmuServer) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableZAdd")
	if err != nil {
		return nil, err
	}

	vtx, err := s.dbList.GetByIndex(ind).VerifiableZAdd(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     s.dbList.GetByIndex(ind).GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
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
func (s *ImmuServer) CreateDatabase(ctx context.Context, newdb *schema.Database) (*empty.Empty, error) {
	s.Logger.Debugf("createdatabase")

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

	if newdb.DatabaseName == SystemdbName {
		return nil, fmt.Errorf("this database name is reserved")
	}

	if strings.ToLower(newdb.DatabaseName) != newdb.DatabaseName {
		return nil, fmt.Errorf("provide a lowercase database name")
	}

	newdb.DatabaseName = strings.ToLower(newdb.DatabaseName)
	if err = IsAllowedDbName(newdb.DatabaseName); err != nil {
		return nil, err
	}

	//check if database exists
	if s.dbList.GetId(newdb.GetDatabaseName()) >= 0 {
		return nil, fmt.Errorf("database %s already exists", newdb.GetDatabaseName())
	}

	dataDir := s.Options.Dir

	op := database.DefaultOption().
		WithDbName(newdb.DatabaseName).
		WithDbRootPath(dataDir).
		WithDbRootPath(s.Options.Dir).
		WithStoreOptions(s.storeOptionsForDb(newdb.DatabaseName, s.remoteStorage))

	db, err := database.NewDb(op, s.sysDb, s.Logger)
	if err != nil {
		s.Logger.Errorf(err.Error())
		return nil, err
	}

	s.dbList.Append(db)
	s.multidbmode = true

	return &empty.Empty{}, nil
}

//DatabaseList returns a list of databases based on the requesting user permissins
func (s *ImmuServer) DatabaseList(ctx context.Context, req *empty.Empty) (*schema.DatabaseListResponse, error) {
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
func (s *ImmuServer) UseDatabase(ctx context.Context, db *schema.Database) (*schema.UseDatabaseReply, error) {
	s.Logger.Debugf("UseDatabase %+v", db)
	user := &auth.User{}

	var err error

	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}

		_, user, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
				return nil, status.Error(
					codes.PermissionDenied, err.Error())
			}
			return nil, status.Errorf(codes.Unauthenticated, "Please login")
		}

		if db.DatabaseName == SystemdbName {
			return nil, fmt.Errorf("this database can not be selected")
		}

		//check if this user has permission on this database
		//if sysadmin allow to continue
		if (!user.IsSysAdmin) &&
			(!user.HasPermission(db.DatabaseName, auth.PermissionAdmin)) &&
			(!user.HasPermission(db.DatabaseName, auth.PermissionR)) &&
			(!user.HasPermission(db.DatabaseName, auth.PermissionRW)) {

			return nil, status.Errorf(codes.PermissionDenied,
				"Logged in user does not have permission on this database")
		}
	} else {
		user.IsSysAdmin = true
		user.Username = ""
		s.addUserToLoginList(user)
	}

	//check if database exists
	dbid := s.dbList.GetId(db.DatabaseName)
	if dbid < 0 {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("%s does not exist", db.DatabaseName))
	}

	token, err := auth.GenerateToken(*user, dbid, s.Options.TokenExpiryTimeMin)
	if err != nil {
		return nil, err
	}

	return &schema.UseDatabaseReply{
		Token: token,
	}, nil
}

func (s *ImmuServer) CleanIndex(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(ctx, "CleanIndex")
	if err != nil {
		return nil, err
	}

	err = s.dbList.GetByIndex(ind).CompactIndex()

	return &empty.Empty{}, err
}

// getDbIndexFromCtx checks if user (loggedin from context) has access to methodname.
// returns index of database
func (s *ImmuServer) getDbIndexFromCtx(ctx context.Context, methodname string) (int64, error) {
	//if auth is disabled return index zero (defaultdb) as it is the first database created/loaded
	if !s.Options.auth {
		if !s.multidbmode {
			return DefaultDbIndex, nil
		}
	}

	ind, usr, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
			return 0, status.Error(codes.PermissionDenied, err.Error())
		}
		if s.Options.GetMaintenance() {
			return 0, fmt.Errorf("please select database first")
		}
		return 0, fmt.Errorf("please login first")
	}

	if ind < 0 {
		return 0, fmt.Errorf("please select a database first")
	}

	if usr.IsSysAdmin {
		return ind, nil
	}

	if ok := auth.HasPermissionForMethod(usr.WhichPermission(s.dbList.GetByIndex(ind).GetOptions().GetDbName()), methodname); !ok {
		return 0, fmt.Errorf("you do not have permission for this operation")
	}

	return ind, nil
}

// IsAllowedDbName checks if the provided database name meets the requirements
func IsAllowedDbName(dbName string) error {
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
	if (s.dbList.Length() == 1) && (s.dbList.GetByIndex(DefaultDbIndex).GetOptions().GetDbName() == s.Options.defaultDbName) {
		return false
	}

	if s.sysDb != nil {
		//check if there is only sysadmin on systemdb and no other user
		itemList, err := s.sysDb.Scan(&schema.ScanRequest{
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
