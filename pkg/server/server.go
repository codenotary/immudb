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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
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

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/signer"

	"github.com/rs/xid"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store/sysstore"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var ErrEmptyAdminPassword = fmt.Errorf("Admin password cannot be empty")

var startedAt time.Time

var immudbTextLogo = " _                               _ _     \n" +
	"(_)                             | | |    \n" +
	" _ _ __ ___  _ __ ___  _   _  __| | |__  \n" +
	"| | '_ ` _ \\| '_ ` _ \\| | | |/ _` | '_ \\ \n" +
	"| | | | | | | | | | | | |_| | (_| | |_) |\n" +
	"|_|_| |_| |_|_| |_| |_|\\__,_|\\__,_|_.__/ \n"

// Start starts the immudb server
// Loads and starts the System DB, default db and user db
func (s *ImmuServer) Start() error {
	s.mux.Lock()

	_, err := fmt.Fprintf(os.Stdout, "%s\n%s\n\n", immudbTextLogo, s.Options)
	logErr(s.Logger, "Error printing immudb config: %v", err)

	if s.Options.Logfile != "" {
		s.Logger.Infof("\n%s\n%s\n\n", immudbTextLogo, s.Options)
	}

	dataDir := s.Options.Dir
	if err = s.loadDefaultDatabase(dataDir); err != nil {
		return logErr(s.Logger, "Unable load default database: %v", err)
	}

	adminPassword, err := auth.DecodeBase64Password(s.Options.AdminPassword)
	if err != nil {
		return logErr(s.Logger, "%v", err)
	}

	if len(adminPassword) == 0 {
		s.Logger.Errorf(ErrEmptyAdminPassword.Error())
		return ErrEmptyAdminPassword
	}

	if err = s.loadSystemDatabase(dataDir, adminPassword); err != nil {
		return logErr(s.Logger, "Unable load system database: %v", err)
	}

	if err = s.loadUserDatabases(dataDir); err != nil {
		return logErr(s.Logger, "Unable load databases: %v", err)
	}

	s.multidbmode = s.mandatoryAuth()
	if !s.Options.GetAuth() && s.multidbmode {
		s.Logger.Infof("Authentication must be on.")
		return fmt.Errorf("auth should be on")
	}

	options, err := s.setUpMTLS()
	if err != nil {
		return logErr(s.Logger, "Error setting up MTLS: %v", err)
	}

	if s.Options.SigningKey != "" {
		if signer, err := signer.NewSigner(s.Options.SigningKey); err == nil {
			s.RootSigner = NewRootSigner(signer)
		} else {
			return logErr(s.Logger, "Unable to configure the cryptographic signer: %v", err)
		}
	}

	var listener net.Listener
	if s.Options.usingCustomListener {
		s.Logger.Infof("Using custom listener")
		listener = s.Options.listener
	} else {
		listener, err = net.Listen(s.Options.Network, s.Options.Bind())
		if err != nil {
			return logErr(s.Logger, "Immudb unable to listen: %v", err)
		}
	}

	systemDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDbName())
	var uuid xid.ID
	if uuid, err = getOrSetUuid(systemDbRootDir); err != nil {
		return logErr(s.Logger, "Unable to get or set uuid: %v", err)
	}

	auth.AuthEnabled = s.Options.GetAuth()
	auth.DevMode = s.Options.DevMode
	auth.UpdateMetrics = func(ctx context.Context) { Metrics.UpdateClientMetrics(ctx) }

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

	dbSize, _ := s.dbList.GetByIndex(DefaultDbIndex).Store.DbSize()
	if dbSize <= 0 {
		s.Logger.Infof("Started with an empty database")
	}

	if err = s.setupPidFile(); err != nil {
		return err
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

	uuidContext := NewUuidContext(uuid)

	uis := []grpc.UnaryServerInterceptor{
		uuidContext.UuidContextSetter,
		grpc_prometheus.UnaryServerInterceptor,
		auth.ServerUnaryInterceptor,
	}
	sss := []grpc.StreamServerInterceptor{
		uuidContext.UuidStreamContextSetter,
		grpc_prometheus.StreamServerInterceptor,
		auth.ServerStreamInterceptor,
	}
	options = append(
		options,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(uis...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(sss...)),
		grpc.MaxRecvMsgSize(s.Options.MaxRecvMsgSize),
	)

	s.GrpcServer = grpc.NewServer(options...)
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
	grpc_prometheus.Register(s.GrpcServer)
	s.startCorruptionChecker()

	go s.printUsageCallToAction()

	startedAt = time.Now()

	go func() {
		if err := s.GrpcServer.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	s.mux.Unlock()
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
		s.Options.MetricsBind(),
		s.Logger,
		func() float64 { return float64(s.dbList.GetByIndex(DefaultDbIndex).Store.CountAll()) },
		func() float64 { return time.Since(startedAt).Hours() },
	)
	return nil
}

func (s *ImmuServer) setUpMTLS() ([]grpc.ServerOption, error) {
	if s.Options.MTLs {
		// credentials needed to communicate with client
		certificate, err := tls.LoadX509KeyPair(
			s.Options.MTLsOptions.Certificate,
			s.Options.MTLsOptions.Pkey,
		)
		if err != nil {
			return nil, logErr(s.Logger, "Failed to read server key pair: %s", err)
		}

		certPool := x509.NewCertPool()
		// Trusted store, contain the list of trusted certificates. client has to use one of this certificate to be trusted by this server
		bs, err := s.OS.ReadFile(s.Options.MTLsOptions.ClientCAs)
		if err != nil {
			return nil, logErr(s.Logger, "Failed to read client ca cert: %s", err)
		}

		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			return nil, logErr(s.Logger, "Failed to append client certs", nil)
		}

		tlsConfig := &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{certificate},
			ClientCAs:    certPool,
		}

		return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
	}
	return []grpc.ServerOption{}, nil
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

func (s *ImmuServer) loadSystemDatabase(dataDir string, adminPassword string) error {
	if s.dbList.Length() == 0 {
		panic("loadSystemDatabase should be called after loadDefaultDatabase as system database should be at index 1")
	}

	systemDbRootDir := s.OS.Join(dataDir, s.Options.GetSystemAdminDbName())

	_, sysDbErr := s.OS.Stat(systemDbRootDir)
	if s.OS.IsNotExist(sysDbErr) {
		if s.Options.GetAuth() {
			op := DefaultOption().
				WithDbName(s.Options.GetSystemAdminDbName()).
				WithDbRootPath(dataDir).
				WithCorruptionChecker(s.Options.CorruptionCheck).
				WithInMemoryStore(s.Options.GetInMemoryStore()).WithDbRootPath(s.Options.Dir)

			db, err := NewDb(op, s.Logger)
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
		}
	} else {
		op := DefaultOption().
			WithDbName(s.Options.GetSystemAdminDbName()).
			WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck).WithDbRootPath(s.Options.Dir)

		db, err := OpenDb(op, s.Logger)
		if err != nil {
			return err
		}

		s.sysDb = db
	}

	return nil
}

//loadSystemDatabase it is important that is is called before loadDatabases so that defaultdb is at index zero of the databases array
func (s *ImmuServer) loadDefaultDatabase(dataDir string) error {
	if s.dbList.Length() > 0 {
		panic("loadDefaultDatabase should be called before any other database loading")
	}

	defaultDbRootDir := s.OS.Join(dataDir, s.Options.GetDefaultDbName())

	_, defaultDbErr := s.OS.Stat(defaultDbRootDir)
	if s.OS.IsNotExist(defaultDbErr) {
		op := DefaultOption().
			WithDbName(s.Options.GetDefaultDbName()).
			WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck).
			WithInMemoryStore(s.Options.GetInMemoryStore()).WithDbRootPath(s.Options.Dir)

		db, err := NewDb(op, s.Logger)
		if err != nil {
			return err
		}

		s.databasenameToIndex[s.Options.GetDefaultDbName()] = int64(s.dbList.Length())
		s.dbList.Append(db)
	} else {
		op := DefaultOption().
			WithDbName(s.Options.GetDefaultDbName()).
			WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck).WithDbRootPath(s.Options.Dir)

		db, err := OpenDb(op, s.Logger)
		if err != nil {
			return err
		}

		s.databasenameToIndex[s.Options.GetDefaultDbName()] = int64(s.dbList.Length())
		s.dbList.Append(db)
	}

	return nil
}

func (s *ImmuServer) loadUserDatabases(dataDir string) error {
	//return early since there is nothing to load
	if s.Options.GetInMemoryStore() {
		return nil
	}

	var dirs []string

	//get first level sub directories of data dir
	err := s.OS.Walk(s.Options.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		//get only first child directories, exclude datadir, exclude systemdb dir
		if info.IsDir() &&
			(dataDir != path) &&
			!strings.Contains(path, s.Options.GetSystemAdminDbName()) &&
			!strings.Contains(path, s.Options.GetDefaultDbName()) &&
			!strings.Contains(path, "config") {
			dirs = append(dirs, path)
		}
		return nil
	})

	if err != nil {
		return err
	}

	//load databases that are inside each directory
	for _, val := range dirs {
		//dbname is the directory name where it is stored
		//path iteration above stores the directories as data/db_name
		pathparts := strings.Split(val, string(filepath.Separator))
		dbname := pathparts[len(pathparts)-1]

		op := DefaultOption().WithDbName(dbname).WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck).WithDbRootPath(s.Options.Dir)

		db, err := OpenDb(op, s.Logger)
		if err != nil {
			return err
		}

		//associate this database name to it's index in the array
		s.databasenameToIndex[dbname] = int64(s.dbList.Length())
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
	s.stopCorruptionChecker()

	if s.sysDb != nil {
		s.sysDb.Store.Close()
	}

	for i := 0; i < s.dbList.Length(); i++ {
		val := s.dbList.GetByIndex(int64(i))
		val.Store.Close()
	}

	return nil
}

func (s *ImmuServer) startCorruptionChecker() {
	if s.Options.CorruptionCheck {
		cco := CCOptions{}
		cco.singleiteration = false
		cco.iterationSleepTime = 5 * time.Second
		cco.frequencySleepTime = 500 * time.Millisecond

		s.Cc = NewCorruptionChecker(cco, s.dbList, s.Logger, randomGenerator{})

		go func() {
			s.Logger.Infof("Starting consistency-checker")
			if err := s.Cc.Start(context.Background()); err != nil {
				s.Logger.Errorf("Unable to start consistency-checker: %s", err)
			}
		}()
	}
}

//StopCorruptionChecker shutdown the corruption checkcer
func (s *ImmuServer) stopCorruptionChecker() error {
	if s.Options.CorruptionCheck {
		s.Cc.Stop()
	}
	return nil
}

// Login ...
func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	if !s.Options.auth {
		return nil, fmt.Errorf("server is running with authentication disabled, please enable authentication to login")
	}

	u, err := s.getValidatedUser(r.User, r.Password)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user name or password")
	}

	if !u.Active {
		return nil, fmt.Errorf("user is not active")
	}

	//-1 no database yet, must exec the "use" (UseDatabase) command first
	var token string
	if s.multidbmode {
		token, err = auth.GenerateToken(*u, -1)
	} else {
		token, err = auth.GenerateToken(*u, DefaultDbIndex)
	}
	if err != nil {
		return nil, err
	}

	loginResponse := &schema.LoginResponse{Token: token}
	if u.Username == auth.SysAdminUsername && string(r.GetPassword()) == auth.SysAdminPassword {
		loginResponse.Warning = []byte(auth.WarnDefaultAdminPassword)
	}

	if u.Username == auth.SysAdminUsername {
		u.IsSysAdmin = true
	}

	//add user to loggedin list
	s.addUserToLoginList(u)
	return loginResponse, nil
}

// Logout ...
func (s *ImmuServer) Logout(ctx context.Context, r *empty.Empty) (*empty.Empty, error) {
	loggedOut, err := auth.DropTokenKeysForCtx(ctx)
	if err != nil {
		return new(empty.Empty), err
	}

	if !loggedOut {
		return new(empty.Empty), status.Error(codes.Unauthenticated, "not logged in")
	}

	return new(empty.Empty), nil
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

// CurrentRoot ...
func (s *ImmuServer) CurrentRoot(ctx context.Context, e *empty.Empty) (root *schema.Root, err error) {
	var ind int64

	if ind, err = s.getDbIndexFromCtx(ctx, "CurrentRoot"); err != nil {
		return nil, err
	}

	if root, err = s.dbList.GetByIndex(ind).CurrentRoot(e); err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		return s.RootSigner.Sign(root)
	}

	return root, err
}

// Set ...
func (s *ImmuServer) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	s.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))

	ind, err := s.getDbIndexFromCtx(ctx, "Set")

	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Set(kv)
}

// SafeSet ...
func (s *ImmuServer) SafeSet(ctx context.Context, opts *schema.SafeSetOptions) (*schema.Proof, error) {
	s.Logger.Debugf("SafeSet %+v", opts)

	ind, err := s.getDbIndexFromCtx(ctx, "SafeSet")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SafeSet(opts)
}

// Get ...
func (s *ImmuServer) Get(ctx context.Context, k *schema.Key) (*schema.Item, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Get")

	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Get(k)
}

// SafeGet ...
func (s *ImmuServer) SafeGet(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("safeget %s", opts.Key)

	ind, err := s.getDbIndexFromCtx(ctx, "SafeGet")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SafeGet(opts)
}

// Scan ...
func (s *ImmuServer) Scan(ctx context.Context, opts *schema.ScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx, "Scan")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).Scan(opts)
}

// Count ...
func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	s.Logger.Debugf("count %s", prefix.Prefix)
	ind, err := s.getDbIndexFromCtx(ctx, "Count")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).Count(prefix)
}

// CountAll ...
func (s *ImmuServer) CountAll(ctx context.Context, e *empty.Empty) (*schema.ItemsCount, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "CountAll")
	s.Logger.Debugf("count all for db index %d", ind)
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).CountAll(), nil
}

// Inclusion ...
func (s *ImmuServer) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Inclusion")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).Inclusion(index)
}

// Consistency ...
func (s *ImmuServer) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "Consistency")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).Consistency(index)
}

// ByIndex ...
func (s *ImmuServer) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	s.Logger.Debugf("get by index %d ", index.Index)

	ind, err := s.getDbIndexFromCtx(ctx, "ByIndex")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ByIndex(index)
}

// BySafeIndex ...
func (s *ImmuServer) BySafeIndex(ctx context.Context, sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("get by safeIndex %d ", sio.Index)
	ind, err := s.getDbIndexFromCtx(ctx, "BySafeIndex")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).BySafeIndex(sio)
}

// History ...
func (s *ImmuServer) History(ctx context.Context, options *schema.HistoryOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("history for key %s ", string(options.Key))
	ind, err := s.getDbIndexFromCtx(ctx, "History")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).History(options)
}

// Health ...
func (s *ImmuServer) Health(ctx context.Context, e *empty.Empty) (*schema.HealthResponse, error) {
	ind, _ := s.getDbIndexFromCtx(ctx, "Health")

	if ind < 0 { //probably immuclient hasn't logged in yet
		return s.dbList.GetByIndex(DefaultDbIndex).Health(e)
	}
	return s.dbList.GetByIndex(ind).Health(e)
}

// Reference ...
func (s *ImmuServer) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	s.Logger.Debugf("reference options: %v", refOpts)
	ind, err := s.getDbIndexFromCtx(ctx, "Reference")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).Reference(refOpts)
}

// SafeReference ...
func (s *ImmuServer) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	s.Logger.Debugf("safe reference options: %v", safeRefOpts)
	ind, err := s.getDbIndexFromCtx(ctx, "SafeReference")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).SafeReference(safeRefOpts)
}

// ZAdd ...
func (s *ImmuServer) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx, "ZAdd")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).ZAdd(opts)
}

// ZScan ...
func (s *ImmuServer) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ZItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx, "ZScan")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).ZScan(opts)
}

// SafeZAdd ...
func (s *ImmuServer) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	s.Logger.Debugf("SafeZAdd %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx, "SafeZAdd")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).SafeZAdd(opts)
}

// IScan ...
func (s *ImmuServer) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	s.Logger.Debugf("iscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx, "IScan")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).IScan(opts)
}

// Dump ...
func (s *ImmuServer) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	ind, err := s.getDbIndexFromCtx(stream.Context(), "Dump")
	if err != nil {
		return err
	}
	err = s.dbList.GetByIndex(ind).Dump(in, stream)
	s.Logger.Debugf("Dump stream complete")
	return err
}

// todo(joe-dz): Enable restore when the feature is required again.
// Also, make sure that the generated files are updated
//func (s *ImmuServer) HotRestore(stream schema.ImmuService_RestoreServer) (err error) {
//	kvChan := make(chan *pb.KVList)
//	errs := make(chan error, 1)
//
//	sendLists := func() {
//		defer func() {
//			close(errs)
//			close(kvChan)
//		}()
//		for {
//			list, err := stream.Recv()
//			kvChan <- list
//			if err == io.EOF {
//				return
//			}
//			if err != nil {
//				errs <- err
//				return
//			}
//		}
//	}
//
//	go sendLists()
//
//	i, err := s.SystemAdminDb.Restore(kvChan)
//
//	ic := &schema.ItemsCount{
//		Count: i,
//	}
//	return stream.SendAndClose(ic)
//}

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

// ChangePassword ...
func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	s.Logger.Debugf("ChangePassword %+v", *r)

	if !s.Options.GetAuth() {
		return nil, fmt.Errorf("this command is available only with authentication on")
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("please login first")
	}

	if string(r.User) == auth.SysAdminUsername {
		if err = auth.ComparePasswords(user.HashedPassword, r.OldPassword); err != nil {
			return new(empty.Empty), status.Errorf(codes.PermissionDenied, "old password is incorrect")
		}
	}

	if !user.IsSysAdmin {
		if !user.HasAtLeastOnePermission(auth.PermissionAdmin) {
			return nil, fmt.Errorf("user is not system admin nor admin in any of the databases")
		}
	}

	if len(r.User) == 0 {
		return nil, fmt.Errorf("username can not be empty")
	}

	targetUser, err := s.getUser(r.User, true)
	if err != nil {
		return nil, fmt.Errorf("user %s was not found or it was not created by you", string(r.User))
	}

	//if the user is not sys admin then let's make sure the target was created from this admin
	if !user.IsSysAdmin {
		if user.Username != targetUser.CreatedBy {
			return nil, fmt.Errorf("user %s was not found or it was not created by you", string(r.User))
		}
	}

	_, err = targetUser.SetPassword(r.NewPassword)
	if err != nil {
		return nil, err
	}

	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()
	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}

	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)
	// invalidate the token for this user
	auth.DropTokenKeys(targetUser.Username)

	return new(empty.Empty), nil
}

// CreateDatabase Create a new database instance
func (s *ImmuServer) CreateDatabase(ctx context.Context, newdb *schema.Database) (*empty.Empty, error) {
	s.Logger.Debugf("createdatabase %+v", *newdb)

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

	if newdb.Databasename == SystemdbName {
		return nil, fmt.Errorf("this database name is reserved")
	}

	newdb.Databasename = strings.ToLower(newdb.Databasename)
	if err = IsAllowedDbName(newdb.Databasename); err != nil {
		return nil, err
	}

	//check if database exists
	if _, ok := s.databasenameToIndex[newdb.GetDatabasename()]; ok {
		return nil, fmt.Errorf("database %s already exists", newdb.GetDatabasename())
	}

	dataDir := s.Options.Dir

	op := DefaultOption().
		WithDbName(newdb.Databasename).
		WithDbRootPath(dataDir).
		WithCorruptionChecker(s.Options.CorruptionCheck).
		WithInMemoryStore(s.Options.GetInMemoryStore()).WithDbRootPath(s.Options.Dir)

	db, err := NewDb(op, s.Logger)
	if err != nil {
		s.Logger.Errorf(err.Error())
		return nil, err
	}

	s.databasenameToIndex[newdb.Databasename] = int64(s.dbList.Length())
	s.dbList.Append(db)
	s.multidbmode = true

	return &empty.Empty{}, nil
}

// CreateUser Creates a new user
func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*empty.Empty, error) {
	s.Logger.Debugf("CreateUser %+v", *r)

	loggedInuser := &auth.User{}
	var err error

	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}

		_, loggedInuser, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			return nil, fmt.Errorf("please login")
		}

		if len(r.User) == 0 {
			return nil, fmt.Errorf("username can not be empty")
		}

		if (len(r.Database) == 0) && s.multidbmode {
			return nil, fmt.Errorf("database name can not be empty when there are multiple databases")
		}

		if (len(r.Database) == 0) && !s.multidbmode {
			r.Database = DefaultdbName
		}

		//check if database exists
		if _, ok := s.databasenameToIndex[r.Database]; !ok {
			return nil, fmt.Errorf("database %s does not exist", r.Database)
		}

		//check permission is a known value
		if (r.Permission == auth.PermissionNone) ||
			((r.Permission > auth.PermissionRW) &&
				(r.Permission < auth.PermissionAdmin)) {
			return nil, fmt.Errorf("unrecognized permission")
		}

		//if the requesting user has admin permission on this database
		if (!loggedInuser.IsSysAdmin) &&
			(!loggedInuser.HasPermission(r.Database, auth.PermissionAdmin)) {
			return nil, fmt.Errorf("you do not have permission on this database")
		}

		//do not allow to create another system admin
		if r.Permission == auth.PermissionSysAdmin {
			return nil, fmt.Errorf("can not create another system admin")
		}
	}

	_, err = s.getUser(r.User, true)
	if err == nil {
		return nil, fmt.Errorf("user already exists")
	}

	_, _, err = s.insertNewUser(r.User, r.Password, r.GetPermission(), r.Database, true, loggedInuser.Username)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

// ListUsers returns a list of users based on the requesting user permissions
func (s *ImmuServer) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	s.Logger.Debugf("ListUsers %+v")

	loggedInuser := &auth.User{}
	var dbInd = int64(0)
	var err error
	userlist := &schema.UserList{}

	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}
		dbInd, loggedInuser, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			return nil, fmt.Errorf("please login")
		}
	}

	itemList, err := s.sysDb.Scan(&schema.ScanOptions{
		Prefix: []byte{sysstore.KeyPrefixUser},
	})
	if err != nil {
		s.Logger.Errorf("error getting users: %v", err)
		return nil, err
	}

	if loggedInuser.IsSysAdmin || s.Options.GetMaintenance() {
		// return all users, including the deactivated ones
		for i := 0; i < len(itemList.Items); i++ {
			itemList.Items[i].Key = itemList.Items[i].Key[1:]
			var user auth.User
			err = json.Unmarshal(itemList.Items[i].Value, &user)
			if err != nil {
				return nil, err
			}
			permissions := []*schema.Permission{}
			for _, val := range user.Permissions {
				permissions = append(permissions, &schema.Permission{
					Database:   val.Database,
					Permission: val.Permission,
				})
			}
			u := schema.User{
				User:        []byte(user.Username),
				Createdat:   user.CreatedAt.String(),
				Createdby:   user.CreatedBy,
				Permissions: permissions,
				Active:      user.Active,
			}
			userlist.Users = append(userlist.Users, &u)
		}
		return userlist, nil
	} else if loggedInuser.WhichPermission(s.dbList.GetByIndex(dbInd).options.dbName) == auth.PermissionAdmin {
		// for admin users return only users for the database that is has selected
		selectedDbname := s.dbList.GetByIndex(dbInd).options.dbName
		userlist := &schema.UserList{}
		for i := 0; i < len(itemList.Items); i++ {
			include := false
			itemList.Items[i].Key = itemList.Items[i].Key[1:]
			var user auth.User
			err = json.Unmarshal(itemList.Items[i].Value, &user)
			if err != nil {
				return nil, err
			}
			permissions := []*schema.Permission{}
			for _, val := range user.Permissions {
				//check if this user has any permission for this database
				//include in the reply only if it has any permission for the currently selected database
				if val.Database == selectedDbname {
					include = true
				}
				permissions = append(permissions, &schema.Permission{
					Database:   val.Database,
					Permission: val.Permission,
				})
			}
			if include {
				u := schema.User{
					User:        []byte(user.Username),
					Createdat:   user.CreatedAt.String(),
					Createdby:   user.CreatedBy,
					Permissions: permissions,
					Active:      user.Active,
				}
				userlist.Users = append(userlist.Users, &u)
			}
		}
		return userlist, nil
	} else {
		// any other permission return only its data
		userlist := &schema.UserList{}
		permissions := []*schema.Permission{}
		for _, val := range loggedInuser.Permissions {
			permissions = append(permissions, &schema.Permission{
				Database:   val.Database,
				Permission: val.Permission,
			})
		}
		u := schema.User{
			User:        []byte(loggedInuser.Username),
			Createdat:   loggedInuser.CreatedAt.String(),
			Createdby:   loggedInuser.CreatedBy,
			Permissions: permissions,
			Active:      loggedInuser.Active,
		}
		userlist.Users = append(userlist.Users, &u)
		return userlist, nil
	}
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
			if val.options.dbName == SystemdbName {
				//do not put sysemdb in the list
				continue
			}
			db := &schema.Database{
				Databasename: val.options.dbName,
			}
			dbList.Databases = append(dbList.Databases, db)
		}
	} else {
		for _, val := range loggedInuser.Permissions {
			db := &schema.Database{
				Databasename: val.Database,
			}
			dbList.Databases = append(dbList.Databases, db)
		}
	}
	return dbList, nil
}

// PrintTree ...
func (s *ImmuServer) PrintTree(ctx context.Context, r *empty.Empty) (*schema.Tree, error) {
	s.Logger.Debugf("PrintTree")
	ind, err := s.getDbIndexFromCtx(ctx, "PrintTree")
	if err != nil {
		return nil, err
	}
	return s.dbList.GetByIndex(ind).PrintTree(), nil
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
		if db.Databasename == SystemdbName {
			return nil, fmt.Errorf("this database can not be selected")
		}
		//check if this user has permission on this database
		//if sysadmin allow to continue
		if (!user.IsSysAdmin) &&
			(!user.HasPermission(db.Databasename, auth.PermissionAdmin)) &&
			(!user.HasPermission(db.Databasename, auth.PermissionR)) &&
			(!user.HasPermission(db.Databasename, auth.PermissionRW)) {

			return nil, status.Errorf(codes.PermissionDenied,
				"Logged in user does not have permission on this database")
		}
	} else {
		user.IsSysAdmin = true
		user.Username = ""
		s.addUserToLoginList(user)
	}
	//check if database exists
	ind, ok := s.databasenameToIndex[db.Databasename]
	if !ok {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("%s does not exist", db.Databasename))
	}
	token, err := auth.GenerateToken(*user, ind)
	if err != nil {
		return nil, err
	}
	return &schema.UseDatabaseReply{
		Token: token,
	}, nil
}

//ChangePermission grant or revoke user permissions on databases
func (s *ImmuServer) ChangePermission(ctx context.Context, r *schema.ChangePermissionRequest) (*empty.Empty, error) {
	s.Logger.Debugf("ChangePermission %+v", r)

	if r.Database == SystemdbName {
		return nil, fmt.Errorf("this database can not be assigned")
	}
	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}
	}
	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Please login")
	}

	//sanitize input
	{
		if len(r.Username) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "username can not be empty")
		}
		if len(r.Database) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "database can not be empty")
		}
		if (r.Action != schema.PermissionAction_GRANT) &&
			(r.Action != schema.PermissionAction_REVOKE) {
			return nil, status.Errorf(codes.InvalidArgument, "action not recognized")
		}
		if (r.Permission == auth.PermissionNone) ||
			((r.Permission > auth.PermissionRW) &&
				(r.Permission < auth.PermissionAdmin)) {
			return nil, status.Errorf(codes.InvalidArgument, "unrecognized permission")
		}
	}

	//do not allow to change own permissions, user can lock itsself out
	if r.Username == user.Username {
		return nil, status.Errorf(codes.InvalidArgument, "changing you own permissions is not allowed")
	}

	//check if user exists
	targetUser, err := s.getUser([]byte(r.Username), true)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user %s not found", string(r.Username))
	}
	//target user should be active
	if !targetUser.Active {
		return nil, status.Errorf(codes.FailedPrecondition, "user %s is not active", string(r.Username))
	}

	//check if requesting user has permission on this database
	if !user.IsSysAdmin {
		if !user.HasPermission(r.Database, auth.PermissionAdmin) {
			return nil, status.Errorf(codes.PermissionDenied, "you do not have permission on this database")
		}
	}

	if r.Action == schema.PermissionAction_REVOKE {
		targetUser.RevokePermission(r.Database)
	} else {
		targetUser.GrantPermission(r.Database, r.Permission)
	}
	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()

	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}
	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)

	return new(empty.Empty), nil
}

//SetActiveUser activate or deactivate a user
func (s *ImmuServer) SetActiveUser(ctx context.Context, r *schema.SetActiveUserRequest) (*empty.Empty, error) {
	s.Logger.Debugf("SetActiveUser %+v", *r)
	if len(r.Username) == 0 {
		return nil, fmt.Errorf("username can not be empty")
	}
	user := &auth.User{}
	var err error
	if !s.Options.GetMaintenance() {
		if !s.Options.GetAuth() {
			return nil, fmt.Errorf("this command is available only with authentication on")
		}
		_, user, err = s.getLoggedInUserdataFromCtx(ctx)
		if err != nil {
			return nil, fmt.Errorf("please login first")
		}
		if !user.IsSysAdmin {
			if !user.HasAtLeastOnePermission(auth.PermissionAdmin) {
				return nil, fmt.Errorf("user is not system admin nor admin in any of the databases")
			}
		}
		if r.Username == user.Username {
			return nil, fmt.Errorf("changing your own status is not allowed")
		}
	}

	targetUser, err := s.getUser([]byte(r.Username), true)
	if err != nil {
		return nil, fmt.Errorf("user %s not found", r.Username)
	}
	if !s.Options.GetMaintenance() {
		//if the user is not sys admin then let's make sure the target was created from this admin
		if !user.IsSysAdmin {
			if user.Username != targetUser.CreatedBy {
				return nil, fmt.Errorf("%s was not created by you", r.Username)
			}
		}
	}
	targetUser.Active = r.Active
	targetUser.CreatedBy = user.Username
	targetUser.CreatedAt = time.Now()
	if err := s.saveUser(targetUser); err != nil {
		return nil, err
	}
	//remove user from loggedin users
	s.removeUserFromLoginList(targetUser.Username)
	return new(empty.Empty), nil
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

	if ok := auth.HasPermissionForMethod(usr.WhichPermission(s.dbList.GetByIndex(ind).options.dbName), methodname); !ok {
		return 0, fmt.Errorf("you do not have permission for this operation")
	}
	return ind, nil
}

func (s *ImmuServer) getLoggedInUserdataFromCtx(ctx context.Context) (int64, *auth.User, error) {
	jsUser, err := auth.GetLoggedInUser(ctx)
	if err != nil {
		if strings.HasPrefix(fmt.Sprintf("%s", err), "token has expired") {
			return -1, nil, err
		}
		return -1, nil, fmt.Errorf("could not get userdata from token")
	}
	u, err := s.getLoggedInUserDataFromUsername(jsUser.Username)
	return jsUser.DatabaseIndex, u, err
}

func (s *ImmuServer) getLoggedInUserDataFromUsername(username string) (*auth.User, error) {
	userdata, ok := s.userdata.Userdata[username]
	if !ok {
		return nil, fmt.Errorf("logged in user data not found")
	}
	return userdata, nil
}

// insertNewUser inserts a new user to the system database and returns username and plain text password
// A new password is generated automatically if passed parameter is empty
// If enforceStrongAuth is true it checks if username and password meet security criteria
func (s *ImmuServer) insertNewUser(username []byte, plainPassword []byte, permission uint32, database string, enforceStrongAuth bool, createdBy string) ([]byte, []byte, error) {
	if enforceStrongAuth {
		if !auth.IsValidUsername(string(username)) {
			return nil, nil, status.Errorf(
				codes.InvalidArgument,
				"username can only contain letters, digits and underscores")
		}
	}

	if enforceStrongAuth {
		if err := auth.IsStrongPassword(string(plainPassword)); err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}

	userdata := new(auth.User)
	plainpassword, err := userdata.SetPassword(plainPassword)
	if err != nil {
		return nil, nil, err
	}

	userdata.Active = true
	userdata.Username = string(username)
	userdata.Permissions = append(userdata.Permissions, auth.Permission{Permission: permission, Database: database})
	userdata.CreatedBy = createdBy
	userdata.CreatedAt = time.Now()

	if permission == auth.PermissionSysAdmin {
		userdata.IsSysAdmin = true
	}

	if (permission > auth.PermissionRW) && (permission < auth.PermissionAdmin) {
		return nil, nil, fmt.Errorf("unknown permission")
	}

	err = s.saveUser(userdata)

	return username, plainpassword, err
}

func (s *ImmuServer) getValidatedUser(username []byte, password []byte) (*auth.User, error) {
	userdata, err := s.getUser(username, true)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	err = userdata.ComparePasswords(password)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	return userdata, nil
}

// getUser returns userdata (username,hashed password, permission, active) from username
func (s *ImmuServer) getUser(username []byte, includeDeactivated bool) (*auth.User, error) {
	key := make([]byte, 1+len(username))
	key[0] = sysstore.KeyPrefixUser
	copy(key[1:], username)
	item, err := s.sysDb.Store.Get(schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	var usr auth.User
	err = json.Unmarshal(item.Value, &usr)
	if err != nil {
		return nil, err
	}
	if !includeDeactivated {
		if usr.Active {
			return nil, fmt.Errorf("user not found")
		}
	}
	return &usr, nil
}
func (s *ImmuServer) saveUser(user *auth.User) error {
	userData, err := json.Marshal(user)
	if err != nil {
		return logErr(s.Logger, "error saving user: %v", err)
	}

	userKey := make([]byte, 1+len(user.Username))
	userKey[0] = sysstore.KeyPrefixUser
	copy(userKey[1:], []byte(user.Username))

	userKV := schema.KeyValue{Key: userKey, Value: userData}
	_, err = s.sysDb.SafeSet(&schema.SafeSetOptions{
		Kv: &userKV,
	})

	return logErr(s.Logger, "error saving user: %v", err)
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
func (s *ImmuServer) removeUserFromLoginList(username string) {
	s.userdata.Lock()
	defer s.userdata.Unlock()
	delete(s.userdata.Userdata, username)
}
func (s *ImmuServer) addUserToLoginList(u *auth.User) {
	s.userdata.Lock()
	defer s.userdata.Unlock()
	s.userdata.Userdata[u.Username] = u
}

//checkMandatoryAuth checks if auth should be madatory for immudb to start
func (s *ImmuServer) mandatoryAuth() bool {
	if s.Options.GetMaintenance() {
		return false
	}
	//check if there are user created databases, should be zero for auth to be off
	for i := 0; i < s.dbList.Length(); i++ {
		val := s.dbList.GetByIndex(int64(i))
		if (val.options.dbName != s.Options.defaultDbName) &&
			(val.options.dbName != s.Options.systemAdminDbName) {
			return true
		}
	}
	//check if there is only default database
	if (s.dbList.Length() == 1) && (s.dbList.GetByIndex(DefaultDbIndex).options.dbName == s.Options.defaultDbName) {
		return false
	}
	if s.sysDb != nil {
		//check if there is only sysadmin on systemdb and no other user
		itemList, err := s.sysDb.Scan(&schema.ScanOptions{
			Prefix: []byte{sysstore.KeyPrefixUser},
		})
		if err != nil {
			s.Logger.Errorf("error getting users: %v", err)
			return true
		}
		for _, val := range itemList.Items {
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
