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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/xid"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var startedAt time.Time

// Start starts the immudb server
// Loads and starts the System DB, and puts it in the first index
func (s *ImmuServer) Start() error {
	dataDir := s.Options.GetDataDir()
	//create root dir where all databases are stored
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		s.Logger.Errorf("Unable to create data folder: %s", err)
		return err
	}
	if err := s.loadSystemDatabase(dataDir); err != nil {
		s.Logger.Errorf("Unable load user databases %s", err)
		return err
	}
	if err := s.loadDatabases(dataDir); err != nil {
		s.Logger.Errorf("Unable load user databases %s", err)
		return err
	}

	options := []grpc.ServerOption{}
	//----------TLS Setting-----------//
	if s.Options.MTLs {
		// credentials needed to communicate with client
		certificate, err := tls.LoadX509KeyPair(
			s.Options.MTLsOptions.Certificate,
			s.Options.MTLsOptions.Pkey,
		)
		if err != nil {
			s.Logger.Errorf("failed to read server key pair: %s", err)
			return err
		}
		certPool := x509.NewCertPool()
		// Trusted store, contain the list of trusted certificates. client has to use one of this certificate to be trusted by this server
		bs, err := ioutil.ReadFile(s.Options.MTLsOptions.ClientCAs)
		if err != nil {
			s.Logger.Errorf("failed to read client ca cert: %s", err)
			return err
		}

		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			s.Logger.Errorf("failed to append client certs")
			return err
		}

		tlsConfig := &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{certificate},
			ClientCAs:    certPool,
		}

		options = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}
	}

	var err error
	var listener net.Listener
	if s.Options.usingCustomListener {
		s.Logger.Infof("Using custom listener")
		listener = s.Options.listener
	} else {
		listener, err = net.Listen(s.Options.Network, s.Options.Bind())
		if err != nil {
			s.Logger.Errorf("Immudb unable to listen: %s", err)
			return err
		}
	}

	systemDbRootDir := filepath.Join(dataDir, s.Options.GetSystemAdminDbName())
	var uuid xid.ID
	if uuid, err = getOrSetUuid(systemDbRootDir); err != nil {
		return err
	}

	auth.AuthEnabled = s.Options.Auth
	auth.DevMode = s.Options.DevMode
	adminPassword, err := auth.DecodeBase64Password(s.Options.AdminPassword)
	if err != nil {
		s.Logger.Errorf(err.Error())
		return err
	}
	auth.AdminPassword = adminPassword
	auth.UpdateMetrics = func(ctx context.Context) { Metrics.UpdateClientMetrics(ctx) }

	if s.Options.MetricsServer {
		metricsServer := StartMetrics(
			s.Options.MetricsBind(),
			s.Logger,
			func() float64 { return float64(s.databases[0].Store.CountAll()) },
			func() float64 { return time.Since(startedAt).Hours() },
		)
		defer func() {
			if err = metricsServer.Close(); err != nil {
				s.Logger.Errorf("failed to shutdown metric server: %s", err)
			}
		}()
	}
	s.installShutdownHandler()
	s.Logger.Infof("starting immudb: %v", s.Options)

	dbSize, _ := s.databases[0].Store.DbSize()
	if dbSize <= 0 {
		s.Logger.Infof("Started with an empty database")
	}

	if s.Options.Pidfile != "" {
		if s.Pid, err = NewPid(s.Options.Pidfile); err != nil {
			s.Logger.Errorf("failed to write pidfile: %s", err)
			return err
		}
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
	//TODO gj check why is this needed
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
	)
	s.GrpcServer = grpc.NewServer(options...)
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
	grpc_prometheus.Register(s.GrpcServer)

	startedAt = time.Now()

	err = s.GrpcServer.Serve(listener)
	<-s.quit
	return err
}

//loadSystemDatabase it is important that is is called before loadDatabases so that systemdb is at index zero of the databases array
func (s *ImmuServer) loadSystemDatabase(dataDir string) error {
	if len(s.databases) > 0 {
		panic("loadSystemDatabase should be called before loadDatabases")
	}

	systemDbRootDir := filepath.Join(dataDir, s.Options.GetSystemAdminDbName())

	_, sysDbErr := os.Stat(systemDbRootDir)
	if os.IsNotExist(sysDbErr) {
		//if s.Options.GetInMemoryStore()
		op := DefaultOption().
			WithDbName(s.Options.GetSystemAdminDbName()).
			WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck).
			WithInMemoryStore(s.Options.GetInMemoryStore())
		db, err := NewDb(op)
		if err != nil {
			return err
		}
		adminUsername, adminPlainPass, err := db.CreateUser([]byte(auth.AdminUsername), []byte(auth.AdminPassword), auth.PermissionSysAdmin, false)
		if err != nil {
			s.Logger.Errorf(err.Error())
			return err
		} else if len(adminUsername) > 0 && len(adminPlainPass) > 0 {
			s.Logger.Infof("admin user %s created with password %s", adminUsername, adminPlainPass)
		}
		s.databases = append(s.databases, db)
	} else {
		op := DefaultOption().
			WithDbName(s.Options.GetSystemAdminDbName()).
			WithDbRootPath(dataDir).
			WithCorruptionChecker(s.Options.CorruptionCheck)
		db, err := OpenDb(op)
		if err != nil {
			return err
		}
		s.databases = append(s.databases, db)
	}
	return nil
}

func (s *ImmuServer) loadDatabases(dataDir string) error {
	var dirs []string
	//get first level sub directories of data dir
	err := filepath.Walk(s.Options.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		//get only first child directories, exclude datadir, exclude systemdb dir
		if info.IsDir() &&
			(strings.Count(path, string(filepath.Separator)) == 1) &&
			(dataDir != path) &&
			!strings.Contains(path, s.Options.GetSystemAdminDbName()) {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	//load databases that are inside each directory
	for _, val := range dirs {
		op := DefaultOption().WithDbName(val).WithCorruptionChecker(s.Options.CorruptionCheck)
		db, err := OpenDb(op)
		if err != nil {
			return err
		}
		s.databases = append(s.databases, db)
	}
	return nil
}

// Stop stops the immudb server
func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immudb: %v", s.Options)
	defer func() { s.quit <- struct{}{} }()
	s.GrpcServer.Stop()
	s.GrpcServer = nil

	for _, db := range s.databases {
		db.StopCorruptionChecker()
		if db.SysStore != nil {
			defer func() { db.SysStore = nil }()
			db.SysStore.Close()
		}
		if db != nil {
			defer func() { db.Store = nil }()
			db.Store.Close()
		}

	}
	return nil
}

// Login ...
func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	var u *auth.User
	var err error
	index := 0
	for _, d := range s.databases {
		u, err = d.Login(r.GetUser(), r.GetPassword())
		//we found the uer, let's break the loop
		if err == nil {
			break
		}
		index++
	}
	//finished the loop
	//let's test if we found a user or not
	if err != nil {
		return nil, err
	}
	u.UserUUID = auth.NewStringUUID()
	token, err := auth.GenerateToken(*u)
	if err != nil {
		return nil, err
	}
	loginResponse := &schema.LoginResponse{Token: []byte(token)}
	if u.Username == auth.AdminUsername && string(r.GetPassword()) == auth.AdminDefaultPassword {
		//TODO gj warning is not displayed to immuclient
		loginResponse.Warning = []byte(auth.WarnDefaultAdminPassword)
	}

	//associate the userUUID to userdata and db index
	s.userDatabases.Lock()
	defer s.userDatabases.Unlock()
	s.userDatabases.userDatabaseID[u.UserUUID] = &userDatabasePair{
		userUUID: u.UserUUID,
		index:    index,
		User:     *u,
	}
	fmt.Println("userDatabaseID", s.userDatabases.userDatabaseID, index)
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

func updateConfigItem(
	configFilepath string,
	key string,
	newOrUpdatedLine string,
	unchanged func(string) bool) error {
	if strings.TrimSpace(configFilepath) == "" {
		return fmt.Errorf("config file does not exist")
	}
	configBytes, err := ioutil.ReadFile(configFilepath)
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
	if err := ioutil.WriteFile(configFilepath, []byte(strings.Join(configLines, "\n")), 0644); err != nil {
		return err
	}
	return nil
}

// UpdateAuthConfig ...
func (s *ImmuServer) UpdateAuthConfig(ctx context.Context, req *schema.AuthConfig) (*empty.Empty, error) {
	e := new(empty.Empty)
	s.Options.Auth = req.GetKind() > 0
	auth.AuthEnabled = s.Options.Auth
	if err := updateConfigItem(
		s.Options.Config,
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
	e := new(empty.Empty)
	if err := updateConfigItem(
		s.Options.Config,
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
func (s *ImmuServer) CurrentRoot(ctx context.Context, e *empty.Empty) (*schema.Root, error) {
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].CurrentRoot(e)
}

// Set ...
func (s *ImmuServer) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	s.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Set(kv)
}

// SetSV ...
func (s *ImmuServer) SetSV(ctx context.Context, skv *schema.StructuredKeyValue) (*schema.Index, error) {
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	return s.Set(ctx, kv)
}

// SafeSet ...
func (s *ImmuServer) SafeSet(ctx context.Context, opts *schema.SafeSetOptions) (*schema.Proof, error) {
	fmt.Println("safeset")
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].SafeSet(opts)
}

// SafeSetSV ...
func (s *ImmuServer) SafeSetSV(ctx context.Context, sopts *schema.SafeSetSVOptions) (*schema.Proof, error) {
	kv, err := sopts.Skv.ToKV()
	if err != nil {
		return nil, err
	}
	opts := &schema.SafeSetOptions{
		Kv:        kv,
		RootIndex: sopts.RootIndex,
	}
	return s.SafeSet(ctx, opts)
}

// SetBatch ...
func (s *ImmuServer) SetBatch(ctx context.Context, kvl *schema.KVList) (*schema.Index, error) {
	s.Logger.Debugf("set batch %d", len(kvl.KVs))
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	index, err := s.databases[ind].SetBatch(kvl)
	if err != nil {
		return nil, err
	}
	return index, nil
}

// SetBatchSV ...
func (s *ImmuServer) SetBatchSV(ctx context.Context, skvl *schema.SKVList) (*schema.Index, error) {
	kvl, err := skvl.ToKVList()
	if err != nil {
		return nil, err
	}
	return s.SetBatch(ctx, kvl)
}

// Get ...
func (s *ImmuServer) Get(ctx context.Context, k *schema.Key) (*schema.Item, error) {
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	item, err := s.databases[ind].Get(k)
	if item == nil {
		s.Logger.Debugf("get %s: item not found", k.Key)
	} else {
		s.Logger.Debugf("get %s %d bytes", k.Key, len(item.Value))
	}
	return item, err
}

// GetSV ...
func (s *ImmuServer) GetSV(ctx context.Context, k *schema.Key) (*schema.StructuredItem, error) {
	it, err := s.Get(ctx, k)
	si, err := it.ToSItem()
	if err != nil {
		return nil, err
	}
	return si, err
}

// SafeGet ...
func (s *ImmuServer) SafeGet(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("safeget %s", opts.Key)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].SafeGet(opts)
}

// SafeGetSV ...
func (s *ImmuServer) SafeGetSV(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeStructuredItem, error) {
	it, err := s.SafeGet(ctx, opts)
	if err != nil {
		return nil, err
	}
	return it.ToSafeSItem()
}

// GetBatch ...
func (s *ImmuServer) GetBatch(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	for _, key := range kl.Keys {
		item, err := s.databases[ind].Get(key)
		if err == nil || err == store.ErrKeyNotFound {
			if item != nil {
				list.Items = append(list.Items, item)
			}
		} else {
			return nil, err
		}
	}
	return list, nil
}

// GetBatchSV ...
func (s *ImmuServer) GetBatchSV(ctx context.Context, kl *schema.KeyList) (*schema.StructuredItemList, error) {
	list, err := s.GetBatch(ctx, kl)
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// Scan ...
func (s *ImmuServer) Scan(ctx context.Context, opts *schema.ScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Scan(opts)
}

// ScanSV ...
func (s *ImmuServer) ScanSV(ctx context.Context, opts *schema.ScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ScanSV(opts)
}

// Count ...
func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	s.Logger.Debugf("count %s", prefix.Prefix)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Count(prefix)
}

// Inclusion ...
func (s *ImmuServer) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Inclusion(index)
}

// Consistency ...
func (s *ImmuServer) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Consistency(index)
}

// ByIndex ...
func (s *ImmuServer) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ByIndex(index)
}

// ByIndexSV ...
func (s *ImmuServer) ByIndexSV(ctx context.Context, index *schema.Index) (*schema.StructuredItem, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	item, err := s.databases[ind].ByIndex(index)
	if err != nil {
		return nil, err
	}
	return item.ToSItem()
}

// BySafeIndex ...
func (s *ImmuServer) BySafeIndex(ctx context.Context, sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("get by safeIndex %d ", sio.Index)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].BySafeIndex(sio)
}

// History ...
func (s *ImmuServer) History(ctx context.Context, key *schema.Key) (*schema.ItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].History(key)
}

// HistorySV ...
func (s *ImmuServer) HistorySV(ctx context.Context, key *schema.Key) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	list, err := s.databases[ind].History(key)
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// Health ...
func (s *ImmuServer) Health(ctx context.Context, e *empty.Empty) (*schema.HealthResponse, error) {
	//s.Logger.Debugf("health check: %v", health)
	ind, _ := s.getDbIndexFromCtx(ctx)

	if ind < 0 { //probably immuclient hasn't logged in yet
		return s.databases[0].Health(e)
	}
	return s.databases[ind].Health(e)
}

// Reference ...
func (s *ImmuServer) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	s.Logger.Debugf("reference options: %v", refOpts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].Reference(refOpts)
}

// SafeReference ...
func (s *ImmuServer) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	s.Logger.Debugf("safe reference options: %v", safeRefOpts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].SafeReference(safeRefOpts)
}

// ZAdd ...
func (s *ImmuServer) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ZAdd(opts)
}

// ZScan ...
func (s *ImmuServer) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ZScan(opts)
}

// ZScanSV ...
func (s *ImmuServer) ZScanSV(ctx context.Context, opts *schema.ZScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	list, err := s.databases[ind].ZScan(opts)
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// SafeZAdd ...
func (s *ImmuServer) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].SafeZAdd(opts)
}

// IScan ...
func (s *ImmuServer) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	s.Logger.Debugf("iscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].IScan(opts)
}

// IScanSV ...
func (s *ImmuServer) IScanSV(ctx context.Context, opts *schema.IScanOptions) (*schema.SPage, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	page, err := s.databases[ind].IScan(opts)
	if err != nil {
		return nil, err
	}
	return page.ToSPage()
}

// Dump ...
func (s *ImmuServer) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	jsonUser, err := auth.GetLoggedInUser(stream.Context())
	if err != nil {
		return err
	}
	ind, err := s.getDbIndexFromUUID(jsonUser.UserUUID)
	if err != nil {
		return err
	}
	err = s.databases[ind].Dump(in, stream)

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
		s.Logger.Infof("caught SIGTERM")
		if err := s.Stop(); err != nil {
			s.Logger.Errorf("shutdown error: %v", err)
		}
		s.Logger.Infof("shutdown completed")
	}()
}

// ChangePassword ...
func (s *ImmuServer) ChangePassword(ctx context.Context, r *schema.ChangePasswordRequest) (*empty.Empty, error) {
	s.Logger.Debugf("ChangePassword %+v", *r)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ChangePassword(r)
}

// CreateDatabase Create a new database instance and asign the default user to it //TODO
func (s *ImmuServer) CreateDatabase(ctx context.Context, newdb *schema.Database) (*schema.CreateDatabaseReply, error) {
	jsonUser, err := auth.GetLoggedInUser(ctx)
	if err != nil {
		return nil, fmt.Errorf("Could not get loggedin user data")
	}
	if (jsonUser.Permissions != auth.PermissionAdmin) &&
		(jsonUser.Permissions != auth.PermissionSysAdmin) {
		return nil, fmt.Errorf("Logged In user does not have permisions for this operation")
	}

	s.Logger.Debugf("createdatabase %+v", *newdb)
	dataDir := s.Options.GetDataDir()

	dbName := newdb.Databasename + "_" + GenerateDbID()
	op := DefaultOption().
		WithDbName(dbName).
		WithDbRootPath(dataDir).
		WithCorruptionChecker(s.Options.CorruptionCheck).
		WithInMemoryStore(s.Options.GetInMemoryStore())
	db, err := NewDb(op)
	if err != nil {
		s.Logger.Errorf(err.Error())
		return nil, fmt.Errorf("Could not create new database")
	}

	//create the dafault admin user and generate a password
	adminUsername, adminPlainPass, err := db.CreateAdminUser([]byte(auth.AdminUsername))
	if err != nil {
		return nil, err
	}

	if len(adminUsername) > 0 && len(adminPlainPass) > 0 {
		s.Logger.Infof("Created Newdatabase %s", newdb.Databasename)
	}
	s.databases = append(s.databases, db)
	return &schema.CreateDatabaseReply{
		Error: &schema.Error{
			Errorcode:    0,
			Errormessage: fmt.Sprintf("Created Database: %s with user: %s and password: %s", newdb.Databasename, string(adminUsername), string(adminPlainPass)),
		},
	}, nil
}

// CreateUser ...
func (s *ImmuServer) CreateUser(ctx context.Context, r *schema.CreateUserRequest) (*schema.UserResponse, error) {
	s.Logger.Debugf("CreateUser %+v", *r)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	_, err = s.databases[ind].userExists(r.User, auth.PermissionNone, nil)
	if err == nil {
		return nil, fmt.Errorf("user with this username already exists")
	}
	username, _, err := s.databases[ind].CreateUser(r.User, r.Password, r.Permissions[0], true)
	if err != nil {
		return nil, err
	}
	return &schema.UserResponse{User: username, Permissions: r.GetPermissions()}, nil
}

// SetPermission ...
func (s *ImmuServer) SetPermission(ctx context.Context, r *schema.Item) (*empty.Empty, error) {
	s.Logger.Debugf("SetPermission %+v", *r)
	if len(r.GetValue()) <= 0 {
		return new(empty.Empty), status.Errorf(
			codes.InvalidArgument, "no permission specified")
	}
	if (int(r.GetValue()[0]) == auth.PermissionAdmin) ||
		(int(r.GetValue()[0]) == auth.PermissionSysAdmin) {
		return new(empty.Empty), status.Error(
			codes.PermissionDenied, "admin permission is not allowed to be set")
	}
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].SetPermission(r)
}

// DeactivateUser ...
func (s *ImmuServer) DeactivateUser(ctx context.Context, r *schema.UserRequest) (*empty.Empty, error) {
	s.Logger.Debugf("DeactivateUser %+v", *r)
	// jsonUser, err := auth.GetLoggedInUser(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	// if (jsonUser.Permissions != auth.PermissionAdmin) &&
	// 	(jsonUser.Permissions != auth.PermissionSysAdmin) {
	// 	return nil, fmt.Errorf("you do not have permision for this operation")
	// }
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].DeactivateUser(r)
}

// GetUser ...
func (s *ImmuServer) GetUser(ctx context.Context, r *schema.UserRequest) (*schema.UserResponse, error) {
	s.Logger.Debugf("DeactivateUser %+v", *r)
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	item, err := s.databases[ind].getUser(r.User, true)
	if err != nil {
		return nil, fmt.Errorf("user not found")
	}
	return &schema.UserResponse{User: item.Key, Permissions: []byte("")}, nil
}

// ListUsers ...
func (s *ImmuServer) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	s.Logger.Debugf("ListUsers %+v")
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].ListUsers(&emptypb.Empty{})
}

// PrintTree ...
func (s *ImmuServer) PrintTree(ctx context.Context, r *empty.Empty) (*schema.Tree, error) {
	s.Logger.Debugf("PrintTree %+v")
	ind, err := s.getDbIndexFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	return s.databases[ind].PrintTree(), nil
}
func (s *ImmuServer) getDbIndexFromCtx(ctx context.Context) (int, error) {
	//if we're in devmode just work with system db, since it's just testing
	if s.Options.DevMode {
		return 0, nil
	}
	userUUID, ok := ctx.Value("userUUID").(string)
	if !ok {
		return -1, fmt.Errorf("user uuid not found")
	}
	fmt.Println("userUUID", userUUID)
	return s.getDbIndexFromUUID(userUUID)
}
func (s *ImmuServer) getDbIndexFromUUID(userUUID string) (int, error) {
	s.userDatabases.Lock()
	defer s.userDatabases.Unlock()
	userdata, ok := s.userDatabases.userDatabaseID[userUUID]
	if !ok {
		return -1, fmt.Errorf("Logedin user data not found")
	}
	return userdata.index, nil
}
