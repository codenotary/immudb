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
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var startedAt time.Time

func (s *ImmuServer) Start() error {

	dataDir := s.Options.GetDataDir()
	//create root dir where all databases are stored
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		s.Logger.Errorf("Unable to create data folder: %s", err)
		return err
	}
	if err := s.loadDatabases(dataDir); err != nil {
		s.Logger.Errorf("Unable load user databases %s", err)
		return err
	}
	if err := s.loadSystemDatabase(dataDir); err != nil {
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

	listener, err := net.Listen(s.Options.Network, s.Options.Bind())
	if err != nil {
		s.Logger.Errorf("Immudb unable to listen: %s", err)
		return err
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

	if s.Options.MetricsServer {
		metricsServer := StartMetrics(
			s.Options.MetricsBind(),
			s.Logger,
			func() float64 { return float64(s.SystemAdminDb.Store.CountAll()) },
			func() float64 { return time.Since(startedAt).Hours() },
		)
		defer func() {
			if err = metricsServer.Close(); err != nil {
				s.Logger.Errorf("failed to shutdown metric server: %s", err)
			}
		}()
	}

	s.GrpcServer = grpc.NewServer(options...)
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
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
	grpc_prometheus.Register(s.GrpcServer)
	if s.Options.CorruptionCheck {
		s.Cc = NewCorruptionChecker(s.SystemAdminDb.Store, s.Logger, s.Stop)
		go func() {
			s.Logger.Infof("starting consistency-checker")
			if err = s.Cc.Start(context.Background()); err != nil {
				s.Logger.Errorf("unable to start consistency-checker: %s", err)
			}
		}()
	}

	s.installShutdownHandler()
	s.Logger.Infof("starting immudb: %v", s.Options)

	dbSize, _ := s.SystemAdminDb.Store.DbSize()
	if dbSize <= 0 {
		s.Logger.Infof("Started with an empty database")
	}

	if s.Options.Pidfile != "" {
		if s.Pid, err = NewPid(s.Options.Pidfile); err != nil {
			s.Logger.Errorf("failed to write pidfile: %s", err)
			return err
		}
	}

	startedAt = time.Now()

	err = s.GrpcServer.Serve(listener)
	<-s.quit
	return err
}
func (s *ImmuServer) loadSystemDatabase(dataDir string) error {
	var err error
	systemDbRootDir := filepath.Join(dataDir, s.Options.GetSystemAdminDbName())

	_, sysDbErr := os.Stat(systemDbRootDir)
	if os.IsNotExist(sysDbErr) {
		op := DefaultOption().WithDbName(s.Options.GetSystemAdminDbName()).WithDbRootPath(dataDir)
		s.SystemAdminDb, err = NewDb(op)
		if err != nil {
			return err
		}
	} else {
		op := DefaultOption().WithDbName(s.Options.GetSystemAdminDbName()).WithDbRootPath(dataDir)
		s.SystemAdminDb, err = OpenDb(op)
		if err != nil {
			return err
		}
	}
	adminUsername, adminPlainPass, err := s.CreateAdminUser()
	if err != nil {
		s.Logger.Errorf(err.Error())
		return err
	} else if len(adminUsername) > 0 && len(adminPlainPass) > 0 {
		s.Logger.Infof("admin user %s created with password %s", adminUsername, adminPlainPass)
	}
	return nil
}

func (s *ImmuServer) loadDatabases(dataDir string) error {
	var dirs []string
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
	for _, val := range dirs {
		op := DefaultOption().WithDbName(val)
		db, err := OpenDb(op)
		if err != nil {
			return err
		}
		s.Databases = append(s.Databases, db)
	}
	return nil
}

func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immudb: %v", s.Options)
	defer func() { s.quit <- struct{}{} }()
	s.GrpcServer.Stop()
	s.GrpcServer = nil
	for _, db := range s.Databases {
		if db.SysStore != nil {
			defer func() { db.SysStore = nil }()
			db.SysStore.Close()
		}
		if db.Store != nil {
			defer func() { db.Store = nil }()
			db.Store.Close()
		}
	}

	if s.Options.CorruptionCheck {
		s.Cc.Stop(context.Background())
		s.Cc = nil
	}
	if s.SystemAdminDb.SysStore != nil {
		defer func() { s.SystemAdminDb.SysStore = nil }()
		s.SystemAdminDb.SysStore.Close()
	}
	if s.SystemAdminDb.Store != nil {
		defer func() {
			s.SystemAdminDb.Store = nil
		}()
		return s.SystemAdminDb.Store.Close()
	}
	return nil
}

func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	item, err := s.getUser(r.GetUser(), false)
	if err != nil {
		return nil, err
	}
	hashedPassword, err := s.getUserPassword(item.GetIndex())
	if err != nil {
		return nil, err
	}
	permissions, err := s.getUserPermissions(item.GetIndex())
	if err != nil {
		return nil, err
	}
	u := auth.User{
		Username:       string(item.GetKey()),
		HashedPassword: hashedPassword,
		Permissions:    permissions,
	}
	if u.ComparePasswords(r.GetPassword()) != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	token, err := auth.GenerateToken(u)
	if err != nil {
		return nil, err
	}
	loginResponse := &schema.LoginResponse{Token: []byte(token)}
	if u.Username == auth.AdminUsername && string(r.GetPassword()) == auth.AdminDefaultPassword {
		loginResponse.Warning = []byte(auth.WarnDefaultAdminPassword)
	}
	return loginResponse, nil
}

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

func (s *ImmuServer) CurrentRoot(ctx context.Context, e *empty.Empty) (*schema.Root, error) {
	root, err := s.SystemAdminDb.Store.CurrentRoot()
	if root != nil {
		s.Logger.Debugf("current root: %d %x", root.Index, root.Root)
	}
	return root, err
}

func (s *ImmuServer) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	s.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))
	item, err := s.SystemAdminDb.Store.Set(*kv)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *ImmuServer) SetSV(ctx context.Context, skv *schema.StructuredKeyValue) (*schema.Index, error) {
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	return s.Set(ctx, kv)
}

func (s *ImmuServer) SafeSet(ctx context.Context, opts *schema.SafeSetOptions) (*schema.Proof, error) {
	s.Logger.Debugf("safeset %s %d bytes", opts.Kv.Key, len(opts.Kv.Value))
	item, err := s.SystemAdminDb.Store.SafeSet(*opts)
	if err != nil {
		return nil, err
	}
	return item, nil
}

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

func (s *ImmuServer) SetBatch(ctx context.Context, kvl *schema.KVList) (*schema.Index, error) {
	s.Logger.Debugf("set batch %d", len(kvl.KVs))
	index, err := s.SystemAdminDb.Store.SetBatch(*kvl)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (s *ImmuServer) SetBatchSV(ctx context.Context, skvl *schema.SKVList) (*schema.Index, error) {
	kvl, err := skvl.ToKVList()
	if err != nil {
		return nil, err
	}
	return s.SetBatch(ctx, kvl)
}

func (s *ImmuServer) Get(ctx context.Context, k *schema.Key) (*schema.Item, error) {
	item, err := s.SystemAdminDb.Store.Get(*k)
	if item == nil {
		s.Logger.Debugf("get %s: item not found", k.Key)
	} else {
		s.Logger.Debugf("get %s %d bytes", k.Key, len(item.Value))
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *ImmuServer) GetSV(ctx context.Context, k *schema.Key) (*schema.StructuredItem, error) {
	it, err := s.Get(ctx, k)
	si, err := it.ToSItem()
	if err != nil {
		return nil, err
	}
	return si, err
}

func (s *ImmuServer) SafeGet(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("safeget %s", opts.Key)
	sitem, err := s.SystemAdminDb.Store.SafeGet(*opts)
	if err != nil {
		return nil, err
	}
	return sitem, nil
}

func (s *ImmuServer) SafeGetSV(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeStructuredItem, error) {
	it, err := s.SafeGet(ctx, opts)
	ssitem, err := it.ToSafeSItem()
	if err != nil {
		return nil, err
	}
	return ssitem, err
}

func (s *ImmuServer) GetBatch(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	for _, key := range kl.Keys {
		item, err := s.SystemAdminDb.Store.Get(*key)
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

func (s *ImmuServer) GetBatchSV(ctx context.Context, kl *schema.KeyList) (*schema.StructuredItemList, error) {
	list, err := s.GetBatch(ctx, kl)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) Scan(ctx context.Context, opts *schema.ScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	return s.SystemAdminDb.Store.Scan(*opts)
}

func (s *ImmuServer) ScanSV(ctx context.Context, opts *schema.ScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	list, err := s.SystemAdminDb.Store.Scan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	s.Logger.Debugf("count %s", prefix.Prefix)
	return s.SystemAdminDb.Store.Count(*prefix)
}

func (s *ImmuServer) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	s.Logger.Debugf("inclusion for index %d ", index.Index)
	proof, err := s.SystemAdminDb.Store.InclusionProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (s *ImmuServer) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	s.Logger.Debugf("consistency for index %d ", index.Index)
	proof, err := s.SystemAdminDb.Store.ConsistencyProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (s *ImmuServer) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	item, err := s.SystemAdminDb.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *ImmuServer) ByIndexSV(ctx context.Context, index *schema.Index) (*schema.StructuredItem, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	item, err := s.SystemAdminDb.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	sitem, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	return sitem, nil
}

func (s *ImmuServer) BySafeIndex(ctx context.Context, sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	s.Logger.Debugf("get by safeIndex %d ", sio.Index)
	item, err := s.SystemAdminDb.Store.BySafeIndex(*sio)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *ImmuServer) History(ctx context.Context, key *schema.Key) (*schema.ItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))
	list, err := s.SystemAdminDb.Store.History(*key)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (s *ImmuServer) HistorySV(ctx context.Context, key *schema.Key) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))

	list, err := s.SystemAdminDb.Store.History(*key)
	if err != nil {
		return nil, err
	}

	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) Health(context.Context, *empty.Empty) (*schema.HealthResponse, error) {
	health := s.SystemAdminDb.Store.HealthCheck()
	s.Logger.Debugf("health check: %v", health)
	return &schema.HealthResponse{Status: health}, nil
}

func (s *ImmuServer) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	index, err = s.SystemAdminDb.Store.Reference(refOpts)
	if err != nil {
		return nil, err
	}
	s.Logger.Debugf("reference options: %v", refOpts)
	return index, nil
}

func (s *ImmuServer) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	proof, err = s.SystemAdminDb.Store.SafeReference(*safeRefOpts)
	if err != nil {
		return nil, err
	}
	s.Logger.Debugf("safe reference options: %v", safeRefOpts)
	return proof, nil
}

func (s *ImmuServer) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	return s.SystemAdminDb.Store.ZAdd(*opts)
}

func (s *ImmuServer) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	return s.SystemAdminDb.Store.ZScan(*opts)
}

func (s *ImmuServer) ZScanSV(ctx context.Context, opts *schema.ZScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	list, err := s.SystemAdminDb.Store.ZScan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	return s.SystemAdminDb.Store.SafeZAdd(*opts)
}

func (s *ImmuServer) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	s.Logger.Debugf("iscan %+v", *opts)
	return s.SystemAdminDb.Store.IScan(*opts)
}

func (s *ImmuServer) IScanSV(ctx context.Context, opts *schema.IScanOptions) (*schema.SPage, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	page, err := s.SystemAdminDb.Store.IScan(*opts)
	SPage, err := page.ToSPage()
	if err != nil {
		return nil, err
	}
	return SPage, err
}

func (s *ImmuServer) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	kvChan := make(chan *pb.KVList)
	done := make(chan bool)

	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				stream.Send(list)
			} else {
				done <- true
				return
			}
		}
	}

	go retrieveLists()
	err := s.SystemAdminDb.Store.Dump(kvChan)
	<-done

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
//	i, err := s.SystemAdminDb.Store.Restore(kvChan)
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
