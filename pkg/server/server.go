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
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/rs/xid"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/fs"
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
	sysDbDir := filepath.Join(s.Options.Dir, s.Options.SysDbName)
	if err = os.MkdirAll(sysDbDir, os.ModePerm); err != nil {
		s.Logger.Errorf("Unable to create sys data folder: %s", err)
		return err
	}
	dbDir := filepath.Join(s.Options.Dir, s.Options.DbName)
	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		s.Logger.Errorf("Unable to create data folder: %s", err)
		return err
	}
	var uuid xid.ID
	if uuid, err = getOrSetUuid(s.Options.Dir); err != nil {
		return err
	}

	auth.AuthEnabled = s.Options.Auth
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

	s.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, s.Logger))
	if err != nil {
		s.Logger.Errorf("Unable to open sysstore: %s", err)
		return err
	}
	s.Store, err = store.Open(store.DefaultOptions(dbDir, s.Logger))
	if err != nil {
		s.Logger.Errorf("Unable to open store: %s", err)
		return err
	}

	auth.AdminUserExists = s.adminUserExists
	auth.CreateAdminUser = s.createAdminUser

	metricsServer := StartMetrics(
		s.Options.MetricsBind(),
		s.Logger,
		func() float64 { return float64(s.Store.CountAll()) },
		func() float64 { return time.Since(startedAt).Hours() },
	)
	defer func() {
		if err = metricsServer.Close(); err != nil {
			s.Logger.Errorf("failed to shutdown metric server: %s", err)
		}
	}()

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
	s.installShutdownHandler()
	s.Logger.Infof("starting immudb: %v", s.Options)

	dbSize, _ := s.Store.DbSize()
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

func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immudb: %v", s.Options)
	defer func() { s.quit <- struct{}{} }()
	s.GrpcServer.Stop()
	s.GrpcServer = nil
	if s.SysStore != nil {
		defer func() { s.SysStore = nil }()
		s.SysStore.Close()
	}
	if s.Store != nil {
		defer func() {
			s.Store = nil
		}()
		return s.Store.Close()
	}
	return nil
}

func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	if !s.Options.Auth && !auth.IsAdminClient(ctx) {
		return nil, status.Errorf(codes.Unavailable, "authentication is disabled on server")
	}
	item, err := s.SysStore.Get(schema.Key{Key: r.User})
	if err != nil {
		if err == store.ErrKeyNotFound {
			return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
		}
		s.Logger.Errorf("error getting user %s during login: %v", string(r.User), err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	username := string(item.GetKey())
	u := auth.User{
		Username:       username,
		HashedPassword: item.GetValue(),
		Admin:          username == auth.AdminUsername,
	}
	if u.ComparePasswords(r.GetPassword()) != nil {
		return nil, status.Errorf(codes.PermissionDenied, "invalid user or password")
	}
	token, err := auth.GenerateToken(u)
	if err != nil {
		return nil, err
	}
	return &schema.LoginResponse{Token: []byte(token)}, nil
}

func (s *ImmuServer) CurrentRoot(ctx context.Context, e *empty.Empty) (*schema.Root, error) {
	root, err := s.Store.CurrentRoot()
	if root != nil {
		s.Logger.Debugf("current root: %d %x", root.Index, root.Root)
	}
	return root, err
}

func (s *ImmuServer) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	s.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))
	item, err := s.Store.Set(*kv)
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
	item, err := s.Store.SafeSet(*opts)
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
	index, err := s.Store.SetBatch(*kvl)
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
	item, err := s.Store.Get(*k)
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
	sitem, err := s.Store.SafeGet(*opts)
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
		item, err := s.Store.Get(*key)
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
	return s.Store.Scan(*opts)
}

func (s *ImmuServer) ScanSV(ctx context.Context, opts *schema.ScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("scan %+v", *opts)
	list, err := s.Store.Scan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	s.Logger.Debugf("count %s", prefix.Prefix)
	return s.Store.Count(*prefix)
}

func (s *ImmuServer) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	s.Logger.Debugf("inclusion for index %d ", index.Index)
	proof, err := s.Store.InclusionProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (s *ImmuServer) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	s.Logger.Debugf("consistency for index %d ", index.Index)
	proof, err := s.Store.ConsistencyProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (s *ImmuServer) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	item, err := s.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *ImmuServer) ByIndexSV(ctx context.Context, index *schema.Index) (*schema.StructuredItem, error) {
	s.Logger.Debugf("get by index %d ", index.Index)
	item, err := s.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	sitem, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	return sitem, nil
}

func (s *ImmuServer) History(ctx context.Context, key *schema.Key) (*schema.ItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))
	list, err := s.Store.History(*key)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (s *ImmuServer) HistorySV(ctx context.Context, key *schema.Key) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("history for key %s ", string(key.Key))

	list, err := s.Store.History(*key)
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
	health := s.Store.HealthCheck()
	s.Logger.Debugf("health check: %v", health)
	return &schema.HealthResponse{Status: health}, nil
}

func (s *ImmuServer) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	index, err = s.Store.Reference(refOpts)
	if err != nil {
		return nil, err
	}
	s.Logger.Debugf("reference options: %v", refOpts)
	return index, nil
}

func (s *ImmuServer) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	proof, err = s.Store.SafeReference(*safeRefOpts)
	if err != nil {
		return nil, err
	}
	s.Logger.Debugf("safe reference options: %v", safeRefOpts)
	return proof, nil
}

func (s *ImmuServer) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	return s.Store.ZAdd(*opts)
}

func (s *ImmuServer) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	return s.Store.ZScan(*opts)
}

func (s *ImmuServer) ZScanSV(ctx context.Context, opts *schema.ZScanOptions) (*schema.StructuredItemList, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	list, err := s.Store.ZScan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (s *ImmuServer) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	s.Logger.Debugf("zadd %+v", *opts)
	return s.Store.SafeZAdd(*opts)
}

func (s *ImmuServer) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	s.Logger.Debugf("iscan %+v", *opts)
	return s.Store.IScan(*opts)
}

func (s *ImmuServer) IScanSV(ctx context.Context, opts *schema.IScanOptions) (*schema.SPage, error) {
	s.Logger.Debugf("zscan %+v", *opts)
	page, err := s.Store.IScan(*opts)
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
	err := s.Store.Dump(kvChan)
	<-done

	s.Logger.Debugf("Dump stream complete")
	return err
}

func (s *ImmuServer) Backup(ctx context.Context, req *schema.BackupRequest) (*schema.BackupResponse, error) {
	defer s.Store.Unlock()
	defer s.SysStore.Unlock()
	s.Store.Lock()
	s.SysStore.Lock()
	s.Store.FlushToDisk()
	s.SysStore.FlushToDisk()

	snapshotPath := "immudb_bkp_" + time.Now().Format("2006-01-02_15-04-05")
	if err := fs.CopyDir(s.Options.Dir, snapshotPath); err != nil {
		return nil, err
	}
	// remove the immudb.identifier file from the backup
	if err := os.Remove(snapshotPath + "/" + IDENTIFIER_FNAME); err != nil {
		s.Logger.Errorf(
			"error removing immudb identifier file %s from db snapshot %s: %v",
			IDENTIFIER_FNAME, snapshotPath, err)
	}
	response := &schema.BackupResponse{Message: []byte(snapshotPath)}
	if req.GetUncompressed() {
		return response, nil
	}

	var archivePath string
	var archiveErr error
	if runtime.GOOS != "windows" {
		archivePath = snapshotPath + ".tar.gz"
		archiveErr = fs.TarIt(snapshotPath, archivePath)
	} else {
		archivePath = snapshotPath + ".zip"
		archiveErr = fs.ZipIt(snapshotPath, archivePath, fs.ZipDefaultCompression)
	}
	if archiveErr != nil {
		return response, status.Errorf(
			codes.Internal,
			"database copied successfully to %s, but compression to %s failed: %v",
			snapshotPath, archivePath, archiveErr)
	}
	if err := os.RemoveAll(snapshotPath); err != nil {
		s.Logger.Errorf(
			"error removing db snapshot dir %s after successfully compressing it to %s: %v",
			snapshotPath, archivePath, err)
	}
	absArchivePath, err := filepath.Abs(archivePath)
	if err != nil {
		s.Logger.Errorf("error converting rel path %s to absolute: %v", archivePath, err)
		absArchivePath = archivePath
	}
	response.Message = []byte(absArchivePath)
	return response, nil
}

func (s *ImmuServer) Restore(ctx context.Context, req *schema.RestoreRequest) (*empty.Empty, error) {
	e := new(empty.Empty)
	snapshotPath := string(req.GetSnapshotPath())
	_, err := os.Stat(snapshotPath)
	if err != nil {
		return e, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	snapshotExt := filepath.Ext(snapshotPath)
	snapshotName := filepath.Base(snapshotPath)
	snapshotNameNoExt := strings.TrimSuffix(snapshotName, snapshotExt)
	if strings.ToLower(snapshotExt) == ".gz" {
		snapshotExt = filepath.Ext(snapshotNameNoExt) + snapshotExt
		snapshotNameNoExt = strings.TrimSuffix(snapshotName, snapshotExt)
	}
	dbParentDir := filepath.Dir(s.Options.Dir) + string(os.PathSeparator)
	extractedSnapshotDir := dbParentDir + snapshotNameNoExt
	now := time.Now().Format("2006-01-02_15-04-05")
	var extract func(string, string) error
	switch snapshotExt {
	case ".tar.gz":
		extract = fs.UnTarIt
	case ".zip":
		extract = fs.UnZipIt
	case "": // uncompressed
		// TODO OGG: this will result in the backup being renamed directly to the db folder
		if dbParentDir != filepath.Dir(snapshotPath)+string(os.PathSeparator) {
			extract = fs.CopyDir
		}
	default:
		return e, status.Errorf(
			codes.InvalidArgument,
			"snapshot %s has unsupported format %s; supported formats: .tar.gz, .zip or none (uncompressed)",
			snapshotPath, snapshotExt)
	}
	if extract != nil {
		if err = extract(snapshotPath, dbParentDir); err != nil {
			return e, status.Errorf(codes.Internal, "%v", err)
		}
	}
	// keep the same db identifier
	if err = fs.CopyFile(
		path.Join(s.Options.Dir, IDENTIFIER_FNAME),
		path.Join(extractedSnapshotDir, IDENTIFIER_FNAME)); err != nil {
		return e, status.Errorf(codes.Internal, "%v", err)
	}

	defer s.Store.Unlock()
	defer s.SysStore.Unlock()
	s.Store.Lock()
	s.SysStore.Lock()
	s.Store.FlushToDisk()
	s.SysStore.FlushToDisk()

	dbDirAutoBackupPath := s.Options.Dir + "_bkp_before_restore_" + now
	if err = os.Rename(s.Options.Dir, dbDirAutoBackupPath); err != nil {
		return e, status.Errorf(
			codes.Internal,
			"error renaming previous db dir %s to %s during restore: %v",
			s.Options.Dir, dbDirAutoBackupPath, err)
	}
	if err = os.Rename(extractedSnapshotDir, s.Options.Dir); err != nil {
		return e, status.Errorf(
			codes.Internal,
			"error renaming new tmp snapshot dir %s to db dir %s during restore: %v",
			extractedSnapshotDir, s.Options.Dir, err)
	}

	if err = s.Store.Close(); err != nil {
		s.Logger.Errorf("error closing previous store before db restore: %v", err)
	}
	s.Store = nil
	if err = s.SysStore.Close(); err != nil {
		s.Logger.Errorf("error closing previous sysstore before db restore: %v", err)
	}
	s.SysStore = nil

	sysDbDir := filepath.Join(s.Options.Dir, s.Options.SysDbName)
	s.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, s.Logger))
	if err != nil {
		s.Logger.Errorf("Unable to reopen sysstore: %s", err)
		return e, status.Errorf(codes.Internal, "unable to reopen sysstore: %s", err)
	}
	dbDir := filepath.Join(s.Options.Dir, s.Options.DbName)
	s.Store, err = store.Open(store.DefaultOptions(dbDir, s.Logger))
	if err != nil {
		s.Logger.Errorf("Unable to reopen store: %s", err)
		return e, status.Errorf(codes.Internal, "unable to reopen store: %s", err)
	}

	return e, nil
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
//	i, err := s.Store.Restore(kvChan)
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
