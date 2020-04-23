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
	"github.com/rs/xid"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

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
		return err
	}
	dbDir := filepath.Join(s.Options.Dir, s.Options.DbName)
	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return err
	}
	var uuid xid.ID
	if uuid, err = getOrSetUuid(s.Options.Dir); err != nil {
		return err
	}

	uuidContext := NewUuidContext(uuid)

	var uis []grpc.UnaryServerInterceptor
	var sss []grpc.StreamServerInterceptor
	uis = append(uis, uuidContext.UuidContextSetter)
	sss = append(sss, uuidContext.UuidStreamContextSetter)
	if s.Options.Auth {
		if err := s.loadOrGeneratePassword(); err != nil {
			return err
		}
		uis = append(uis, auth.ServerUnaryInterceptor)
		sss = append(sss, auth.ServerStreamInterceptor)
	}
	options = append(
		options,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(uis...)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(sss...)),
	)

	s.Store, err = store.Open(store.DefaultOptions(dbDir, s.Logger))
	if err != nil {
		return err
	}

	metricsServer := StartMetrics(s.Options.MetricsBind(), s.Logger)
	defer func() {
		if err = metricsServer.Close(); err != nil {
			s.Logger.Errorf("failed to shutdown metric server: %s", err)
		}
	}()

	s.GrpcServer = grpc.NewServer(options...)
	schema.RegisterImmuServiceServer(s.GrpcServer, s)
	s.installShutdownHandler()
	s.Logger.Infof("starting immudb: %v", s.Options)

	dbSize, _ := s.Store.DbSize()
	if dbSize <= 0 {
		s.Logger.Infof("Started with an empty database")
	}

	if s.Options.Pidfile != "" {
		if s.Pid, err = NewPid(s.Options.Pidfile); err != nil {
			return err
		}
	}

	err = s.GrpcServer.Serve(listener)
	<-s.quit
	return err
}

func (s *ImmuServer) Stop() error {
	s.Logger.Infof("stopping immudb: %v", s.Options)
	defer func() { s.quit <- struct{}{} }()
	s.GrpcServer.Stop()
	s.GrpcServer = nil
	if s.Store != nil {
		defer func() { s.Store = nil }()
		return s.Store.Close()
	}
	return nil
}

func (s *ImmuServer) Login(ctx context.Context, r *schema.LoginRequest) (*schema.LoginResponse, error) {
	if !s.Options.Auth {
		return nil, status.Errorf(codes.Unavailable, "authentication is disabled on server")
	}
	user := string(r.User)
	if user != auth.AdminUser.Username {
		return nil, status.Errorf(codes.Unauthenticated, "non-existent user %s", user)
	}
	pass := string(r.Password)
	if err := auth.AdminUser.ComparePasswords(pass); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "incorrect password")
	}
	token, err := auth.GenerateToken(user)
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

// todo(joe-dz): Enable restore when the feature is required again.
// Also, make sure that the generated files are updated
//func (s *ImmuServer) Restore(stream schema.ImmuService_RestoreServer) (err error) {
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

func (s *ImmuServer) loadOrGeneratePassword() error {
	var filename = "immudb_pwd"
	if err := auth.GenerateKeys(); err != nil {
		return fmt.Errorf("error generating or loading access keys (used for auth): %v", err)
	}

	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		hashedPassword, err := ioutil.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("error reading hashed password from file %s: %v", filename, err)
		}
		auth.AdminUser.SetPassword(hashedPassword)
		s.Logger.Infof("previous hashed password read from file %s\n", filename)
		return nil
	}

	plainPassword, err := auth.AdminUser.GenerateAndSetPassword()
	if err != nil {
		return fmt.Errorf("error generating password: %v", err)
	}
	if err := ioutil.WriteFile(filename, auth.AdminUser.HashedPassword, 0644); err != nil {
		return fmt.Errorf("error saving generated password hash to file %s: %v", filename, err)
	}

	s.Logger.Infof("user: %s, password: %s\n", auth.AdminUser.Username, plainPassword)
	s.Logger.Infof("hashed password saved to file %s\n", filename)

	return nil
}
