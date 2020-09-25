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

package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/client/rootservice"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/store"
)

// ImmuClient ...
type ImmuClient interface {
	Disconnect() error
	IsConnected() bool
	WaitForHealthCheck(ctx context.Context) (err error)
	Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error)
	Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error)
	Logout(ctx context.Context) error
	CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error
	ListUsers(ctx context.Context) (*schema.UserList, error)
	ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error
	ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error
	UpdateAuthConfig(ctx context.Context, kind auth.Kind) error
	UpdateMTLSConfig(ctx context.Context, enabled bool) error
	PrintTree(ctx context.Context) (*schema.Tree, error)
	CurrentRoot(ctx context.Context) (*schema.Root, error)
	Set(ctx context.Context, key []byte, value []byte) (*schema.Index, error)
	SafeSet(ctx context.Context, key []byte, value []byte) (*VerifiedIndex, error)
	RawSafeSet(ctx context.Context, key []byte, value []byte) (*VerifiedIndex, error)
	Get(ctx context.Context, key []byte) (*schema.StructuredItem, error)
	SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*VerifiedItem, error)
	RawSafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*VerifiedItem, error)
	Scan(ctx context.Context, prefix []byte) (*schema.StructuredItemList, error)
	ZScan(ctx context.Context, set []byte) (*schema.StructuredItemList, error)
	ByIndex(ctx context.Context, index uint64) (*schema.StructuredItem, error)
	RawBySafeIndex(ctx context.Context, index uint64) (*VerifiedItem, error)
	IScan(ctx context.Context, pageNumber uint64, pageSize uint64) (*schema.SPage, error)
	Count(ctx context.Context, prefix []byte) (*schema.ItemsCount, error)
	SetBatch(ctx context.Context, request *BatchRequest) (*schema.Index, error)
	GetBatch(ctx context.Context, keys [][]byte) (*schema.StructuredItemList, error)
	Inclusion(ctx context.Context, index uint64) (*schema.InclusionProof, error)
	Consistency(ctx context.Context, index uint64) (*schema.ConsistencyProof, error)
	History(ctx context.Context, key []byte) (*schema.StructuredItemList, error)
	Reference(ctx context.Context, reference []byte, key []byte) (*schema.Index, error)
	SafeReference(ctx context.Context, reference []byte, key []byte) (*VerifiedIndex, error)
	ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.Index, error)
	SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*VerifiedIndex, error)
	Dump(ctx context.Context, writer io.WriteSeeker) (int64, error)
	HealthCheck(ctx context.Context) error
	verifyAndSetRoot(result *schema.Proof, root *schema.Root, ctx context.Context) (bool, error)

	WithOptions(options *Options) *immuClient
	WithLogger(logger logger.Logger) *immuClient
	WithRootService(rs rootservice.RootService) *immuClient
	WithTimestampService(ts TimestampService) *immuClient
	WithClientConn(clientConn *grpc.ClientConn) *immuClient
	WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient
	WithTokenService(tokenService TokenService) *immuClient

	GetServiceClient() *schema.ImmuServiceClient
	GetOptions() *Options
	SetupDialOptions(options *Options) *[]grpc.DialOption
	CreateDatabase(ctx context.Context, d *schema.Database) error
	UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error)
	SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error
	DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error)
}

type immuClient struct {
	Dir           string
	Logger        logger.Logger
	Options       *Options
	clientConn    *grpc.ClientConn
	ServiceClient schema.ImmuServiceClient
	Rootservice   rootservice.RootService
	ts            TimestampService
	Tkns          TokenService
	sync.RWMutex
}

// DefaultClient ...
func DefaultClient() ImmuClient {
	return &immuClient{
		Dir:     "",
		Options: DefaultOptions(),
		Logger:  logger.NewSimpleLogger("immuclient", os.Stderr),
	}
}

// NewImmuClient ...
func NewImmuClient(options *Options) (c ImmuClient, err error) {
	ctx := context.Background()

	c = DefaultClient()
	l := logger.NewSimpleLogger("immuclient", os.Stderr)
	c.WithLogger(l)
	c.WithTokenService(options.Tkns.WithTokenFileName(options.TokenFileName))

	options.DialOptions = c.SetupDialOptions(options)
	if db, err := options.Tkns.GetDatabase(); err == nil && len(db) > 0 {
		options.CurrentDatabase = db
	}

	c.WithOptions(options)

	var clientConn *grpc.ClientConn
	if clientConn, err = c.Connect(ctx); err != nil {
		return nil, err
	}
	c.WithClientConn(clientConn)
	serviceClient := schema.NewImmuServiceClient(clientConn)
	c.WithServiceClient(serviceClient)
	if err = c.WaitForHealthCheck(ctx); err != nil {
		return nil, err
	}

	if err = os.MkdirAll(options.Dir, os.ModePerm); err != nil {
		l.Errorf("unable to create program file folder: %s", err)
		return nil, err
	}

	immudbRootProvider := rootservice.NewImmudbRootProvider(serviceClient)
	immudbUuidProvider := rootservice.NewImmudbUuidProvider(serviceClient)
	rootService := rootservice.NewRootService(cache.NewFileCache(options.Dir), l, immudbRootProvider, immudbUuidProvider)

	dt, err := timestamp.NewTdefault()
	if err != nil {
		return nil, err
	}
	ts := NewTimestampService(dt)
	c.WithTimestampService(ts).
		WithRootService(rootService)

	return c, nil
}

func (c *immuClient) SetupDialOptions(options *Options) *[]grpc.DialOption {

	opts := *options.DialOptions
	opts = append(opts, grpc.WithInsecure())

	//---------- TLS Setting -----------//
	if options.MTLs {
		//LoadX509KeyPair reads and parses a public/private key pair from a pair of files.
		//The files must contain PEM encoded data.
		//The certificate file may contain intermediate certificates following the leaf certificate to form a certificate chain.
		//On successful return, Certificate.Leaf will be nil because the parsed form of the certificate is not retained.
		cert, err := tls.LoadX509KeyPair(
			//certificate signed by intermediary for the client. It contains the public key.
			options.MTLsOptions.Certificate,
			//client key (needed to sign the requests. Only the public key can open the data)
			options.MTLsOptions.Pkey,
		)
		if err != nil {
			grpclog.Errorf("failed to read credentials: %s", err)
		}
		certPool := x509.NewCertPool()
		// chain is composed by default by ca.cert.pem and intermediate.cert.pem
		bs, err := ioutil.ReadFile(options.MTLsOptions.ClientCAs)
		if err != nil {
			grpclog.Errorf("failed to read ca cert: %s", err)
		}

		// AppendCertsFromPEM attempts to parse a series of PEM encoded certificates.
		// It appends any certificates found to s and reports whether any certificates were successfully parsed.
		// On many Linux systems, /etc/ssl/cert.pem will contain the system wide set of root CAs
		// in a format suitable for this function.
		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			grpclog.Errorf("failed to append certs")
		}

		transportCreds := credentials.NewTLS(&tls.Config{
			// ServerName is used to verify the hostname on the returned
			// certificates unless InsecureSkipVerify is given. It is also included
			// in the client's handshake to support virtual hosting unless it is
			// an IP address.
			ServerName: options.MTLsOptions.Servername,
			// Certificates contains one or more certificate chains to present to the
			// other side of the connection. The first certificate compatible with the
			// peer's requirements is selected automatically.
			// Server configurations must set one of Certificates, GetCertificate or
			// GetConfigForClient. Clients doing client-authentication may set either
			// Certificates or GetClientCertificate.
			Certificates: []tls.Certificate{cert},
			// Safe store, trusted certificate list
			// Server need to use one certificate presents in this lists.
			// RootCAs defines the set of root certificate authorities
			// that clients use when verifying server certificates.
			// If RootCAs is nil, TLS uses the host's root CA set.
			RootCAs: certPool,
		})
		opts = []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}
	}
	if options.Auth {
		token, err := c.Tkns.GetToken()
		if err == nil {
			opts = append(opts, grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)))
			opts = append(opts, grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)))
		}
	}
	return &opts
}

func (c *immuClient) Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error) {
	if c.clientConn, err = grpc.Dial(c.Options.Bind(), *c.Options.DialOptions...); err != nil {
		c.Logger.Debugf("dialed %v", c.Options)
		return nil, err
	}
	return c.clientConn, nil
}

func (c *immuClient) Disconnect() error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	if err := c.clientConn.Close(); err != nil {
		return err
	}
	c.ServiceClient = nil
	c.clientConn = nil
	c.Logger.Debugf("disconnected %v in %s", c.Options, time.Since(start))
	return nil
}

func (c *immuClient) IsConnected() bool {
	return c.clientConn != nil && c.ServiceClient != nil
}

func (c *immuClient) WaitForHealthCheck(ctx context.Context) (err error) {
	for i := 0; i < c.Options.HealthCheckRetries+1; i++ {
		if err = c.HealthCheck(ctx); err == nil {
			c.Logger.Debugf("health check succeeded %v", c.Options)
			return nil
		}
		c.Logger.Debugf("health check failed: %v", err)
		if c.Options.HealthCheckRetries > 0 {
			time.Sleep(time.Second)
		}
	}
	return err
}

// GetServiceClient ...
func (c *immuClient) GetServiceClient() *schema.ImmuServiceClient {
	return &c.ServiceClient
}

func (c *immuClient) GetOptions() *Options {
	return c.Options
}

// Deprecated: use user list instead
func (c *immuClient) ListUsers(ctx context.Context) (*schema.UserList, error) {
	return c.ServiceClient.ListUsers(ctx, new(empty.Empty))
}

// CreateUser ...
func (c *immuClient) CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.CreateUser(ctx, &schema.CreateUserRequest{
		User:       user,
		Password:   pass,
		Permission: permission,
		Database:   databasename,
	})
	c.Logger.Debugf("createuser finished in %s", time.Since(start))
	return err
}

// ChangePassword ...
func (c *immuClient) ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.ChangePassword(ctx, &schema.ChangePasswordRequest{
		User:        user,
		OldPassword: oldPass,
		NewPassword: newPass,
	})
	c.Logger.Debugf("changepassword finished in %s", time.Since(start))
	return err
}

func (c *immuClient) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.UpdateAuthConfig(ctx, &schema.AuthConfig{
		Kind: uint32(kind),
	})
	c.Logger.Debugf("updateauthconfig finished in %s", time.Since(start))
	return err
}

func (c *immuClient) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.UpdateMTLSConfig(ctx, &schema.MTLSConfig{
		Enabled: enabled,
	})
	c.Logger.Debugf("updatemtlsconfig finished in %s", time.Since(start))
	return err
}

func (c *immuClient) PrintTree(ctx context.Context) (*schema.Tree, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	tree, err := c.ServiceClient.PrintTree(ctx, new(empty.Empty))
	c.Logger.Debugf("set finished in %s", time.Since(start))
	return tree, err
}

// Login ...
func (c *immuClient) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.Login(ctx, &schema.LoginRequest{
		User:     user,
		Password: pass,
	})
	c.Logger.Debugf("login finished in %s", time.Since(start))
	return result, err
}

// Logout ...
func (c *immuClient) Logout(ctx context.Context) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	if err := c.Tkns.DeleteToken(); err != nil {
		return err
	}
	_, err := c.ServiceClient.Logout(ctx, new(empty.Empty))
	c.Logger.Debugf("logout finished in %s", time.Since(start))
	return err
}

// Get ...
func (c *immuClient) Get(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	item, err := c.ServiceClient.Get(ctx, &schema.Key{Key: key})
	if err != nil {
		return nil, err
	}
	result, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("get finished in %s", time.Since(start))
	return result, err
}

// CurrentRoot returns current merkle tree root and index
func (c *immuClient) CurrentRoot(ctx context.Context) (*schema.Root, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	root, err := c.ServiceClient.CurrentRoot(ctx, &empty.Empty{})
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("Current root finished in %s", time.Since(start))
	return root, err
}

// SafeGet ...
func (c *immuClient) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (vi *VerifiedItem, err error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	sgOpts := &schema.SafeGetOptions{
		Key: key,
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	safeItem, err := c.ServiceClient.SafeGet(ctx, sgOpts, opts...)
	if err != nil {
		return nil, err
	}

	h, err := safeItem.Hash()
	if err != nil {
		return nil, err
	}
	verified := safeItem.Proof.Verify(h, *root)
	if verified {
		// saving a fresh root
		tocache := schema.NewRoot()
		tocache.SetIndex(safeItem.Proof.At)
		tocache.SetRoot(safeItem.Proof.Root)
		err = c.Rootservice.SetRoot(tocache, c.Options.CurrentDatabase)
		if err != nil {
			return nil, err
		}
	}

	c.Logger.Debugf("safeget finished in %s", time.Since(start))
	sitem, err := safeItem.ToSafeSItem()
	if err != nil {
		return nil, err
	}
	return &VerifiedItem{
			Key:      sitem.Item.GetKey(),
			Value:    sitem.Item.Value.Payload,
			Index:    sitem.Item.GetIndex(),
			Time:     sitem.Item.Value.Timestamp,
			Verified: verified,
		},
		nil
}

// RawSafeGet ...
func (c *immuClient) RawSafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (vi *VerifiedItem, err error) {
	c.Lock()
	defer c.Unlock()

	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	sgOpts := &schema.SafeGetOptions{
		Key: key,
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	safeItem, err := c.ServiceClient.SafeGet(ctx, sgOpts, opts...)
	if err != nil {
		return nil, err
	}

	h, err := safeItem.Hash()
	if err != nil {
		return nil, err
	}
	verified := safeItem.Proof.Verify(h, *root)
	if verified {
		// saving a fresh root
		tocache := schema.NewRoot()
		tocache.SetIndex(safeItem.Proof.At)
		tocache.SetRoot(safeItem.Proof.Root)
		err = c.Rootservice.SetRoot(tocache, c.Options.CurrentDatabase)
		if err != nil {
			return nil, err
		}
	}

	c.Logger.Debugf("safeget finished in %s", time.Since(start))

	return &VerifiedItem{
			Key:      safeItem.Item.GetKey(),
			Value:    safeItem.Item.Value,
			Index:    safeItem.Item.GetIndex(),
			Verified: verified,
		},
		nil
}

// Scan ...
func (c *immuClient) Scan(ctx context.Context, prefix []byte) (*schema.StructuredItemList, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	list, err := c.ServiceClient.Scan(ctx, &schema.ScanOptions{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// ZScan ...
func (c *immuClient) ZScan(ctx context.Context, set []byte) (*schema.StructuredItemList, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	list, err := c.ServiceClient.ZScan(ctx, &schema.ZScanOptions{Set: set})
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// IScan ...
func (c *immuClient) IScan(ctx context.Context, pageNumber uint64, pageSize uint64) (*schema.SPage, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	page, err := c.ServiceClient.IScan(ctx, &schema.IScanOptions{PageSize: pageSize, PageNumber: pageNumber})
	if err != nil {
		return nil, err
	}
	return page.ToSPage()
}

// Count ...
func (c *immuClient) Count(ctx context.Context, prefix []byte) (*schema.ItemsCount, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.Count(ctx, &schema.KeyPrefix{Prefix: prefix})
}

// Set ...
func (c *immuClient) Set(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	skv := c.NewSKV(key, value)
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	result, err := c.ServiceClient.Set(ctx, kv)
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("set finished in %s", time.Since(start))
	return result, err
}

// SafeSet ...
func (c *immuClient) SafeSet(ctx context.Context, key []byte, value []byte) (*VerifiedIndex, error) {
	start := time.Now()
	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	skv := c.NewSKV(key, value)
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	opts := &schema.SafeSetOptions{
		Kv: kv,
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	var metadata runtime.ServerMetadata

	result, err := c.ServiceClient.SafeSet(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed from
	// request values. From now on, result.Leaf can be trusted.
	sitem := schema.StructuredItem{
		Key: key,
		Value: &schema.Content{
			Timestamp: skv.Value.Timestamp,
			Payload:   value,
		},
		Index: result.Index,
	}
	item, err := sitem.ToItem()
	if err != nil {
		return nil, err
	}
	h := item.Hash()

	if !bytes.Equal(h, result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root, ctx)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safeset finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

// RawSafeSet ...
func (c *immuClient) RawSafeSet(ctx context.Context, key []byte, value []byte) (vi *VerifiedIndex, err error) {
	start := time.Now()
	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeSetOptions{
		Kv: &schema.KeyValue{
			Key:   key,
			Value: value,
		},
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	var metadata runtime.ServerMetadata

	result, err := c.ServiceClient.SafeSet(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed from
	// request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   key,
		Value: value,
		Index: result.Index,
	}

	h := item.Hash()

	if !bytes.Equal(h, result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root, ctx)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safeset finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

// SetBatch ...
func (c *immuClient) SetBatch(ctx context.Context, request *BatchRequest) (*schema.Index, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	list, err := request.toKVList()
	slist := c.NewSKVList(list)
	if err != nil {
		return nil, err
	}
	result, err := c.ServiceClient.SetBatchSV(ctx, slist)
	c.Logger.Debugf("set-batch finished in %s", time.Since(start))
	return result, err
}

// GetBatch ...
func (c *immuClient) GetBatch(ctx context.Context, keys [][]byte) (*schema.StructuredItemList, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	keyList := &schema.KeyList{}
	for _, key := range keys {
		keyList.Keys = append(keyList.Keys, &schema.Key{Key: key})
	}
	list, err := c.ServiceClient.GetBatch(ctx, keyList)
	c.Logger.Debugf("get-batch finished in %s", time.Since(start))
	if err != nil {
		return nil, err
	}
	return list.ToSItemList()
}

// Inclusion ...
func (c *immuClient) Inclusion(ctx context.Context, index uint64) (*schema.InclusionProof, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.Inclusion(ctx, &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("inclusion finished in %s", time.Since(start))
	return result, err
}

// Consistency ...
func (c *immuClient) Consistency(ctx context.Context, index uint64) (*schema.ConsistencyProof, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.Consistency(ctx, &schema.Index{
		Index: index,
	})
	c.Logger.Debugf("consistency finished in %s", time.Since(start))
	return result, err
}

// ByIndex returns a structured value at index
func (c *immuClient) ByIndex(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	item, err := c.ServiceClient.ByIndex(ctx, &schema.Index{
		Index: index,
	})
	if err != nil {
		return nil, err
	}
	result, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("by-index finished in %s", time.Since(start))
	return result, err
}

// RawBySafeIndex returns a verified index at specified index
func (c *immuClient) RawBySafeIndex(ctx context.Context, index uint64) (*VerifiedItem, error) {
	c.Lock()
	defer c.Unlock()

	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	safeItem, err := c.ServiceClient.BySafeIndex(ctx, &schema.SafeIndexOptions{
		Index: index,
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	})

	if err != nil {
		return nil, err
	}

	h, err := safeItem.Hash()
	if err != nil {
		return nil, err
	}
	verified := safeItem.Proof.Verify(h, *root)
	if verified {
		// saving a fresh root
		tocache := schema.NewRoot()
		tocache.SetIndex(safeItem.Proof.At)
		tocache.SetRoot(safeItem.Proof.Root)
		err = c.Rootservice.SetRoot(tocache, c.Options.CurrentDatabase)
		if err != nil {
			return nil, err
		}
	}

	c.Logger.Debugf("by-rawsafeindex finished in %s", time.Since(start))

	return &VerifiedItem{
			Key:      safeItem.Item.GetKey(),
			Value:    safeItem.Item.Value,
			Index:    safeItem.Item.GetIndex(),
			Verified: verified,
		},
		nil
}

// History ...
func (c *immuClient) History(ctx context.Context, key []byte) (sl *schema.StructuredItemList, err error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	list, err := c.ServiceClient.History(ctx, &schema.Key{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	sl, err = list.ToSItemList()
	if err != nil {
		return nil, err
	}
	c.Logger.Debugf("history finished in %s", time.Since(start))
	return sl, err
}

// Reference ...
func (c *immuClient) Reference(ctx context.Context, reference []byte, key []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.Reference(ctx, &schema.ReferenceOptions{
		Reference: reference,
		Key:       key,
	})
	c.Logger.Debugf("reference finished in %s", time.Since(start))
	return result, err
}

// SafeReference ...
func (c *immuClient) SafeReference(ctx context.Context, reference []byte, key []byte) (*VerifiedIndex, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: reference,
			Key:       key,
		},
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	var metadata runtime.ServerMetadata

	result, err := c.ServiceClient.SafeReference(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   reference,
		Value: key,
		Index: result.Index,
	}
	if !bytes.Equal(item.Hash(), result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root, ctx)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safereference finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

// ZAdd ...
func (c *immuClient) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.Index, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.ZAdd(ctx, &schema.ZAddOptions{
		Set:   set,
		Score: score,
		Key:   key,
	})
	c.Logger.Debugf("zadd finished in %s", time.Since(start))
	return result, err
}

// SafeZAdd ...
func (c *immuClient) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*VerifiedIndex, error) {
	start := time.Now()

	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	root, err := c.Rootservice.GetRoot(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	opts := &schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   set,
			Score: score,
			Key:   key,
		},
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	}

	var metadata runtime.ServerMetadata
	result, err := c.ServiceClient.SafeZAdd(
		ctx,
		opts,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	key2, err := store.SetKey(key, set, score)
	if err != nil {
		return nil, err
	}

	// This guard ensures that result.Leaf is equal to the item's hash computed
	// from request values. From now on, result.Leaf can be trusted.
	item := schema.Item{
		Key:   key2,
		Value: key,
		Index: result.Index,
	}
	if !bytes.Equal(item.Hash(), result.Leaf) {
		return nil, errors.New("proof does not match the given item")
	}

	verified, err := c.verifyAndSetRoot(result, root, ctx)
	if err != nil {
		return nil, err
	}

	c.Logger.Debugf("safezadd finished in %s", time.Since(start))

	return &VerifiedIndex{
			Index:    result.Index,
			Verified: verified,
		},
		nil
}

// Dump to be used from Immu CLI
func (c *immuClient) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	start := time.Now()

	if !c.IsConnected() {
		return 0, ErrNotConnected
	}

	bkpClient, err := c.ServiceClient.Dump(ctx, &empty.Empty{})
	if err != nil {
		return 0, err
	}
	defer bkpClient.CloseSend()

	var offset int64
	var counter int64
	for {
		kvList, err := bkpClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("error receiving chunk: %v", err)
		}
		for _, kv := range kvList.Kv {
			kvBytes, err := proto.Marshal(kv)
			if err != nil {
				return 0, fmt.Errorf("error marshaling key-value %+v: %v", kv, err)
			}
			o, err := writeSeek(writer, kvBytes, offset)
			if err != nil {
				return 0, fmt.Errorf("error writing as bytes key-value %+v: %v", kv, err)
			}
			offset = o
			counter++
		}
	}
	c.Logger.Debugf("dump finished in %s", time.Since(start))
	return counter, nil
}

// todo(joe-dz): Enable restore when the feature is required again.
// Also, make sure that the generated files are updated
// Restore to be used from Immu CLI
//func (c *immuClient) Restore(ctx context.Context, reader io.ReadSeeker, chunkSize int) (int64, error) {
//	start := time.Now()
//
//	var entryCounter int64
//	var counter int64
//
//	if !c.IsConnected() {
//		return counter, ErrNotConnected
//	}
//
//	var errs []string
//	var offset int64
//	kvList := new(pb.KVList)
//	for {
//		lineBytes, o, err := readSeek(reader, offset)
//		if err == io.EOF {
//			break
//		}
//		entryCounter++
//		offset = o
//		if err != nil {
//			errs = append(errs, fmt.Sprintf("error reading file entry %d: %v", entryCounter, err))
//			continue
//		}
//		if len(lineBytes) <= 1 {
//			continue
//		}
//		kv := new(pb.KV)
//		err = proto.Unmarshal(lineBytes, kv)
//		if err != nil {
//			errs = append(errs, fmt.Sprintf("error unmarshaling to key-value the file entry %d: %v", entryCounter, err))
//			continue
//		}
//
//		kvList.Kv = append(kvList.Kv, kv)
//		if len(kvList.Kv) == chunkSize {
//			if err := c.restoreChunk(ctx, kvList); err != nil {
//				errs = append(errs, err.Error())
//			} else {
//				counter += int64(len(kvList.Kv))
//			}
//			kvList.Kv = []*pb.KV{}
//		}
//	}
//
//	if len(kvList.Kv) > 0 {
//		if err := c.restoreChunk(ctx, kvList); err != nil {
//			errs = append(errs, err.Error())
//		} else {
//			counter += int64(len(kvList.Kv))
//		}
//		kvList.Kv = []*pb.KV{}
//	}
//
//	var errorsMerged error
//	if len(errs) > 0 {
//		errorsMerged = fmt.Errorf("Errors:\n\t%s", strings.Join(errs[:], "\n\t"))
//		c.Logger.Errorf("restore terminated with errors. %v", errorsMerged)
//	} else {
//		c.Logger.Infof("restore finished restoring %d of %d entries in %s", counter, entryCounter, time.Since(start))
//	}
//
//	return counter, errorsMerged
//}
func (c *immuClient) HealthCheck(ctx context.Context) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	response, err := c.ServiceClient.Health(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	if !response.Status {
		return ErrHealthCheckFailed
	}
	c.Logger.Debugf("health-check finished in %s", time.Since(start))
	return nil
}

// todo(joe-dz): Enable restore when the feature is required again.
// Also, make sure that the generated files are updated
//func (c *immuClient) restoreChunk(ctx context.Context, kvList *pb.KVList) error {
//	kvListLen := len(kvList.Kv)
//	kvListStr := fmt.Sprintf("%+v", kvList)
//	restoreClient, err := c.ServiceClient.Restore(ctx)
//	if err != nil {
//		c.Logger.Errorf("error sending to restore client a chunk of %d KVs in key-value list %s: error getting restore client: %v", kvListLen, kvListStr, err)
//		return err
//	}
//	defer restoreClient.CloseAndRecv()
//	err = restoreClient.Send(kvList)
//	if err != nil {
//		c.Logger.Errorf("error sending to restore client a chunk of %d KVs in key-value list %s: %v", kvListLen, kvListStr, err)
//		return err
//	}
//	return nil
//}

func writeSeek(w io.WriteSeeker, msg []byte, offset int64) (int64, error) {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(msg)))
	if _, err := w.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}
	if _, err := w.Write(buf); err != nil {
		return offset, err
	}
	if _, err := w.Seek(offset+int64(len(buf)), io.SeekStart); err != nil {
		return offset, err
	}
	if _, err := w.Write(msg); err != nil {
		return offset, err
	}
	return offset + int64(len(buf)) + int64(len(msg)), nil
}

/*func readSeek(r io.ReadSeeker, offset int64) ([]byte, int64, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	buf := make([]byte, 4)
	o1 := offset + int64(len(buf))
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, o1, err
	}
	if _, err := r.Seek(o1, io.SeekStart); err != nil {
		return nil, o1, err
	}
	size := binary.LittleEndian.Uint32(buf)
	msg := make([]byte, size)
	o2 := o1 + int64(size)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, o2, err
	}
	return msg, o2, nil
}*/

func (c *immuClient) verifyAndSetRoot(result *schema.Proof, root *schema.Root, ctx context.Context) (bool, error) {
	verified := result.Verify(result.Leaf, *root)
	var err error
	if verified {
		//saving a fresh root
		tocache := schema.NewRoot()
		tocache.SetIndex(result.Index)
		tocache.SetRoot(result.Root)
		err = c.Rootservice.SetRoot(tocache, c.Options.CurrentDatabase)
	}
	return verified, err
}

// CreateDatabase create a new database by making a grpc call
func (c *immuClient) CreateDatabase(ctx context.Context, db *schema.Database) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.CreateDatabase(ctx, db)
	c.Logger.Debugf("CreateDatabase finished in %s", time.Since(start))
	return err
}

// UseDatabase create a new database by making a grpc call
func (c *immuClient) UseDatabase(ctx context.Context, db *schema.Database) (*schema.UseDatabaseReply, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.UseDatabase(ctx, db)

	c.Options.CurrentDatabase = db.Databasename

	c.Logger.Debugf("UseDatabase finished in %s", time.Since(start))
	return result, err
}
func (c *immuClient) ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	in := &schema.ChangePermissionRequest{
		Action:     action,
		Username:   username,
		Database:   database,
		Permission: permissions,
	}
	_, err := c.ServiceClient.ChangePermission(ctx, in)
	c.Logger.Debugf("ChangePermission finished in %s", time.Since(start))
	return err
}

func (c *immuClient) SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error {
	start := time.Now()
	if !c.IsConnected() {
		return ErrNotConnected
	}
	_, err := c.ServiceClient.SetActiveUser(ctx, u)
	c.Logger.Debugf("SetActiveUser finished in %s", time.Since(start))
	return err
}

func (c *immuClient) DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error) {
	start := time.Now()
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	result, err := c.ServiceClient.DatabaseList(ctx, &empty.Empty{})
	c.Logger.Debugf("DatabaseList finished in %s", time.Since(start))
	return result, err
}
