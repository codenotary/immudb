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

package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/pkg/signer"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// ImmuClient ...
type ImmuClient interface {
	Disconnect() error
	IsConnected() bool
	WaitForHealthCheck(ctx context.Context) (err error)
	HealthCheck(ctx context.Context) error
	Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error)

	Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error)
	Logout(ctx context.Context) error

	CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error
	ListUsers(ctx context.Context) (*schema.UserList, error)
	ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error
	ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error
	UpdateAuthConfig(ctx context.Context, kind auth.Kind) error
	UpdateMTLSConfig(ctx context.Context, enabled bool) error

	WithOptions(options *Options) *immuClient
	WithLogger(logger logger.Logger) *immuClient
	WithStateService(rs state.StateService) *immuClient
	WithClientConn(clientConn *grpc.ClientConn) *immuClient
	WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient
	WithTokenService(tokenService TokenService) *immuClient
	WithServerSigningPubKey(serverSigningPubKey *ecdsa.PublicKey) *immuClient

	GetServiceClient() *schema.ImmuServiceClient
	GetOptions() *Options
	SetupDialOptions(options *Options) *[]grpc.DialOption

	DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error)
	CreateDatabase(ctx context.Context, d *schema.Database) error
	UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error)
	SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error

	CleanIndex(ctx context.Context, req *emptypb.Empty) error

	CurrentState(ctx context.Context) (*schema.ImmutableState, error)

	Set(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error)
	VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error)

	Get(ctx context.Context, key []byte) (*schema.Entry, error)
	GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	VerifiedGet(ctx context.Context, key []byte) (*schema.Entry, error)
	VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error)
	VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error)

	ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error)
	VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error)

	Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)
	ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	TxByID(ctx context.Context, tx uint64) (*schema.Tx, error)
	VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error)
	TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error)

	Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error)
	CountAll(ctx context.Context) (*schema.EntryCount, error)

	SetAll(ctx context.Context, kvList *schema.SetRequest) (*schema.TxMetadata, error)
	GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error)

	ExecAll(ctx context.Context, in *schema.ExecAllRequest) (*schema.TxMetadata, error)

	SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error)
	VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error)

	SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error)
	VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error)

	Dump(ctx context.Context, writer io.WriteSeeker) (int64, error)

	streamSet(ctx context.Context) (schema.ImmuService_StreamSetClient, error)
	streamGet(ctx context.Context, in *schema.KeyRequest) (schema.ImmuService_StreamGetClient, error)
	streamScan(ctx context.Context, in *schema.ScanRequest) (schema.ImmuService_StreamScanClient, error)
	streamZScan(ctx context.Context, in *schema.ZScanRequest) (schema.ImmuService_StreamZScanClient, error)
	streamHistory(ctx context.Context, in *schema.HistoryRequest) (schema.ImmuService_StreamHistoryClient, error)

	StreamSet(ctx context.Context, kv []*stream.KeyValue) (*schema.TxMetadata, error)
	StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error)
	StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)
	StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)
	StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	// DEPRECATED: Please use CurrentState
	CurrentRoot(ctx context.Context) (*schema.ImmutableState, error)
	// DEPRECATED: Please use VerifiedSet
	SafeSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error)
	// DEPRECATED: Please use VerifiedGet
	SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*schema.Entry, error)
	// DEPRECATED: Please use VerifiedZAdd
	SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error)
	// DEPRECATED: Please use VerifiedSetReference
	SafeReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error)
}

const DefaultDB = "defaultdb"

type immuClient struct {
	Dir                 string
	Logger              logger.Logger
	Options             *Options
	clientConn          *grpc.ClientConn
	ServiceClient       schema.ImmuServiceClient
	StateService        state.StateService
	Tkns                TokenService
	serverSigningPubKey *ecdsa.PublicKey
	Ssf                 stream.ServiceFactory
	sync.RWMutex
}

// DefaultClient ...
func DefaultClient() ImmuClient {
	return &immuClient{
		Dir:     "",
		Options: DefaultOptions(),
		Logger:  logger.NewSimpleLogger("immuclient", os.Stderr),
		Ssf:     stream.NewStreamServiceFactory(DefaultOptions().StreamChunkSize),
	}
}

// NewImmuClient ...
func NewImmuClient(options *Options) (c ImmuClient, err error) {
	ctx := context.Background()

	c = DefaultClient()
	l := logger.NewSimpleLogger("immuclient", os.Stderr)
	c.WithLogger(l)
	c.WithTokenService(options.Tkns.WithTokenFileName(options.TokenFileName))

	if options.ServerSigningPubKey != "" {
		pk, err := signer.ParsePublicKeyFile(options.ServerSigningPubKey)
		if err != nil {
			return nil, err
		}
		c.WithServerSigningPubKey(pk)
	}

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
		return nil, logErr(l, "Unable to create program file folder: %s", err)
	}

	stateProvider := state.NewStateProvider(serviceClient)
	uuidProvider := state.NewUUIDProvider(serviceClient)

	stateService, err := state.NewStateService(cache.NewFileCache(options.Dir), l, stateProvider, uuidProvider)
	if err != nil {
		return nil, logErr(l, "Unable to create state service: %s", err)
	}

	c.WithStateService(stateService)

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
	var uic []grpc.UnaryClientInterceptor

	if c.serverSigningPubKey != nil {
		uic = append(uic, c.SignatureVerifierInterceptor)
	}

	if options.Auth && c.Tkns != nil {
		token, err := c.Tkns.GetToken()
		uic = append(uic, auth.ClientUnaryInterceptor(token))
		if err == nil {
			opts = append(opts, grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)))
		}
	}
	opts = append(opts, grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(uic...)))
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.MaxRecvMsgSize)))

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

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
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

// CurrentState returns current database state
func (c *immuClient) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("Current state finished in %s", time.Since(start))

	return c.ServiceClient.CurrentState(ctx, &empty.Empty{})
}

// Get ...
func (c *immuClient) Get(ctx context.Context, key []byte) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("get finished in %s", time.Since(start))

	return c.ServiceClient.Get(ctx, &schema.KeyRequest{Key: key})
}

// VerifiedGet ...
func (c *immuClient) VerifiedGet(ctx context.Context, key []byte) (vi *schema.Entry, err error) {
	start := time.Now()
	defer c.Logger.Debugf("VerifiedGet finished in %s", time.Since(start))
	return c.verifiedGet(ctx, &schema.KeyRequest{
		Key: key,
	})
}

// VerifiedGetSince ...
func (c *immuClient) VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	start := time.Now()
	defer c.Logger.Debugf("VerifiedGetSince finished in %s", time.Since(start))
	return c.verifiedGet(ctx, &schema.KeyRequest{
		Key:     key,
		SinceTx: tx,
	})
}

// VerifiedGetAt ...
func (c *immuClient) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	start := time.Now()
	defer c.Logger.Debugf("verifiedGetAt finished in %s", time.Since(start))
	return c.verifiedGet(ctx, &schema.KeyRequest{
		Key:  key,
		AtTx: tx,
	})
}

func (c *immuClient) verifiedGet(ctx context.Context, kReq *schema.KeyRequest) (vi *schema.Entry, err error) {
	err = c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableGetRequest{
		KeyRequest:   kReq,
		ProveSinceTx: state.TxId,
	}

	vEntry, err := c.ServiceClient.VerifiableGet(ctx, req)
	if err != nil {
		return nil, err
	}

	inclusionProof := schema.InclusionProofFrom(vEntry.InclusionProof)
	dualProof := schema.DualProofFrom(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	var vTx uint64
	var kv *store.KV

	if vEntry.Entry.ReferencedBy == nil {
		vTx = vEntry.Entry.Tx
		kv = database.EncodeKV(kReq.Key, vEntry.Entry.Value)
	} else {
		vTx = vEntry.Entry.ReferencedBy.Tx
		kv = database.EncodeReference(vEntry.Entry.ReferencedBy.Key, vEntry.Entry.Key, vEntry.Entry.ReferencedBy.AtTx)
	}

	if state.TxId <= vTx {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.TargetTxMetadata.EH)

		sourceID = state.TxId
		sourceAlh = schema.DigestFrom(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxMetadata.Alh()
	} else {
		eh = schema.DigestFrom(vEntry.VerifiableTx.DualProof.SourceTxMetadata.EH)

		sourceID = vTx
		sourceAlh = dualProof.SourceTxMetadata.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFrom(state.TxHash)
	}

	verifies := store.VerifyInclusion(
		inclusionProof,
		kv,
		eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vEntry.Entry, nil
}

// GetSince ...
func (c *immuClient) GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("get finished in %s", time.Since(start))

	return c.ServiceClient.Get(ctx, &schema.KeyRequest{Key: key, SinceTx: tx})
}

// GetAt ...
func (c *immuClient) GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("get finished in %s", time.Since(start))

	return c.ServiceClient.Get(ctx, &schema.KeyRequest{Key: key, AtTx: tx})
}

// Scan ...
func (c *immuClient) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.Scan(ctx, req)
}

// ZScan ...
func (c *immuClient) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.ZScan(ctx, req)
}

// Count ...
func (c *immuClient) Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.Count(ctx, &schema.KeyPrefix{Prefix: prefix})
}

// CountAll ...
func (c *immuClient) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.CountAll(ctx, new(empty.Empty))
}

// Set ...
func (c *immuClient) Set(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("set finished in %s", time.Since(start))

	txmd, err := c.ServiceClient.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}})
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// VerifiedSet ...
func (c *immuClient) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("VerifiedSet finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableSetRequest{
		SetRequest:   &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}},
		ProveSinceTx: state.TxId,
	}

	var metadata runtime.ServerMetadata

	verifiableTx, err := c.ServiceClient.VerifiableSet(
		ctx,
		req,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	if verifiableTx.Tx.Metadata.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFrom(verifiableTx.Tx)

	inclusionProof, err := tx.Proof(database.EncodeKey(key))
	if err != nil {
		return nil, err
	}

	verifies := store.VerifyInclusion(inclusionProof, database.EncodeKV(key, value), tx.Eh())
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Eh() != schema.DigestFrom(verifiableTx.DualProof.TargetTxMetadata.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFrom(state.TxHash)
	targetID = tx.ID
	targetAlh = tx.Alh

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFrom(verifiableTx.DualProof),
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)

		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Metadata, nil
}

func (c *immuClient) SetAll(ctx context.Context, req *schema.SetRequest) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	txmd, err := c.ServiceClient.Set(ctx, req)
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != len(req.KVs) {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// ExecAll ...
func (c *immuClient) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	txmd, err := c.ServiceClient.ExecAll(ctx, req)
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != len(req.Operations) {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// GetAll ...
func (c *immuClient) GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("get-batch finished in %s", time.Since(start))

	keyList := &schema.KeyListRequest{}

	for _, key := range keys {
		keyList.Keys = append(keyList.Keys, key)
	}

	return c.ServiceClient.GetAll(ctx, keyList)
}

// TxByID ...
func (c *immuClient) TxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("by-index finished in %s", time.Since(start))

	t, err := c.ServiceClient.TxById(ctx, &schema.TxRequest{
		Tx: tx,
	})

	if err != nil {
		return nil, err
	}

	decodeTxEntries(t.Entries)

	return t, err
}

// VerifiedTxByID returns a verified tx
func (c *immuClient) VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("VerifiedTxByID finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	vTx, err := c.ServiceClient.VerifiableTxById(ctx, &schema.VerifiableTxRequest{
		Tx:           tx,
		ProveSinceTx: state.TxId,
	})
	if err != nil {
		return nil, err
	}

	dualProof := schema.DualProofFrom(vTx.DualProof)

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	if state.TxId <= vTx.Tx.Metadata.Id {
		sourceID = state.TxId
		sourceAlh = schema.DigestFrom(state.TxHash)
		targetID = vTx.Tx.Metadata.Id
		targetAlh = dualProof.TargetTxMetadata.Alh()
	} else {
		sourceID = vTx.Tx.Metadata.Id
		sourceAlh = dualProof.SourceTxMetadata.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFrom(state.TxHash)
	}

	if state.TxId > 0 {
		verifies := store.VerifyDualProof(
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	decodeTxEntries(vTx.Tx.Entries)

	return vTx.Tx, nil
}

// TxScan ...
func (c *immuClient) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.TxScan(ctx, req)
}

// History ...
func (c *immuClient) History(ctx context.Context, req *schema.HistoryRequest) (sl *schema.Entries, err error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("history finished in %s", time.Since(start))

	return c.ServiceClient.History(ctx, req)
}

// SetReference ...
func (c *immuClient) SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return c.SetReferenceAt(ctx, key, referencedKey, 0)
}

// SetReferenceAt ...
func (c *immuClient) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("SetReference finished in %s", time.Since(start))

	txmd, err := c.ServiceClient.SetReference(ctx, &schema.ReferenceRequest{
		Key:           key,
		ReferencedKey: referencedKey,
		AtTx:          atTx,
		BoundRef:      atTx > 0,
	})
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// VerifiedSetReference ...
func (c *immuClient) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return c.VerifiedSetReferenceAt(ctx, key, referencedKey, 0)
}

// VerifiedSetReferenceAt ...
func (c *immuClient) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("safereference finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableReferenceRequest{
		ReferenceRequest: &schema.ReferenceRequest{
			Key:           key,
			ReferencedKey: referencedKey,
			AtTx:          atTx,
			BoundRef:      atTx > 0,
		},
		ProveSinceTx: state.TxId,
	}

	var metadata runtime.ServerMetadata

	verifiableTx, err := c.ServiceClient.VerifiableSetReference(
		ctx,
		req,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	if err != nil {
		return nil, err
	}

	if verifiableTx.Tx.Metadata.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFrom(verifiableTx.Tx)

	inclusionProof, err := tx.Proof(database.EncodeKey(key))
	if err != nil {
		return nil, err
	}

	verifies := store.VerifyInclusion(inclusionProof, database.EncodeReference(key, referencedKey, atTx), tx.Eh())
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Eh() != schema.DigestFrom(verifiableTx.DualProof.TargetTxMetadata.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFrom(state.TxHash)
	targetID = tx.ID
	targetAlh = tx.Alh

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFrom(verifiableTx.DualProof),
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Metadata, nil
}

// ZAdd ...
func (c *immuClient) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return c.ZAddAt(ctx, set, score, key, 0)
}

// ZAddAt ...
func (c *immuClient) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("zadd finished in %s", time.Since(start))

	txmd, err := c.ServiceClient.ZAdd(ctx, &schema.ZAddRequest{
		Set:      set,
		Score:    score,
		Key:      key,
		AtTx:     atTx,
		BoundRef: atTx > 0,
	})
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// ZAdd ...
func (c *immuClient) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return c.VerifiedZAddAt(ctx, set, score, key, 0)
}

// VerifiedZAdd ...
func (c *immuClient) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("safezadd finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableZAddRequest{
		ZAddRequest: &schema.ZAddRequest{
			Set:   set,
			Score: score,
			Key:   key,
			AtTx:  atTx,
		},
		ProveSinceTx: state.TxId,
	}

	var metadata runtime.ServerMetadata

	vtx, err := c.ServiceClient.VerifiableZAdd(
		ctx,
		req,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	if vtx.Tx.Metadata.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFrom(vtx.Tx)

	ekv := database.EncodeZAdd(req.ZAddRequest.Set,
		req.ZAddRequest.Score,
		database.EncodeKey(req.ZAddRequest.Key),
		req.ZAddRequest.AtTx,
	)

	inclusionProof, err := tx.Proof(ekv.Key)
	if err != nil {
		return nil, err
	}

	verifies := store.VerifyInclusion(inclusionProof, ekv, tx.Eh())
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Eh() != schema.DigestFrom(vtx.DualProof.TargetTxMetadata.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFrom(state.TxHash)
	targetID = tx.ID
	targetAlh = tx.Alh

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFrom(vtx.DualProof),
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if !verifies {
			return nil, store.ErrCorruptedData
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vtx.Signature,
	}

	if c.serverSigningPubKey != nil {
		ok, err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrCorruptedData
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vtx.Tx.Metadata, nil
}

// Dump to be used from Immu CLI
func (c *immuClient) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	return 0, errors.New("Functionality not yet supported")
}

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

func (c *immuClient) currentDatabase() string {
	if c.Options.CurrentDatabase == "" {
		return DefaultDB
	}
	return c.Options.CurrentDatabase
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

func (c *immuClient) CleanIndex(ctx context.Context, req *empty.Empty) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.CleanIndex(ctx, req)

	c.Logger.Debugf("CleanIndex finished in %s", time.Since(start))

	return err
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

// DEPRECATED: Please use CurrentState
func (c *immuClient) CurrentRoot(ctx context.Context) (*schema.ImmutableState, error) {
	return c.CurrentState(ctx)
}

// DEPRECATED: Please use VerifiedSet
func (c *immuClient) SafeSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	return c.VerifiedSet(ctx, key, value)
}

// DEPRECATED: Please use VerifiedGet
func (c *immuClient) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*schema.Entry, error) {
	return c.VerifiedGet(ctx, key)
}

// DEPRECATED: Please use VerifiedZAdd
func (c *immuClient) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return c.VerifiedZAdd(ctx, set, score, key)
}

// DEPRECATED: Please use VerifiedSetRefrence
func (c *immuClient) SafeReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return c.VerifiedSetReference(ctx, key, referencedKey)
}

func decodeTxEntries(entries []*schema.TxEntry) {
	for _, it := range entries {
		it.Key = it.Key[1:]
	}
}
