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
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

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
	PrintTree(ctx context.Context) (*schema.Tree, error)

	WithOptions(options *Options) *immuClient
	WithLogger(logger logger.Logger) *immuClient
	WithStateService(rs state.StateService) *immuClient
	WithTimestampService(ts TimestampService) *immuClient
	WithClientConn(clientConn *grpc.ClientConn) *immuClient
	WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient
	WithTokenService(tokenService TokenService) *immuClient

	GetServiceClient() *schema.ImmuServiceClient
	GetOptions() *Options
	SetupDialOptions(options *Options) *[]grpc.DialOption

	DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error)
	CreateDatabase(ctx context.Context, d *schema.Database) error
	UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error)
	SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error

	//

	CurrentState(ctx context.Context) (*schema.ImmutableState, error)

	Set(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error)
	VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error)

	Get(ctx context.Context, key []byte) (*schema.Entry, error)
	VerifiedGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*schema.Entry, error)

	GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error)
	VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error)

	ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error)
	VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error)

	Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)
	ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	TxByID(ctx context.Context, tx uint64) (*schema.Tx, error)
	VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error)

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
}

type immuClient struct {
	Dir           string
	Logger        logger.Logger
	Options       *Options
	clientConn    *grpc.ClientConn
	ServiceClient schema.ImmuServiceClient
	StateService  state.StateService
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
		return nil, logErr(l, "Unable to create program file folder: %s", err)
	}

	stateProvider := state.NewStateProvider(serviceClient)
	uuidProvider := state.NewUUIDProvider(serviceClient)

	stateService, err := state.NewStateService(cache.NewFileCache(options.Dir), l, stateProvider, uuidProvider)
	if err != nil {
		return nil, logErr(l, "Unable to create state service: %s", err)
	}

	dt, err := timestamp.NewDefaultTimestamp()
	if err != nil {
		return nil, err
	}

	ts := NewTimestampService(dt)
	c.WithTimestampService(ts).WithStateService(stateService)

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

	if options.Auth && c.Tkns != nil {
		token, err := c.Tkns.GetToken()
		if err == nil {
			opts = append(opts, grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)))
			opts = append(opts, grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)))
		}
	}

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
func (c *immuClient) VerifiedGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (vi *schema.Entry, err error) {
	c.Lock()
	defer c.Unlock()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	start := time.Now()
	defer c.Logger.Debugf("verifiedGet finished in %s", time.Since(start))

	state, err := c.StateService.GetState(ctx, c.Options.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableGetRequest{
		KeyRequest:   &schema.KeyRequest{Key: key, SinceTx: state.TxId},
		ProveSinceTx: state.TxId,
	}

	vEntry, err := c.ServiceClient.VerifiableGet(ctx, req, opts...)
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
		kv = database.EncodeKV(key, vEntry.Entry.Value)
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

	newState := &schema.ImmutableState{
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if newState.Signature != nil {
		ok, err := newState.CheckSignature()
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

	return c.ServiceClient.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}})
}

// VerifiedSet ...
func (c *immuClient) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	c.Lock()
	defer c.Unlock()

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

	if state.TxId == 0 {
		sourceID = tx.ID
		sourceAlh = tx.Alh
	} else {
		sourceID = state.TxId
		sourceAlh = schema.DigestFrom(state.TxHash)
	}

	targetID = tx.ID
	targetAlh = tx.Alh

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

	newState := &schema.ImmutableState{
		TxId:      tx.ID,
		TxHash:    tx.Alh[:],
		Signature: verifiableTx.Signature,
	}

	if newState.Signature != nil {
		ok, err := newState.CheckSignature()
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

	return c.ServiceClient.Set(ctx, req)
}

// ExecAll ...
func (c *immuClient) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	return c.ServiceClient.ExecAll(ctx, req)
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

	return c.ServiceClient.TxById(ctx, &schema.TxRequest{
		Tx: tx,
	})
}

// VerifiedTxByID returns a verified tx
func (c *immuClient) VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	c.Lock()
	defer c.Unlock()

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

	newState := &schema.ImmutableState{
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vTx.Signature,
	}

	if newState.Signature != nil {
		ok, err := newState.CheckSignature()
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

	return vTx.Tx, nil
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

	return c.ServiceClient.SetReference(ctx, &schema.ReferenceRequest{
		Key:           key,
		ReferencedKey: referencedKey,
		AtTx:          atTx,
	})
}

// VerifiedSetReference ...
func (c *immuClient) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return c.VerifiedSetReferenceAt(ctx, key, referencedKey, 0)
}

// VerifiedSetReferenceAt ...
func (c *immuClient) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error) {
	c.Lock()
	defer c.Unlock()

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
		},
		ProveSinceTx: state.TxId,
	}

	var metadata runtime.ServerMetadata

	vTx, err := c.ServiceClient.VerifiableSetReference(
		ctx,
		req,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	if err != nil {
		return nil, err
	}

	//TODO: verification and root update

	return vTx.Tx.Metadata, nil
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

	return c.ServiceClient.ZAdd(ctx, &schema.ZAddRequest{
		Set:   set,
		Score: score,
		Key:   key,
		AtTx:  atTx,
	})
}

// ZAdd ...
func (c *immuClient) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return c.VerifiedZAddAt(ctx, set, score, key, 0)
}

// VerifiedZAdd ...
func (c *immuClient) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error) {
	c.Lock()
	defer c.Unlock()

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

	result, err := c.ServiceClient.VerifiableZAdd(
		ctx,
		req,
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		return nil, err
	}

	/*
		verified, err := c.verifyAndSetRoot(result, root, ctx)
		if err != nil {
			return nil, err
		}
	*/

	return result.Tx.Metadata, nil
}

// Dump to be used from Immu CLI
func (c *immuClient) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	return 0, errors.New("Functionality not yet supported")
	/*	start := time.Now()

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
	*/
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

/*
func (c *immuClient) VerifyAndSetRoot(result *schema.DualProof, txMetadata *schema.TxMetadata, ctx context.Context) (bool, error)
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
*/

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
