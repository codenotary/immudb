/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/pkg/client/errors"

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

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// ImmuClient ...
type ImmuClient interface {
	Disconnect() error
	IsConnected() bool
	// Deprecated: grpc retry mechanism can be implemented with WithConnectParams dialOption
	WaitForHealthCheck(ctx context.Context) (err error)
	HealthCheck(ctx context.Context) error
	// Deprecated: connection is handled in OpenSession
	Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error)

	// Deprecated: use OpenSession
	Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error)
	// Deprecated: use CloseSession
	Logout(ctx context.Context) error

	OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error)
	CloseSession(ctx context.Context) error

	CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error
	ListUsers(ctx context.Context) (*schema.UserList, error)
	ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error
	ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error
	// Deprecated: will be removed in future versions
	UpdateAuthConfig(ctx context.Context, kind auth.Kind) error
	// Deprecated: will be removed in future versions
	UpdateMTLSConfig(ctx context.Context, enabled bool) error

	WithOptions(options *Options) *immuClient
	WithLogger(logger logger.Logger) *immuClient
	WithStateService(rs state.StateService) *immuClient
	// Deprecated: will be removed in future versions
	WithClientConn(clientConn *grpc.ClientConn) *immuClient
	// Deprecated: will be removed in future versions
	WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient
	WithTokenService(tokenService tokenservice.TokenService) *immuClient
	WithServerSigningPubKey(serverSigningPubKey *ecdsa.PublicKey) *immuClient
	WithStreamServiceFactory(ssf stream.ServiceFactory) *immuClient

	GetServiceClient() schema.ImmuServiceClient
	GetOptions() *Options
	SetupDialOptions(options *Options) []grpc.DialOption

	DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error)
	DatabaseListV2(ctx context.Context) (*schema.DatabaseListResponseV2, error)
	CreateDatabase(ctx context.Context, d *schema.DatabaseSettings) error
	CreateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error)
	LoadDatabase(ctx context.Context, r *schema.LoadDatabaseRequest) (*schema.LoadDatabaseResponse, error)
	UnloadDatabase(ctx context.Context, r *schema.UnloadDatabaseRequest) (*schema.UnloadDatabaseResponse, error)
	DeleteDatabase(ctx context.Context, r *schema.DeleteDatabaseRequest) (*schema.DeleteDatabaseResponse, error)
	UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error)
	UpdateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error
	UpdateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error)
	GetDatabaseSettings(ctx context.Context) (*schema.DatabaseSettings, error)
	GetDatabaseSettingsV2(ctx context.Context) (*schema.DatabaseSettingsResponse, error)

	SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error

	FlushIndex(ctx context.Context, cleanupPercentage float32, synced bool) (*schema.FlushIndexResponse, error)
	CompactIndex(ctx context.Context, req *empty.Empty) error

	Health(ctx context.Context) (*schema.DatabaseHealthResponse, error)
	CurrentState(ctx context.Context) (*schema.ImmutableState, error)

	Set(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error)
	VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error)

	ExpirableSet(ctx context.Context, key []byte, value []byte, expiresAt time.Time) (*schema.TxHeader, error)

	Get(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error)
	GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	GetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error)

	VerifiedGet(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error)
	VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)
	VerifiedGetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error)

	History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error)
	VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error)

	ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error)
	VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error)

	Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)
	ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	TxByID(ctx context.Context, tx uint64) (*schema.Tx, error)
	VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error)

	TxByIDWithSpec(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error)

	TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error)

	Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error)
	CountAll(ctx context.Context) (*schema.EntryCount, error)

	SetAll(ctx context.Context, kvList *schema.SetRequest) (*schema.TxHeader, error)
	GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error)

	Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error)

	ExecAll(ctx context.Context, in *schema.ExecAllRequest) (*schema.TxHeader, error)

	SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error)
	VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error)

	SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error)
	VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error)

	Dump(ctx context.Context, writer io.WriteSeeker) (int64, error)

	StreamSet(ctx context.Context, kv []*stream.KeyValue) (*schema.TxHeader, error)
	StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error)
	StreamVerifiedSet(ctx context.Context, kv []*stream.KeyValue) (*schema.TxHeader, error)
	StreamVerifiedGet(ctx context.Context, k *schema.VerifiableGetRequest) (*schema.Entry, error)
	StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)
	StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)
	StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)
	StreamExecAll(ctx context.Context, req *stream.ExecAllRequest) (*schema.TxHeader, error)

	ExportTx(ctx context.Context, req *schema.ExportTxRequest) (schema.ImmuService_ExportTxClient, error)
	ReplicateTx(ctx context.Context) (schema.ImmuService_ReplicateTxClient, error)

	SQLExec(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLExecResult, error)
	SQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error)
	ListTables(ctx context.Context) (*schema.SQLQueryResult, error)
	DescribeTable(ctx context.Context, tableName string) (*schema.SQLQueryResult, error)

	VerifyRow(ctx context.Context, row *schema.Row, table string, pkVals []*schema.SQLValue) error

	NewTx(ctx context.Context) (Tx, error)
}

const DefaultDB = "defaultdb"

type immuClient struct {
	Dir                  string
	Logger               logger.Logger
	Options              *Options
	clientConn           *grpc.ClientConn
	ServiceClient        schema.ImmuServiceClient
	StateService         state.StateService
	Tkns                 tokenservice.TokenService
	serverSigningPubKey  *ecdsa.PublicKey
	StreamServiceFactory stream.ServiceFactory
	SessionID            string
	HeartBeater          heartbeater.HeartBeater
}

// Ensure immuClient implements the ImmuClient interface
var _ ImmuClient = &immuClient{}

// NewClient ...
func NewClient() *immuClient {
	c := &immuClient{
		Dir:                  "",
		Options:              DefaultOptions(),
		Logger:               logger.NewSimpleLogger("immuclient", os.Stderr),
		StreamServiceFactory: stream.NewStreamServiceFactory(DefaultOptions().StreamChunkSize),
		Tkns:                 tokenservice.NewInmemoryTokenService(),
	}
	return c
}

// NewImmuClient ...
// Deprecated: use NewClient instead.
func NewImmuClient(options *Options) (*immuClient, error) {
	ctx := context.Background()

	c := NewClient()
	c.WithOptions(options)
	l := logger.NewSimpleLogger("immuclient", os.Stderr)
	c.WithLogger(l)

	if options.ServerSigningPubKey != "" {
		pk, err := signer.ParsePublicKeyFile(options.ServerSigningPubKey)
		if err != nil {
			return nil, err
		}
		c.WithServerSigningPubKey(pk)
	}

	options.DialOptions = c.SetupDialOptions(options)
	if db, err := c.Tkns.GetDatabase(); err == nil && len(db) > 0 {
		options.CurrentDatabase = db
	}

	if options.StreamChunkSize < stream.MinChunkSize {
		return nil, errors.New(stream.ErrChunkTooSmall).WithCode(errors.CodInvalidParameterValue)
	}

	c.WithOptions(options)

	clientConn, err := c.Connect(ctx)
	if err != nil {
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

func (c *immuClient) SetupDialOptions(options *Options) []grpc.DialOption {
	opts := options.DialOptions
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
	uic = append(uic, c.IllegalStateHandlerInterceptor, c.TokenInterceptor)

	if options.Auth && c.Tkns != nil {
		token, err := c.Tkns.GetToken()
		uic = append(uic, auth.ClientUnaryInterceptor(token))
		if err == nil {
			// todo here is possible to remove ClientUnaryInterceptor and use only tokenInterceptor
			opts = append(opts, grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)), grpc.WithStreamInterceptor(c.SessionIDInjectorStreamInterceptor))
		}
	}
	uic = append(uic, c.SessionIDInjectorInterceptor)

	opts = append(opts, grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(uic...)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.MaxRecvMsgSize)))

	return opts
}

// Deprecated: use NewClient and OpenSession instead.
func (c *immuClient) Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error) {
	if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err != nil {
		c.Logger.Debugf("dialed %v", c.Options)
		return nil, err
	}
	return c.clientConn, nil
}

// Deprecated: use NewClient and CloseSession instead.
func (c *immuClient) Disconnect() error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
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
func (c *immuClient) GetServiceClient() schema.ImmuServiceClient {
	return c.ServiceClient
}

func (c *immuClient) GetOptions() *Options {
	return c.Options
}

// Deprecated: use user list instead
func (c *immuClient) ListUsers(ctx context.Context) (*schema.UserList, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.ListUsers(ctx, new(empty.Empty))
}

// CreateUser ...
func (c *immuClient) CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
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
		return errors.FromError(ErrNotConnected)
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
		return errors.FromError(ErrNotConnected)
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
		return errors.FromError(ErrNotConnected)
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
		return nil, errors.FromError(ErrNotConnected)
	}

	result, err := c.ServiceClient.Login(ctx, &schema.LoginRequest{
		User:     user,
		Password: pass,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}
	c.Logger.Debugf("login finished in %s", time.Since(start))

	err = c.Tkns.SetToken("defaultdb", result.Token)
	if err != nil {
		return nil, errors.FromError(err)
	}

	return result, nil
}

// Logout ...
func (c *immuClient) Logout(ctx context.Context) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	if _, err := c.ServiceClient.Logout(ctx, new(empty.Empty)); err != nil {
		return err
	}

	tokenFileExists, err := c.Tkns.IsTokenPresent()
	if err != nil {
		return fmt.Errorf("error checking if token file exists: %v", err)
	}
	if tokenFileExists {
		if err := c.Tkns.DeleteToken(); err != nil {
			return fmt.Errorf("error deleting token file during logout: %v", err)
		}
	}

	c.Logger.Debugf("logout finished in %s", time.Since(start))

	return nil
}

func (c *immuClient) Health(ctx context.Context) (*schema.DatabaseHealthResponse, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.DatabaseHealth(ctx, &empty.Empty{})
}

// CurrentState returns current database state
func (c *immuClient) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.Logger.Debugf("Current state finished in %s", time.Since(start))

	return c.ServiceClient.CurrentState(ctx, &empty.Empty{})
}

// Get ...
func (c *immuClient) Get(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.Logger.Debugf("get finished in %s", time.Since(start))

	req := &schema.KeyRequest{Key: key}
	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return nil, err
		}
	}

	return c.ServiceClient.Get(ctx, req)
}

// GetSince ...
func (c *immuClient) GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return c.Get(ctx, key, SinceTx(tx))
}

// GetAt ...
func (c *immuClient) GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return c.Get(ctx, key, AtTx(tx))
}

// GetAtRevision ...
func (c *immuClient) GetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error) {
	return c.Get(ctx, key, AtRevision(rev))
}

// VerifiedGet ...
func (c *immuClient) VerifiedGet(ctx context.Context, key []byte, opts ...GetOption) (vi *schema.Entry, err error) {
	start := time.Now()
	defer c.Logger.Debugf("VerifiedGet finished in %s", time.Since(start))

	req := &schema.KeyRequest{Key: key}
	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return nil, err
		}
	}

	return c.verifiedGet(ctx, req)
}

// VerifiedGetSince ...
func (c *immuClient) VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, SinceTx(tx))
}

// VerifiedGetAt ...
func (c *immuClient) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, AtTx(tx))
}

// VerifiedGetAtRevision ...
func (c *immuClient) VerifiedGetAtRevision(ctx context.Context, key []byte, rev int64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, AtRevision(rev))
}

func (c *immuClient) verifiedGet(ctx context.Context, kReq *schema.KeyRequest) (vi *schema.Entry, err error) {
	err = c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

	entrySpecDigest, err := store.EntrySpecDigestFor(int(vEntry.VerifiableTx.Tx.Header.Version))
	if err != nil {
		return nil, err
	}

	inclusionProof := schema.InclusionProofFromProto(vEntry.InclusionProof)
	dualProof := schema.DualProofFromProto(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	vTx := kReq.AtTx
	var e *store.EntrySpec

	if vEntry.Entry.ReferencedBy == nil {
		if kReq.AtTx == 0 {
			vTx = vEntry.Entry.Tx
		}

		e = database.EncodeEntrySpec(kReq.Key, schema.KVMetadataFromProto(vEntry.Entry.Metadata), vEntry.Entry.Value)
	} else {
		ref := vEntry.Entry.ReferencedBy

		if kReq.AtTx == 0 {
			vTx = ref.Tx
		}

		e = database.EncodeReference(kReq.Key, schema.KVMetadataFromProto(ref.Metadata), vEntry.Entry.Key, ref.AtTx)
	}

	if state.TxId <= vTx {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.TargetTxHeader.EH)

		sourceID = state.TxId
		sourceAlh = schema.DigestFromProto(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxHeader.Alh()
	} else {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.SourceTxHeader.EH)

		sourceID = vTx
		sourceAlh = dualProof.SourceTxHeader.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFromProto(state.TxHash)
	}

	verifies := store.VerifyInclusion(
		inclusionProof,
		entrySpecDigest(e),
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

// Scan ...
func (c *immuClient) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.Scan(ctx, req)
}

// ZScan ...
func (c *immuClient) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.ZScan(ctx, req)
}

// Count ...
func (c *immuClient) Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.Count(ctx, &schema.KeyPrefix{Prefix: prefix})
}

// CountAll ...
func (c *immuClient) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.CountAll(ctx, new(empty.Empty))
}

// Set ...
func (c *immuClient) Set(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return c.set(ctx, key, nil, value)
}

func (c *immuClient) set(ctx context.Context, key []byte, md *schema.KVMetadata, value []byte) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	txmd, err := c.ServiceClient.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Metadata: md, Value: value}}})
	if err != nil {
		return nil, err
	}

	if int(txmd.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return txmd, nil
}

// VerifiedSet ...
func (c *immuClient) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

	if verifiableTx.Tx.Header.Nentries != 1 || len(verifiableTx.Tx.Entries) != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFromProto(verifiableTx.Tx)

	entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := tx.Proof(database.EncodeKey(key))
	if err != nil {
		return nil, err
	}

	md := tx.Entries()[0].Metadata()

	if md != nil && md.Deleted() {
		return nil, store.ErrCorruptedData
	}

	e := database.EncodeEntrySpec(key, md, value)

	verifies := store.VerifyInclusion(inclusionProof, entrySpecDigest(e), tx.Header().Eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Header().Eh != schema.DigestFromProto(verifiableTx.DualProof.TargetTxHeader.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFromProto(state.TxHash)
	targetID = tx.Header().ID
	targetAlh = tx.Header().Alh()

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFromProto(verifiableTx.DualProof),
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

	return verifiableTx.Tx.Header, nil
}

func (c *immuClient) ExpirableSet(ctx context.Context, key []byte, value []byte, expiresAt time.Time) (*schema.TxHeader, error) {
	return c.set(ctx, key, &schema.KVMetadata{Expiration: &schema.Expiration{ExpiresAt: expiresAt.Unix()}}, value)
}

func (c *immuClient) SetAll(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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
func (c *immuClient) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	txhdr, err := c.ServiceClient.ExecAll(ctx, req)
	if err != nil {
		return nil, err
	}

	if int(txhdr.Nentries) != len(req.Operations) {
		return nil, store.ErrCorruptedData
	}

	return txhdr, nil
}

// GetAll ...
func (c *immuClient) GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.Logger.Debugf("get-batch finished in %s", time.Since(start))

	keyList := &schema.KeyListRequest{}

	keyList.Keys = append(keyList.Keys, keys...)

	return c.ServiceClient.GetAll(ctx, keyList)
}

func (c *immuClient) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.Delete(ctx, req)
}

// TxByID ...
func (c *immuClient) TxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

func (c *immuClient) TxByIDWithSpec(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.TxById(ctx, req)
}

// VerifiedTxByID returns a verified tx
func (c *immuClient) VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

	dualProof := schema.DualProofFromProto(vTx.DualProof)

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	if state.TxId <= tx {
		sourceID = state.TxId
		sourceAlh = schema.DigestFromProto(state.TxHash)
		targetID = tx
		targetAlh = dualProof.TargetTxHeader.Alh()
	} else {
		sourceID = tx
		sourceAlh = dualProof.SourceTxHeader.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFromProto(state.TxHash)
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
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.TxScan(ctx, req)
}

// History ...
func (c *immuClient) History(ctx context.Context, req *schema.HistoryRequest) (sl *schema.Entries, err error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.Logger.Debugf("history finished in %s", time.Since(start))

	return c.ServiceClient.History(ctx, req)
}

// SetReference ...
func (c *immuClient) SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return c.SetReferenceAt(ctx, key, referencedKey, 0)
}

// SetReferenceAt ...
func (c *immuClient) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.Logger.Debugf("SetReference finished in %s", time.Since(start))

	txhdr, err := c.ServiceClient.SetReference(ctx, &schema.ReferenceRequest{
		Key:           key,
		ReferencedKey: referencedKey,
		AtTx:          atTx,
		BoundRef:      atTx > 0,
	})
	if err != nil {
		return nil, err
	}

	if int(txhdr.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return txhdr, nil
}

// VerifiedSetReference ...
func (c *immuClient) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return c.VerifiedSetReferenceAt(ctx, key, referencedKey, 0)
}

// VerifiedSetReferenceAt ...
func (c *immuClient) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

	if verifiableTx.Tx.Header.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFromProto(verifiableTx.Tx)

	entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := tx.Proof(database.EncodeKey(key))
	if err != nil {
		return nil, err
	}

	e := database.EncodeReference(key, nil, referencedKey, atTx)

	verifies := store.VerifyInclusion(inclusionProof, entrySpecDigest(e), tx.Header().Eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Header().Eh != schema.DigestFromProto(verifiableTx.DualProof.TargetTxHeader.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFromProto(state.TxHash)
	targetID = tx.Header().ID
	targetAlh = tx.Header().Alh()

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFromProto(verifiableTx.DualProof),
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

	return verifiableTx.Tx.Header, nil
}

// ZAdd ...
func (c *immuClient) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return c.ZAddAt(ctx, set, score, key, 0)
}

// ZAddAt ...
func (c *immuClient) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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
func (c *immuClient) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return c.VerifiedZAddAt(ctx, set, score, key, 0)
}

// VerifiedZAdd ...
func (c *immuClient) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error) {
	err := c.StateService.CacheLock()
	if err != nil {
		return nil, err
	}
	defer c.StateService.CacheUnlock()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
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

	if vtx.Tx.Header.Nentries != 1 {
		return nil, store.ErrCorruptedData
	}

	tx := schema.TxFromProto(vtx.Tx)

	entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
	if err != nil {
		return nil, err
	}

	ekv := database.EncodeZAdd(req.ZAddRequest.Set,
		req.ZAddRequest.Score,
		database.EncodeKey(req.ZAddRequest.Key),
		req.ZAddRequest.AtTx,
	)

	inclusionProof, err := tx.Proof(ekv.Key)
	if err != nil {
		return nil, err
	}

	verifies := store.VerifyInclusion(inclusionProof, entrySpecDigest(ekv), tx.Header().Eh)
	if !verifies {
		return nil, store.ErrCorruptedData
	}

	if tx.Header().Eh != schema.DigestFromProto(vtx.DualProof.TargetTxHeader.EH) {
		return nil, store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFromProto(state.TxHash)
	targetID = tx.Header().ID
	targetAlh = tx.Header().Alh()

	if state.TxId > 0 {
		verifies = store.VerifyDualProof(
			schema.DualProofFromProto(vtx.DualProof),
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

	return vtx.Tx.Header, nil
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
func (c *immuClient) CreateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.CreateDatabaseWith(ctx, settings)

	c.Logger.Debugf("CreateDatabase finished in %s", time.Since(start))

	return err
}

// CreateDatabaseV2 create a new database by making a grpc call
func (c *immuClient) CreateDatabaseV2(ctx context.Context, name string, settings *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{
		Name:     name,
		Settings: settings,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}

	c.Logger.Debugf("CreateDatabase finished in %s", time.Since(start))

	return res, nil
}

// LoadDatabase open an existent database by making a grpc call
func (c *immuClient) LoadDatabase(ctx context.Context, r *schema.LoadDatabaseRequest) (*schema.LoadDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.LoadDatabase(ctx, r)

	c.Logger.Debugf("LoadDatabase finished in %s", time.Since(start))

	return res, err
}

// UnloadDatabase closes an existent database by making a grpc call
func (c *immuClient) UnloadDatabase(ctx context.Context, r *schema.UnloadDatabaseRequest) (*schema.UnloadDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.UnloadDatabase(ctx, r)

	c.Logger.Debugf("UnloadDatabase finished in %s", time.Since(start))

	return res, err
}

// DeleteDatabase deletes an existent database by making a grpc call
func (c *immuClient) DeleteDatabase(ctx context.Context, r *schema.DeleteDatabaseRequest) (*schema.DeleteDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.DeleteDatabase(ctx, r)

	c.Logger.Debugf("DeleteDatabase finished in %s", time.Since(start))

	return res, err
}

// UseDatabase set database in use by making a grpc call
func (c *immuClient) UseDatabase(ctx context.Context, db *schema.Database) (*schema.UseDatabaseReply, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	result, err := c.ServiceClient.UseDatabase(ctx, db)
	if err != nil {
		return nil, errors.FromError(err)
	}

	c.Options.CurrentDatabase = db.DatabaseName

	if c.SessionID == "" {
		if err = c.Tkns.SetToken(db.DatabaseName, result.Token); err != nil {
			return nil, errors.FromError(err)
		}
	}

	c.Logger.Debugf("UseDatabase finished in %s", time.Since(start))

	return result, errors.FromError(err)
}

// UpdateDatabase updates database settings
func (c *immuClient) UpdateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.UpdateDatabase(ctx, settings)

	c.Logger.Debugf("UpdateDatabase finished in %s", time.Since(start))

	return err
}

// UpdateDatabaseV2 updates database settings
func (c *immuClient) UpdateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
		Database: database,
		Settings: settings,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}

	c.Logger.Debugf("UpdateDatabase finished in %s", time.Since(start))

	return res, err
}

func (c *immuClient) GetDatabaseSettings(ctx context.Context) (*schema.DatabaseSettings, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.GetDatabaseSettings(ctx, &empty.Empty{})
}

func (c *immuClient) GetDatabaseSettingsV2(ctx context.Context) (*schema.DatabaseSettingsResponse, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.GetDatabaseSettingsV2(ctx, &schema.DatabaseSettingsRequest{})
	if err != nil {
		return nil, errors.FromError(err)
	}

	return res, nil
}

func (c *immuClient) FlushIndex(ctx context.Context, cleanupPercentage float32, synced bool) (*schema.FlushIndexResponse, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	res, err := c.ServiceClient.FlushIndex(ctx, &schema.FlushIndexRequest{
		CleanupPercentage: cleanupPercentage,
		Synced:            synced,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}

	return res, nil
}

func (c *immuClient) CompactIndex(ctx context.Context, req *empty.Empty) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	_, err := c.ServiceClient.CompactIndex(ctx, req)

	c.Logger.Debugf("CompactIndex finished in %s", time.Since(start))

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
		return nil, errors.FromError(ErrNotConnected)
	}

	result, err := c.ServiceClient.DatabaseList(ctx, &empty.Empty{})

	c.Logger.Debugf("DatabaseList finished in %s", time.Since(start))

	return result, err
}

func (c *immuClient) DatabaseListV2(ctx context.Context) (*schema.DatabaseListResponseV2, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.DatabaseListV2(ctx, &schema.DatabaseListRequestV2{})
}

// DEPRECATED: Please use CurrentState
func (c *immuClient) CurrentRoot(ctx context.Context) (*schema.ImmutableState, error) {
	return c.CurrentState(ctx)
}

// DEPRECATED: Please use VerifiedSet
func (c *immuClient) SafeSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return c.VerifiedSet(ctx, key, value)
}

// DEPRECATED: Please use VerifiedGet
func (c *immuClient) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*schema.Entry, error) {
	return c.VerifiedGet(ctx, key)
}

// DEPRECATED: Please use VerifiedZAdd
func (c *immuClient) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return c.VerifiedZAdd(ctx, set, score, key)
}

// DEPRECATED: Please use VerifiedSetReference
func (c *immuClient) SafeReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return c.VerifiedSetReference(ctx, key, referencedKey)
}

func decodeTxEntries(entries []*schema.TxEntry) {
	for _, it := range entries {
		it.Key = it.Key[1:]
	}
}
