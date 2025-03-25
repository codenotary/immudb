/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This package contains the official implementation of the go client for the immudb database.
//
// Please refer to documentation at https://docs.immudb.io for more details on how to use this SDK.
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

	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/codenotary/immudb/pkg/stream"
)

// ImmuClient is an interface that represents immudb client.
// Instances returned from NewClient or NewImmuClient methods implement this interface.
type ImmuClient interface {

	// Disconnect closes the current connection to the server.
	//
	// Deprecated: use NewClient and CloseSession instead.
	Disconnect() error

	// IsConnected checks whether the client is connected to the server.
	IsConnected() bool

	// WaitForHealthCheck waits for up to Options.HealthCheckRetries seconds to
	// get a successful HealthCheck response from the server.
	//
	// Deprecated: grpc retry mechanism can be implemented with WithConnectParams dialOption.
	WaitForHealthCheck(ctx context.Context) error

	// Get server health information.
	//
	// Deprecated: use ServerInfo.
	HealthCheck(ctx context.Context) error

	// Connect establishes new connection to the server.
	//
	// Deprecated: use NewClient and OpenSession instead.
	Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error)

	// Login authenticates the user in an established connection.
	//
	// Deprecated: use NewClient and OpenSession instead.
	Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error)

	// Logout logs out the user.
	//
	// Deprecated: use CloseSession.
	Logout(ctx context.Context) error

	// OpenSession establishes a new session with the server, this method also opens new
	// connection to the server.
	//
	// Note: it is important to call CloseSession() once the session is no longer needed.
	OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error)

	// CloseSession closes the current session and the connection to the server,
	// this call also allows the server to free up all resources allocated for a session
	// (without explicit call, the server will only free resources after session inactivity timeout).
	CloseSession(ctx context.Context) error

	GetSessionID() string

	// CreateUser creates new user with given credentials and permission.
	//
	// Required user permission is SysAdmin or database Admin.
	//
	// SysAdmin user can create users with access to any database.
	//
	// Admin user can only create users with access to databases where
	// the user has admin permissions.
	//
	// The permission argument is the permission level and can be one of those values:
	//   - 1 (auth.PermissionR) - read-only access
	//   - 2 (auth.PermissionRW) - read-write access
	//   - 254 (auth.PermissionAdmin) - read-write with admin rights
	CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error

	// ListUser returns a list of database users.
	//
	// This call requires Admin or SysAdmin permission level.
	//
	// When called as a SysAdmin user, all users in the database are returned.
	// When called as an Admin user, users for currently selected database are returned.
	ListUsers(ctx context.Context) (*schema.UserList, error)

	// ChangePassword changes password for existing user.
	//
	// This call requires Admin or SysAdmin permission level.
	//
	// The oldPass argument is only necessary when changing SysAdmin user's password.
	ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error

	// ChangePermission grants or revokes permission to one database for given user.
	//
	// This call requires SysAdmin or admin permission to the database where we grant permissions.
	//
	// The permission argument is used when granting permission and can be one of those values:
	//   - 1 (auth.PermissionR) - read-only access
	//   - 2 (auth.PermissionRW) - read-write access
	//   - 254 (auth.PermissionAdmin) - read-write with admin rights
	//
	// The following restrictions are applied:
	//  - the user can not change permission for himself
	//  - can not change permissions of the SysAdmin user
	//  - the user must be active
	//  - when the user already had permission to the database, it is overwritten
	//    by the new permission (even if the user had higher permission before)
	ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error

	// UpdateAuthConfig is no longer supported.
	//
	// Deprecated: will be removed in future versions.
	UpdateAuthConfig(ctx context.Context, kind auth.Kind) error

	// UpdateMTLSConfig is no longer supported.
	//
	// Deprecated: will be removed in future versions.
	UpdateMTLSConfig(ctx context.Context, enabled bool) error

	// WithOptions sets up client options for the instance.
	WithOptions(options *Options) *immuClient

	// WithLogger sets up custom client logger.
	WithLogger(logger logger.Logger) *immuClient

	// WithStateService sets up the StateService object.
	WithStateService(rs state.StateService) *immuClient

	// Deprecated: will be removed in future versions.
	WithClientConn(clientConn *grpc.ClientConn) *immuClient

	// Deprecated: will be removed in future versions.
	WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient

	// Deprecated: will be removed in future versions.
	WithTokenService(tokenService tokenservice.TokenService) *immuClient

	// WithServerSigningPubKey sets up public key for server's state validation.
	WithServerSigningPubKey(serverSigningPubKey *ecdsa.PublicKey) *immuClient

	// WithStreamServiceFactory sets up stream factory for the client.
	WithStreamServiceFactory(ssf stream.ServiceFactory) *immuClient

	// GetServiceClient returns low-level GRPC service client.
	GetServiceClient() schema.ImmuServiceClient

	// GetOptions returns current client options.
	GetOptions() *Options

	// SetupDialOptions extracts grpc dial options from provided client options.
	SetupDialOptions(options *Options) []grpc.DialOption

	// Return list of databases the user has access to.
	//
	// Deprecated: Use DatabaseListV2.
	DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error)

	// DatabaseListV2 returns a list of databases the user has access to.
	DatabaseListV2(ctx context.Context) (*schema.DatabaseListResponseV2, error)

	// CreateDatabase creates new database.
	// This call requires SysAdmin permission level.
	//
	// Deprecated: Use CreateDatabaseV2.
	CreateDatabase(ctx context.Context, d *schema.DatabaseSettings) error

	// CreateDatabaseV2 creates a new database.
	// This call requires SysAdmin permission level.
	CreateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error)

	// LoadDatabase loads database on the server. A database is not loaded
	// if it has AutoLoad setting set to false or if it failed to load during
	// immudb startup.
	//
	// This call requires SysAdmin permission level or admin permission to the database.
	LoadDatabase(ctx context.Context, r *schema.LoadDatabaseRequest) (*schema.LoadDatabaseResponse, error)

	// UnloadDatabase unloads database on the server. Such database becomes inaccessible
	// by the client and server frees internal resources allocated for that database.
	//
	// This call requires SysAdmin permission level or admin permission to the database.
	UnloadDatabase(ctx context.Context, r *schema.UnloadDatabaseRequest) (*schema.UnloadDatabaseResponse, error)

	// DeleteDatabase removes an unloaded database.
	// This also removes locally stored files used by the database.
	//
	// This call requires SysAdmin permission level or admin permission to the database.
	DeleteDatabase(ctx context.Context, r *schema.DeleteDatabaseRequest) (*schema.DeleteDatabaseResponse, error)

	// UseDatabase changes the currently selected database.
	//
	// This call requires at least read permission level for the target database.
	UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error)

	// UpdateDatabase updates database settings.
	//
	// Deprecated: Use UpdateDatabaseV2.
	UpdateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error

	// UpdateDatabaseV2 updates database settings.
	//
	// Settings can be set selectively - values not set in the settings object
	// will not be updated.
	//
	// The returned value is the list of settings after the update.
	//
	// Settings other than those related to replication will only be applied after
	// immudb restart or unload/load cycle of the database.
	UpdateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error)

	// Deprecated: Use GetDatabaseSettingsV2.
	GetDatabaseSettings(ctx context.Context) (*schema.DatabaseSettings, error)

	// GetDatabaseSettingsV2 retrieves current persisted database settings.
	GetDatabaseSettingsV2(ctx context.Context) (*schema.DatabaseSettingsResponse, error)

	// SetActiveUser activates or deactivates a user.
	SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error

	// FlushIndex requests a flush operation from the database.
	// This call requires SysAdmin or Admin permission to given database.
	//
	// The cleanupPercentage value is the amount of index nodes data in percent
	// that will be scanned in order to free up unused disk space.
	FlushIndex(ctx context.Context, cleanupPercentage float32, synced bool) (*schema.FlushIndexResponse, error)

	// CompactIndex perform full database compaction.
	// This call requires SysAdmin or Admin permission to given database.
	//
	// Note: Full compaction will greatly affect the performance of the database.
	// It should also be called only when there's a minimal database activity,
	// if full compaction collides with a read or write operation, it will be aborted
	// and may require retry of the whole operation. For that reason it is preferred
	// to periodically call FlushIndex with a small value of cleanupPercentage or set the
	// cleanupPercentage database option.
	CompactIndex(ctx context.Context, req *empty.Empty) error

	// ServerInfo returns information about the server instance.
	ServerInfo(ctx context.Context, req *schema.ServerInfoRequest) (*schema.ServerInfoResponse, error)

	// Health returns Health information about the current database.
	//
	// Requires read access to the database.
	Health(ctx context.Context) (*schema.DatabaseHealthResponse, error)

	// CurrentState returns the current state value from the server for the current database.
	CurrentState(ctx context.Context) (*schema.ImmutableState, error)

	// Set commits a change of a value for a single key.
	Set(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error)

	// VerifiedSet commits a change of a value for a single key.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error)

	// ExpirableSet commits a change of a value for a single key and sets up the expiration
	// time for that value after which the value will no longer be retrievable.
	ExpirableSet(ctx context.Context, key []byte, value []byte, expiresAt time.Time) (*schema.TxHeader, error)

	// Get reads a single value for given key.
	Get(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error)

	// GetSince reads a single value for given key assuming that at least transaction `tx` was indexed.
	// For more information about getting value with sinceTx constraint see the SinceTx get option.
	GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	// GetAt reads a single value that was modified at a specific transaction.
	// For more information about getting value from specific revision see the AtTx get option.
	GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	// GetAtRevision reads value for given key by its revision.
	// For more information about the revisions see the AtRevision get option.
	GetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error)

	// Gets reads a single value for given key with additional server-provided proof validation.
	VerifiedGet(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error)

	// VerifiedGetSince reads a single value for given key assuming that at least transaction `tx` was indexed.
	// For more information about getting value with sinceTx constraint see the SinceTx get option.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	// VerifiedGetAt reads a single value that was modified at a specific transaction.
	// For more information about getting value from specific revision see the AtTx get option.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error)

	// VerifiedGetAtRevision reads value for given key by its revision.
	// For more information about the revisions see the AtRevision get option.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedGetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error)

	// VerifiableGet reads value for a given key, and returs internal data used to perform
	// the verification.
	//
	// You can use this function if you want to have visibility on the verification data
	VerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error)

	// History returns history for a single key.
	History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	// ZAdd adds a new entry to sorted set.
	// New entry is a reference to some other key's value with additional score used for ordering set members.
	ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error)

	// VerifiedZAdd adds a new entry to sorted set.
	// New entry is a reference to some other key's value
	// with additional score used for ordering set members.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error)

	// ZAddAt adds a new entry to sorted set.
	// New entry is a reference to some other key's value at a specific transaction
	// with additional score used for ordering set members.
	ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error)

	// VerifiedZAddAt adds a new entry to sorted set.
	// New entry is a reference to some other key's value at a specific transaction
	// with additional score used for ordering set members.
	//
	// This function also requests a server-generated proof, verifies the entry in the transaction
	// using the proof and verifies the signature of the signed state.
	// If verification does not succeed the store.ErrCorruptedData error is returned.
	VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error)

	// Scan iterates over the set of keys in a topological order.
	Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)

	// ZScan iterates over the elements of sorted set ordered by their score.
	ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	// TxByID retrieves all entries (in a raw, unprocessed form) for given transaction.
	//
	// Note: In order to read keys and values, it is necessary to parse returned entries
	// TxByIDWithSpec can be used to read already-parsed values
	TxByID(ctx context.Context, tx uint64) (*schema.Tx, error)

	// TxByID retrieves all entries (in a raw, unprocessed form) for given transaction
	// and performs verification of the server-provided proof for the whole transaction.
	VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error)

	// TxByIDWithSpec retrieves entries from given transaction according to given spec.
	TxByIDWithSpec(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error)

	// TxScan returns raw entries for a range of transactions.
	TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error)

	// Count returns count of key-value entries with given prefix.
	//
	// Note: This feature is not implemented yet.
	Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error)

	// Count returns count of all key-value entries.
	//
	// Note: This feature is not implemented yet.
	CountAll(ctx context.Context) (*schema.EntryCount, error)

	// SetAll sets multiple entries in a single transaction.
	SetAll(ctx context.Context, kvList *schema.SetRequest) (*schema.TxHeader, error)

	// GetAll retrieves multiple entries in a single call.
	GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error)

	// Delete performs a logical deletion for a list of keys marking them as deleted.
	Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error)

	// ExecAll performs multiple write operations (values, references, sorted set entries)
	// in a single transaction.
	ExecAll(ctx context.Context, in *schema.ExecAllRequest) (*schema.TxHeader, error)

	// SetReference creates a reference to another key's value.
	//
	// Note: references can only be created to non-reference keys.
	SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error)

	// VerifiedSetReference creates a reference to another key's value and verifies server-provided
	// proof for the write.
	//
	// Note: references can only be created to non-reference keys.
	VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error)

	// SetReference creates a reference to another key's value at a specific transaction.
	//
	// Note: references can only be created to non-reference keys.
	SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error)

	// SetReference creates a reference to another key's value at a specific transaction and verifies server-provided
	// proof for the write.
	//
	// Note: references can only be created to non-reference keys.
	VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error)

	// Dump is currently not implemented.
	Dump(ctx context.Context, writer io.WriteSeeker) (int64, error)

	// StreamSet performs a write operation of a value for a single key retrieving key and value form io.Reader streams.
	StreamSet(ctx context.Context, kv []*stream.KeyValue) (*schema.TxHeader, error)

	// StreamGet retrieves a single entry for a key read from an io.Reader stream.
	StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error)

	// StreamVerifiedSet performs a write operation of a value for a single key retrieving key and value form io.Reader streams
	// with additional verification of server-provided write proof.
	StreamVerifiedSet(ctx context.Context, kv []*stream.KeyValue) (*schema.TxHeader, error)

	// StreamVerifiedGet retrieves a single entry for a key read from an io.Reader stream
	// with additional verification of server-provided value proof.
	StreamVerifiedGet(ctx context.Context, k *schema.VerifiableGetRequest) (*schema.Entry, error)

	// StreamScan scans for keys with given prefix, using stream API to overcome limits of large keys and values.
	StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)

	// StreamZScan scans entries from given sorted set, using stream API to overcome limits of large keys and values.
	StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	// StreamHistory returns a history of given key, using stream API to overcome limits of large keys and values.
	StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	// StreamExecAll performs an ExecAll operation (write operation for multiple data types in a single transaction)
	// using stream API to overcome limits of large keys and values.
	StreamExecAll(ctx context.Context, req *stream.ExecAllRequest) (*schema.TxHeader, error)

	// ExportTx retrieves serialized transaction object.
	ExportTx(ctx context.Context, req *schema.ExportTxRequest) (schema.ImmuService_ExportTxClient, error)

	// ReplicateTx sends a previously serialized transaction object replicating it on another database.
	ReplicateTx(ctx context.Context) (schema.ImmuService_ReplicateTxClient, error)

	// StreamExportTx provides a bidirectional endpoint for retrieving serialized transactions
	StreamExportTx(ctx context.Context, opts ...grpc.CallOption) (schema.ImmuService_StreamExportTxClient, error)

	// SQLExec performs a modifying SQL query within the transaction.
	// Such query does not return SQL result.
	SQLExec(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLExecResult, error)

	// SQLQuery performs a query (read-only) operation.
	//
	// Deprecated: use SQLQueryReader instead.
	// The renewSnapshot parameter is deprecated and  is ignored by the server.
	SQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error)

	// SQLQueryReader submits an SQL query to the server and returns a reader object for efficient retrieval of all rows in the result set.
	SQLQueryReader(ctx context.Context, sql string, params map[string]interface{}) (SQLQueryRowReader, error)

	// ListTables returns a list of SQL tables.
	ListTables(ctx context.Context) (*schema.SQLQueryResult, error)

	// Describe table returns a description of a table structure.
	DescribeTable(ctx context.Context, tableName string) (*schema.SQLQueryResult, error)

	// VerifyRow reads a single row from the database with additional validation of server-provided proof.
	//
	// The row parameter should contain row from a single table, either returned from
	// query or manually assembled. The table parameter contains the name of the table
	// where the row comes from. The pkVals argument is an array containing values for
	// the primary key of the row. The row parameter does not have to contain all
	// columns of the table. Once the row itself is verified, only those columns that
	// are in the row will be compared against the verified row retrieved from the database.
	VerifyRow(ctx context.Context, row *schema.Row, table string, pkVals []*schema.SQLValue) error

	// NewTx starts a new transaction.
	//
	// Note: Currently such transaction can only be used for SQL operations.
	NewTx(ctx context.Context, opts ...TxOption) (Tx, error)

	// TruncateDatabase truncates a database.
	// This truncates the locally stored value log files used by the database.
	//
	// This call requires SysAdmin permission level or admin permission to the database.
	TruncateDatabase(ctx context.Context, db string, retentionPeriod time.Duration) error
}

type ErrorHandler func(sessionID string, err error)

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
	HeartBeater          HeartBeater
	errorHandler         ErrorHandler
}

// Ensure immuClient implements the ImmuClient interface
var _ ImmuClient = (*immuClient)(nil)

// NewClient creates a new instance if immudb client object.
//
// The returned object implements the ImmuClient interface.
//
// Returned instance is not connected to the database,
// use OpenSession to establish the connection.
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

// NewImmuClient creates a new immudb client object instance and connects to a server.
//
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

	stateService, err := state.NewStateService(
		cache.NewFileCache(options.Dir),
		l,
		stateProvider,
		uuidProvider,
	)
	if err != nil {
		return nil, logErr(l, "Unable to create state service: %s", err)
	}

	if !c.Options.DisableIdentityCheck {
		stateService.SetServerIdentity(c.getServerIdentity())
	}

	c.WithStateService(stateService)

	return c, nil
}

func (c *immuClient) debugElapsedTime(method string, start time.Time) {
	c.Logger.Debugf("method immuclient.%s took %s", method, time.Since(start))
}

func (c *immuClient) getServerIdentity() string {
	// TODO: Allow customizing this value
	return c.Options.Bind()
}

// SetupDialOptions extracts grpc dial options from provided client options.
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
			opts = append(opts, grpc.WithStreamInterceptor(
				grpc_middleware.ChainStreamClient(
					c.TokenStreamInterceptor,
					auth.ClientStreamInterceptor(token),
					c.SessionIDInjectorStreamInterceptor,
				)),
			)
		}
	}
	uic = append(uic, c.SessionIDInjectorInterceptor)

	opts = append(opts, grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(uic...)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(options.MaxRecvMsgSize)))

	return opts
}

// Connect establishes new connection to the server.
//
// Deprecated: use NewClient and OpenSession instead.
func (c *immuClient) Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error) {
	c.Logger.Debugf("dialed %v", c.Options)

	if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err != nil {
		return nil, err
	}

	return c.clientConn, nil
}

// Disconnect closes the current connection to the server.
//
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

// IsConnected checks whether the client is connected to the server.
func (c *immuClient) IsConnected() bool {
	return c.clientConn != nil && c.ServiceClient != nil
}

// WaitForHealthCheck waits for up to Options.HealthCheckRetries seconds to
// get a successful HealthCheck response from the server.
//
// Deprecated: grpc retry mechanism can be implemented with WithConnectParams dialOption.
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

// GetServiceClient returns low-level GRPC service client.
func (c *immuClient) GetServiceClient() schema.ImmuServiceClient {
	return c.ServiceClient
}

// GetOptions returns current client options.
func (c *immuClient) GetOptions() *Options {
	return c.Options
}

// ListUser returns a list of database users.
//
// This call requires Admin or SysAdmin permission level.
//
// When called as a SysAdmin user, all users in the database are returned.
// When called as an Admin user, users for currently selected database are returned.
func (c *immuClient) ListUsers(ctx context.Context) (*schema.UserList, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.ListUsers(ctx, new(empty.Empty))
}

// CreateUser creates new user with given credentials and permission.
//
// Required user permission is SysAdmin or database Admin.
//
// SysAdmin user can create users with access to any database.
//
// Admin user can only create users with access to databases where
// the user has admin permissions.
//
// The permission argument is the permission level and can be one of those values:
//   - 1 (auth.PermissionR) - read-only access
//   - 2 (auth.PermissionRW) - read-write access
//   - 254 (auth.PermissionAdmin) - read-write with admin rights
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

// ChangePassword changes password for existing user.
//
// This call requires Admin or SysAdmin permission level.
//
// The oldPass argument is only necessary when changing SysAdmin user's password.
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

	c.Logger.Debugf("ChangePassword finished in %s", time.Since(start))

	return err
}

// UpdateAuthConfig is no longer supported.
//
// Deprecated: will be removed in future versions.
func (c *immuClient) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	_, err := c.ServiceClient.UpdateAuthConfig(ctx, &schema.AuthConfig{
		Kind: uint32(kind),
	})

	c.Logger.Debugf("UpdateAuthConfig finished in %s", time.Since(start))

	return err
}

// UpdateMTLSConfig is no longer supported.
//
// Deprecated: will be removed in future versions.
func (c *immuClient) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	_, err := c.ServiceClient.UpdateMTLSConfig(ctx, &schema.MTLSConfig{
		Enabled: enabled,
	})

	c.Logger.Debugf("UpdateMTLSConfig finished in %s", time.Since(start))

	return err
}

// Login authenticates the user in an established connection.
//
// Deprecated: use NewClient and OpenSession instead.
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

// Logout logs out the user.
//
// Deprecated: use CloseSession.
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

// ServerInfo returns information about the server instance.
func (c *immuClient) ServerInfo(ctx context.Context, req *schema.ServerInfoRequest) (*schema.ServerInfoResponse, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.ServerInfo(ctx, req)
}

// Health returns Health information about the current database.
func (c *immuClient) Health(ctx context.Context) (*schema.DatabaseHealthResponse, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.DatabaseHealth(ctx, &empty.Empty{})
}

// CurrentState returns current database state.
func (c *immuClient) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("CurrentState", start)

	return c.ServiceClient.CurrentState(ctx, &empty.Empty{})
}

// Get reads a single value for given key.
func (c *immuClient) Get(ctx context.Context, key []byte, opts ...GetOption) (*schema.Entry, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("Get", start)

	req := &schema.KeyRequest{Key: key}
	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return nil, err
		}
	}

	return c.ServiceClient.Get(ctx, req)
}

// GetSince reads a single value for given key assuming that at least transaction `tx` was indexed.
// For more information about getting value with sinceTx constraint see the SinceTx get option.
func (c *immuClient) GetSince(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return c.Get(ctx, key, SinceTx(tx))
}

// GetAt reads a single value that was modified at a specific transaction.
// For more information about getting value from specific revision see the AtTx get option.
func (c *immuClient) GetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return c.Get(ctx, key, AtTx(tx))
}

// GetAtRevision reads value for given key by its revision.
// For more information about the revisions see the AtRevision get option.
func (c *immuClient) GetAtRevision(ctx context.Context, key []byte, rev int64) (*schema.Entry, error) {
	return c.Get(ctx, key, AtRevision(rev))
}

// Gets reads a single value for given key with additional server-provided proof validation.
func (c *immuClient) VerifiedGet(ctx context.Context, key []byte, opts ...GetOption) (vi *schema.Entry, err error) {
	start := time.Now()
	defer c.debugElapsedTime("VerifiedGet", start)

	req := &schema.KeyRequest{Key: key}
	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return nil, err
		}
	}

	return c.verifiedGet(ctx, req)
}

// VerifiedGetSince reads a single value for given key assuming that at least transaction `tx` was indexed.
// For more information about getting value with sinceTx constraint see the SinceTx get option.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
func (c *immuClient) VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, SinceTx(tx))
}

// VerifiedGetAt reads a single value that was modified at a specific transaction.
// For more information about getting value from specific revision see the AtTx get option.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
func (c *immuClient) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, AtTx(tx))
}

// VerifiedGetAtRevision reads value for given key by its revision.
// For more information about the revisions see the AtRevision get option.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
func (c *immuClient) VerifiedGetAtRevision(ctx context.Context, key []byte, rev int64) (vi *schema.Entry, err error) {
	return c.VerifiedGet(ctx, key, AtRevision(rev))
}

func (c *immuClient) verifyDualProof(
	ctx context.Context,
	dualProof *store.DualProof,
	sourceID uint64,
	targetID uint64,
	sourceAlh [sha256.Size]byte,
	targetAlh [sha256.Size]byte,
) error {
	err := schema.FillMissingLinearAdvanceProof(
		ctx, dualProof, sourceID, targetID, c.ServiceClient,
	)
	if err != nil {
		return err
	}

	verifies := store.VerifyDualProof(
		dualProof,
		sourceID,
		targetID,
		sourceAlh,
		targetAlh,
	)
	if !verifies {
		return store.ErrCorruptedData
	}

	return nil
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
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vEntry.Entry, nil
}

// Scan iterates over the set of keys in a topological order.
func (c *immuClient) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.Scan(ctx, req)
}

// ZScan iterates over the elements of sorted set ordered by their score.
func (c *immuClient) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.ZScan(ctx, req)
}

// Count returns count of key-value entries with given prefix.
//
// Note: This feature is not implemented yet.
func (c *immuClient) Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.Count(ctx, &schema.KeyPrefix{Prefix: prefix})
}

// Count returns count of all key-value entries.
//
// Note: This feature is not implemented yet.
func (c *immuClient) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.CountAll(ctx, new(empty.Empty))
}

// Set commits a change of a value for a single key.
func (c *immuClient) Set(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return c.set(ctx, key, nil, value)
}

func (c *immuClient) set(ctx context.Context, key []byte, md *schema.KVMetadata, value []byte) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	hdr, err := c.ServiceClient.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Metadata: md, Value: value}}})
	if err != nil {
		return nil, err
	}

	if int(hdr.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return hdr, nil
}

// VerifiedSet commits a change of a value for a single key.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
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
	defer c.debugElapsedTime("VerifiedSet", start)

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
		dualProof := schema.DualProofFromProto(verifiableTx.DualProof)
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Header, nil
}

// ExpirableSet commits a change of a value for a single key and sets up the expiration
// time for that value after which the value will no longer be retrievable.
func (c *immuClient) ExpirableSet(ctx context.Context, key []byte, value []byte, expiresAt time.Time) (*schema.TxHeader, error) {
	return c.set(
		ctx,
		key,
		&schema.KVMetadata{
			Expiration: &schema.Expiration{
				ExpiresAt: expiresAt.Unix(),
			},
		},
		value,
	)
}

// SetAll sets multiple entries in a single transaction.
func (c *immuClient) SetAll(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	hdr, err := c.ServiceClient.Set(ctx, req)
	if err != nil {
		return nil, err
	}

	if int(hdr.Nentries) != len(req.KVs) {
		return nil, store.ErrCorruptedData
	}

	return hdr, nil
}

// ExecAll performs multiple write operations (values, references, sorted set entries)
// in a single transaction.
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

// GetAll retrieves multiple entries in a single call.
func (c *immuClient) GetAll(ctx context.Context, keys [][]byte) (*schema.Entries, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("GetAll", start)

	keyList := &schema.KeyListRequest{}

	keyList.Keys = append(keyList.Keys, keys...)

	return c.ServiceClient.GetAll(ctx, keyList)
}

// Delete performs a logical deletion for a list of keys marking them as deleted.
func (c *immuClient) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.Delete(ctx, req)
}

// TxByID retrieves all entries (in a raw, unprocessed form) for given transaction.
//
// Note: In order to read keys and values, it is necessary to parse returned entries
// TxByIDWithSpec can be used to read already-parsed values.
func (c *immuClient) TxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("TxByID", start)

	t, err := c.ServiceClient.TxById(ctx, &schema.TxRequest{
		Tx: tx,
	})
	if err != nil {
		return nil, err
	}

	decodeTxEntries(t.Entries)

	return t, err
}

// TxByIDWithSpec retrieves entries from given transaction according to given spec.
func (c *immuClient) TxByIDWithSpec(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.TxById(ctx, req)
}

// TxByID retrieves all entries (in a raw, unprocessed form) for given transaction
// and performs verification of the server-provided proof for the whole transaction.
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
	defer c.debugElapsedTime("VerifiedTxByID", start)

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
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	decodeTxEntries(vTx.Tx.Entries)

	return vTx.Tx, nil
}

// TxScan returns raw entries for a range of transactions.
func (c *immuClient) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.TxScan(ctx, req)
}

// History returns history for a single key.
func (c *immuClient) History(ctx context.Context, req *schema.HistoryRequest) (sl *schema.Entries, err error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("History", start)

	return c.ServiceClient.History(ctx, req)
}

// SetReference creates a reference to another key's value.
//
// Note: references can only be created to non-reference keys.
func (c *immuClient) SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return c.SetReferenceAt(ctx, key, referencedKey, 0)
}

// SetReference creates a reference to another key's value at a specific transaction.
//
// Note: references can only be created to non-reference keys.
func (c *immuClient) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("SetReferenceAt", start)

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

// VerifiedSetReference creates a reference to another key's value and verifies server-provided
// proof for the write.
//
// Note: references can only be created to non-reference keys.
func (c *immuClient) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return c.VerifiedSetReferenceAt(ctx, key, referencedKey, 0)
}

// SetReference creates a reference to another key's value at a specific transaction and verifies server-provided
// proof for the write.
//
// Note: references can only be created to non-reference keys.
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
	defer c.debugElapsedTime("VerifiedSetReferenceAt", start)

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
		dualProof := schema.DualProofFromProto(verifiableTx.DualProof)
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return verifiableTx.Tx.Header, nil
}

// ZAdd adds a new entry to sorted set.
// New entry is a reference to some other key's value
// with additional score used for ordering set members.
func (c *immuClient) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return c.ZAddAt(ctx, set, score, key, 0)
}

// ZAddAt adds a new entry to sorted set.
// New entry is a reference to some other key's value at a specific transaction
// with additional score used for ordering set members.
func (c *immuClient) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	start := time.Now()
	defer c.debugElapsedTime("ZAddAt", start)

	hdr, err := c.ServiceClient.ZAdd(ctx, &schema.ZAddRequest{
		Set:      set,
		Score:    score,
		Key:      key,
		AtTx:     atTx,
		BoundRef: atTx > 0,
	})
	if err != nil {
		return nil, err
	}

	if int(hdr.Nentries) != 1 {
		return nil, store.ErrCorruptedData
	}

	return hdr, nil
}

// VerifiedZAdd adds a new entry to sorted set.
// New entry is a reference to some other key's value
// with additional score used for ordering set members.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
func (c *immuClient) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return c.VerifiedZAddAt(ctx, set, score, key, 0)
}

// VerifiedZAddAt adds a new entry to sorted set.
// New entry is a reference to some other key's value at a specific transaction
// with additional score used for ordering set members.
//
// This function also requests a server-generated proof, verifies the entry in the transaction
// using the proof and verifies the signature of the signed state.
// If verification does not succeed the store.ErrCorruptedData error is returned.
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
	defer c.debugElapsedTime("VerifiedZAddAt", start)

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
		dualProof := schema.DualProofFromProto(vtx.DualProof)
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return nil, err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vtx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return nil, err
		}
	}

	err = c.StateService.SetState(c.Options.CurrentDatabase, newState)
	if err != nil {
		return nil, err
	}

	return vtx.Tx.Header, nil
}

// Dump is currently not implemented.
func (c *immuClient) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	return 0, errors.New("Functionality not yet supported")
}

// Get server health information.
//
// Deprecated: use ServerInfo.
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

// CreateDatabase creates new database within server instance.
// This call requires SysAdmin permission level.
//
// Deprecated: Use CreateDatabaseV2
func (c *immuClient) CreateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.CreateDatabaseWith(ctx, settings)

	c.Logger.Debugf("CreateDatabase finished in %s", time.Since(start))

	return err
}

// CreateDatabaseV2 creates a new database.
// This call requires SysAdmin permission level.
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

// LoadDatabase loads database on the server. A database is not loaded
// if it has AutoLoad setting set to false or if it failed to load during
// immudb startup.
//
// This call requires SysAdmin permission level or admin permission to the database.
func (c *immuClient) LoadDatabase(ctx context.Context, r *schema.LoadDatabaseRequest) (*schema.LoadDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.LoadDatabase(ctx, r)

	c.Logger.Debugf("LoadDatabase finished in %s", time.Since(start))

	return res, err
}

// UnloadDatabase unloads database on the server. Such database becomes inaccessible
// by the client and server frees internal resources allocated for that database.
//
// This call requires SysAdmin permission level or admin permission to the database.
func (c *immuClient) UnloadDatabase(ctx context.Context, r *schema.UnloadDatabaseRequest) (*schema.UnloadDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.UnloadDatabase(ctx, r)

	c.Logger.Debugf("UnloadDatabase finished in %s", time.Since(start))

	return res, err
}

// DeleteDatabase removes an unloaded database.
// This also removes locally stored files used by the database.
//
// This call requires SysAdmin permission level or admin permission to the database.
func (c *immuClient) DeleteDatabase(ctx context.Context, r *schema.DeleteDatabaseRequest) (*schema.DeleteDatabaseResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	res, err := c.ServiceClient.DeleteDatabase(ctx, r)

	c.Logger.Debugf("DeleteDatabase finished in %s", time.Since(start))

	return res, err
}

// UseDatabase changes the currently selected database.
//
// This call requires at least read permission level for the target database.
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

// UpdateDatabase updates database settings.
//
// Deprecated: Use UpdateDatabaseV2.
func (c *immuClient) UpdateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.UpdateDatabase(ctx, settings)

	c.Logger.Debugf("UpdateDatabase finished in %s", time.Since(start))

	return err
}

// UpdateDatabaseV2 updates database settings.
//
// Settings can be set selectively - values not set in the settings object
// will not be updated.
//
// The returned value is the list of settings after the update.
//
// Settings other than those related to replication will only be applied after
// immudb restart or unload/load cycle of the database.
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

// GetDatabaseSettings returns current database settings.
//
// Deprecated: Use GetDatabaseSettingsV2.
func (c *immuClient) GetDatabaseSettings(ctx context.Context) (*schema.DatabaseSettings, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.GetDatabaseSettings(ctx, &empty.Empty{})
}

// GetDatabaseSettingsV2 returns current database settings.
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

// FlushIndex requests a flush operation from the database.
// This call requires SysAdmin or Admin permission to given database.
//
// The cleanupPercentage value is the amount of index nodes data in percent
// that will be scanned in order to free up unused disk space.
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

// CompactIndex perform full database compaction.
// This call requires SysAdmin or Admin permission to given database.
//
// Note: Full compaction will greatly affect the performance of the database.
// It should also be called only when there's a minimal database activity,
// if full compaction collides with a read or write operation, it will be aborted
// and may require retry of the whole operation. For that reason it is preferred
// to periodically call FlushIndex with a small value of cleanupPercentage or set the
// cleanupPercentage database option.
func (c *immuClient) CompactIndex(ctx context.Context, req *empty.Empty) error {
	start := time.Now()

	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	_, err := c.ServiceClient.CompactIndex(ctx, req)

	c.Logger.Debugf("CompactIndex finished in %s", time.Since(start))

	return err
}

// ChangePermission grants or revokes permission to one database for given user.
//
// This call requires SysAdmin or admin permission to the database where we grant permissions.
//
// The permission argument is used when granting permission and can be one of those values:
//   - 1 (auth.PermissionR) - read-only access
//   - 2 (auth.PermissionRW) - read-write access
//   - 254 (auth.PermissionAdmin) - read-write with admin rights
//
// The following restrictions are applied:
//   - the user can not change permission for himself
//   - can not change permissions of the SysAdmin user
//   - the user must be active
//   - when the user already had permission to the database, it is overwritten
//     by the new permission (even if the user had higher permission before)
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

// SetActiveUser activates or deactivates a user.
// This call requires SysAdmin or Admin permission.
func (c *immuClient) SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	_, err := c.ServiceClient.SetActiveUser(ctx, u)

	c.Logger.Debugf("SetActiveUser finished in %s", time.Since(start))

	return err
}

// Return list of databases
//
// Deprecated: Use DatabaseListV2
func (c *immuClient) DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error) {
	start := time.Now()

	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	result, err := c.ServiceClient.DatabaseList(ctx, &empty.Empty{})

	c.Logger.Debugf("DatabaseList finished in %s", time.Since(start))

	return result, err
}

// DatabaseListV2 returns a list of databases the user has access to.
func (c *immuClient) DatabaseListV2(ctx context.Context) (*schema.DatabaseListResponseV2, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	return c.ServiceClient.DatabaseListV2(ctx, &schema.DatabaseListRequestV2{})
}

func decodeTxEntries(entries []*schema.TxEntry) {
	for _, it := range entries {
		it.Key = it.Key[1:]
	}
}

// TruncateDatabase truncates the database to the given retention period.
func (c *immuClient) TruncateDatabase(ctx context.Context, db string, retentionPeriod time.Duration) error {
	start := time.Now()

	if !c.IsConnected() {
		return ErrNotConnected
	}

	in := &schema.TruncateDatabaseRequest{
		Database:        db,
		RetentionPeriod: retentionPeriod.Milliseconds(),
	}

	_, err := c.ServiceClient.TruncateDatabase(ctx, in)

	c.Logger.Debugf("TruncateDatabase finished in %s", time.Since(start))

	return err
}

// VerifiableGet
func (c *immuClient) VerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error) {
	result, err := c.ServiceClient.VerifiableGet(ctx, in, opts...)

	return result, err
}
