package client

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

const maxBadConnRetries = 5

var ErrMaxRetries = errors.New("max retries achieved")

type client struct {
	next             ImmuClient
	connectionOpener func() error
	mutex            sync.RWMutex
}

func NewClientMiddleware(n ImmuClient) ImmuClient {
	return &client{next: n}
}

func (c *client) Disconnect() error {
	return c.next.Disconnect()
}

func (c *client) IsConnected() bool {
	return c.next.IsConnected()
}

func (c *client) WaitForHealthCheck(ctx context.Context) error {
	return c.next.WaitForHealthCheck(ctx)
}

func (c *client) HealthCheck(ctx context.Context) error {
	return c.next.HealthCheck(ctx)
}

func (c *client) Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error) {
	return c.next.Connect(ctx)
}

func (c *client) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	return c.next.Login(ctx, user, pass)
}

func (c *client) Logout(ctx context.Context) error {
	return c.next.Logout(ctx)
}

// @TODO: Race condition concerns!!!
func (c *client) OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.connectionOpener = func() error {
		log.Printf("try to reconnect user %s pass %s database %s", string(user), string(pass), database)
		_ = c.next.CloseSession(ctx)
		return c.next.OpenSession(ctx, user, pass, database)
	}

	return c.connectionOpener()
}

func (c *client) CloseSession(ctx context.Context) error {
	return c.next.CloseSession(ctx)
}

func (c *client) CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error {
	fn := func() error { return c.next.CreateUser(ctx, user, pass, permission, databasename) }
	return c.execute(fn)
}

func (c *client) ListUsers(ctx context.Context) (u *schema.UserList, err error) {
	fn := func() error {
		u, err = c.next.ListUsers(ctx)
		return err
	}

	err = c.execute(fn)
	return u, err
}

func (c *client) ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error {
	fn := func() error { return c.next.ChangePassword(ctx, user, oldPass, newPass) }
	return c.execute(fn)
}

func (c *client) ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error {
	fn := func() error { return c.next.ChangePermission(ctx, action, username, database, permissions) }
	return c.execute(fn)
}

func (c *client) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	fn := func() error { return c.next.UpdateAuthConfig(ctx, kind) }
	return c.execute(fn)
}

func (c *client) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	fn := func() error { return c.next.UpdateMTLSConfig(ctx, enabled) }
	return c.execute(fn)
}

func (c *client) WithOptions(options *Options) *immuClient {
	return c.next.WithOptions(options)
}

func (c *client) WithLogger(logger logger.Logger) *immuClient {
	return c.next.WithLogger(logger)
}

func (c *client) WithStateService(rs state.StateService) *immuClient {
	return c.next.WithStateService(rs)
}

func (c *client) WithClientConn(clientConn *grpc.ClientConn) *immuClient {
	return c.next.WithClientConn(clientConn)
}

func (c *client) WithServiceClient(serviceClient schema.ImmuServiceClient) *immuClient {
	return c.next.WithServiceClient(serviceClient)
}

func (c *client) WithTokenService(tokenService tokenservice.TokenService) *immuClient {
	return c.next.WithTokenService(tokenService)
}

func (c *client) WithServerSigningPubKey(serverSigningPubKey *ecdsa.PublicKey) *immuClient {
	return c.next.WithServerSigningPubKey(serverSigningPubKey)
}

func (c *client) WithStreamServiceFactory(ssf stream.ServiceFactory) *immuClient {
	return c.next.WithStreamServiceFactory(ssf)
}

func (c *client) GetServiceClient() schema.ImmuServiceClient {
	return c.next.GetServiceClient()
}

func (c *client) GetOptions() *Options {
	return c.next.GetOptions()
}

func (c *client) SetupDialOptions(options *Options) []grpc.DialOption {
	return c.next.SetupDialOptions(options)
}

func (c *client) DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error) {
	return c.next.DatabaseList(ctx)
}

func (c *client) DatabaseListV2(ctx context.Context) (*schema.DatabaseListResponseV2, error) {
	return c.next.DatabaseListV2(ctx)
}

func (c *client) CreateDatabase(ctx context.Context, d *schema.DatabaseSettings) error {
	return c.next.CreateDatabase(ctx, d)
}

func (c *client) CreateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error) {
	return c.next.CreateDatabaseV2(ctx, database, settings)
}

func (c *client) LoadDatabase(ctx context.Context, r *schema.LoadDatabaseRequest) (*schema.LoadDatabaseResponse, error) {
	return c.next.LoadDatabase(ctx, r)
}

func (c *client) UnloadDatabase(ctx context.Context, r *schema.UnloadDatabaseRequest) (*schema.UnloadDatabaseResponse, error) {
	return c.next.UnloadDatabase(ctx, r)
}

func (c *client) DeleteDatabase(ctx context.Context, r *schema.DeleteDatabaseRequest) (*schema.DeleteDatabaseResponse, error) {
	return c.next.DeleteDatabase(ctx, r)
}

func (c *client) UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
	return c.next.UseDatabase(ctx, d)
}

func (c *client) UpdateDatabase(ctx context.Context, settings *schema.DatabaseSettings) error {
	return c.next.UpdateDatabase(ctx, settings)
}

func (c *client) UpdateDatabaseV2(ctx context.Context, database string, settings *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error) {
	return c.next.UpdateDatabaseV2(ctx, database, settings)
}

func (c *client) GetDatabaseSettings(ctx context.Context) (*schema.DatabaseSettings, error) {
	return c.next.GetDatabaseSettings(ctx)
}

func (c *client) GetDatabaseSettingsV2(ctx context.Context) (*schema.DatabaseSettingsResponse, error) {
	return c.next.GetDatabaseSettingsV2(ctx)
}

func (c *client) SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error {
	return c.next.SetActiveUser(ctx, u)
}

func (c *client) FlushIndex(ctx context.Context, cleanupPercentage float32, synced bool) (*schema.FlushIndexResponse, error) {
	return c.next.FlushIndex(ctx, cleanupPercentage, synced)
}

func (c *client) CompactIndex(ctx context.Context, req *empty.Empty) error {
	return c.next.CompactIndex(ctx, req)
}

func (c *client) ServerInfo(ctx context.Context, req *schema.ServerInfoRequest) (*schema.ServerInfoResponse, error) {
	return c.next.ServerInfo(ctx, req)
}

func (c *client) Health(ctx context.Context) (*schema.DatabaseHealthResponse, error) {
	return c.next.Health(ctx)
}

func (c *client) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	return c.next.CurrentState(ctx)
}

func (c *client) Set(ctx context.Context, key []byte, value []byte) (h *schema.TxHeader, err error) {
	fn := func() error {
		h, err = c.next.Set(ctx, key, value)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) VerifiedSet(ctx context.Context, key []byte, value []byte) (h *schema.TxHeader, err error) {
	fn := func() error {
		h, err = c.next.VerifiedSet(ctx, key, value)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) ExpirableSet(ctx context.Context, key []byte, value []byte, expiresAt time.Time) (h *schema.TxHeader, err error) {
	fn := func() error {
		h, err = c.next.ExpirableSet(ctx, key, value, expiresAt)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) Get(ctx context.Context, key []byte, opts ...GetOption) (e *schema.Entry, err error) {
	fn := func() error {
		e, err = c.next.Get(ctx, key, opts...)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) GetSince(ctx context.Context, key []byte, tx uint64) (e *schema.Entry, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.GetSince(ctx, key, tx)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) GetAt(ctx context.Context, key []byte, tx uint64) (e *schema.Entry, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.GetAt(ctx, key, tx)
		return err
	}

	err = c.execute(fn)
	return e, err
}

func (c *client) GetAtRevision(ctx context.Context, key []byte, rev int64) (e *schema.Entry, err error) {
	fn := func() error {
		e, err = c.next.GetAtRevision(ctx, key, rev)
		return err
	}

	err = c.execute(fn)
	return e, err
}

func (c *client) VerifiedGet(ctx context.Context, key []byte, opts ...GetOption) (e *schema.Entry, err error) {
	fn := func() error {
		e, err = c.next.VerifiedGet(ctx, key, opts...)
		return err
	}

	err = c.execute(fn)
	return e, err
}

func (c *client) VerifiedGetSince(ctx context.Context, key []byte, tx uint64) (e *schema.Entry, err error) {
	fn := func() error {
		e, err = c.next.VerifiedGetSince(ctx, key, tx)
		return err
	}

	err = c.execute(fn)
	return e, err
}

func (c *client) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (e *schema.Entry, err error) {
	fn := func() error {
		e, err = c.next.VerifiedGetAt(ctx, key, tx)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) VerifiedGetAtRevision(ctx context.Context, key []byte, rev int64) (e *schema.Entry, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.VerifiedGetAtRevision(ctx, key, rev)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) History(ctx context.Context, req *schema.HistoryRequest) (e *schema.Entries, err error) {
	fn := func() error {
		e, err = c.next.History(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (h *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		h, err = c.next.ZAdd(ctx, set, score, key)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (h *schema.TxHeader, err error) {
	fn := func() error {
		h, err = c.next.VerifiedZAdd(ctx, set, score, key)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (h *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		h, err = c.next.ZAddAt(ctx, set, score, key, atTx)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (h *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		h, err = c.next.VerifiedZAddAt(ctx, set, score, key, atTx)
		return err
	}
	err = c.execute(fn)
	return h, err
}

func (c *client) Scan(ctx context.Context, req *schema.ScanRequest) (e *schema.Entries, err error) {
	fn := func() error {
		e, err = c.next.Scan(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) ZScan(ctx context.Context, req *schema.ZScanRequest) (e *schema.ZEntries, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.ZScan(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) TxByID(ctx context.Context, tx uint64) (t *schema.Tx, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.TxByID(ctx, tx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) VerifiedTxByID(ctx context.Context, tx uint64) (t *schema.Tx, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.VerifiedTxByID(ctx, tx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) TxByIDWithSpec(ctx context.Context, req *schema.TxRequest) (t *schema.Tx, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.TxByIDWithSpec(ctx, req)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) TxScan(ctx context.Context, req *schema.TxScanRequest) (t *schema.TxList, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.TxScan(ctx, req)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error) {
	return c.next.Count(ctx, prefix)
}

func (c *client) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	return c.next.CountAll(ctx)
}

func (c *client) SetAll(ctx context.Context, kvList *schema.SetRequest) (t *schema.TxHeader, err error) {
	fn := func() error {
		t, err = c.next.SetAll(ctx, kvList)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) GetAll(ctx context.Context, keys [][]byte) (e *schema.Entries, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.GetAll(ctx, keys)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.Delete(ctx, req)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) ExecAll(ctx context.Context, in *schema.ExecAllRequest) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.ExecAll(ctx, in)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) SetReference(ctx context.Context, key []byte, referencedKey []byte) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.SetReference(ctx, key, referencedKey)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.VerifiedSetReference(ctx, key, referencedKey)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.SetReferenceAt(ctx, key, referencedKey, atTx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.VerifiedSetReferenceAt(ctx, key, referencedKey, atTx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) Dump(ctx context.Context, writer io.WriteSeeker) (d int64, err error) {
	//TODO implement me
	fn := func() error {
		d, err = c.next.Dump(ctx, writer)
		return err
	}
	err = c.execute(fn)
	return d, err
}

func (c *client) StreamSet(ctx context.Context, kv []*stream.KeyValue) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.StreamSet(ctx, kv)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) StreamGet(ctx context.Context, k *schema.KeyRequest) (e *schema.Entry, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.StreamGet(ctx, k)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) StreamVerifiedSet(ctx context.Context, kv []*stream.KeyValue) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.StreamVerifiedSet(ctx, kv)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) StreamVerifiedGet(ctx context.Context, k *schema.VerifiableGetRequest) (e *schema.Entry, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.StreamVerifiedGet(ctx, k)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) StreamScan(ctx context.Context, req *schema.ScanRequest) (e *schema.Entries, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.StreamScan(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) StreamZScan(ctx context.Context, req *schema.ZScanRequest) (e *schema.ZEntries, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.StreamZScan(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) StreamHistory(ctx context.Context, req *schema.HistoryRequest) (e *schema.Entries, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.StreamHistory(ctx, req)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) StreamExecAll(ctx context.Context, req *stream.ExecAllRequest) (t *schema.TxHeader, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.StreamExecAll(ctx, req)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) ExportTx(ctx context.Context, req *schema.ExportTxRequest) (t schema.ImmuService_ExportTxClient, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.ExportTx(ctx, req)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) ReplicateTx(ctx context.Context) (t schema.ImmuService_ReplicateTxClient, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.ReplicateTx(ctx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) SQLExec(ctx context.Context, sql string, params map[string]interface{}) (e *schema.SQLExecResult, err error) {
	//TODO implement me
	fn := func() error {
		e, err = c.next.SQLExec(ctx, sql, params)
		return err
	}
	err = c.execute(fn)
	return e, err
}

func (c *client) SQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (s *schema.SQLQueryResult, err error) {
	//TODO implement me
	fn := func() error {
		s, err = c.next.SQLQuery(ctx, sql, params, renewSnapshot)
		return err
	}
	err = c.execute(fn)
	return s, err
}

func (c *client) ListTables(ctx context.Context) (s *schema.SQLQueryResult, err error) {
	//TODO implement me
	fn := func() error {
		s, err = c.next.ListTables(ctx)
		return err
	}
	err = c.execute(fn)
	return s, err
}

func (c *client) DescribeTable(ctx context.Context, tableName string) (s *schema.SQLQueryResult, err error) {
	//TODO implement me
	fn := func() error {
		s, err = c.next.DescribeTable(ctx, tableName)
		return err
	}
	err = c.execute(fn)
	return s, err
}

func (c *client) VerifyRow(ctx context.Context, row *schema.Row, table string, pkVals []*schema.SQLValue) error {
	//TODO implement me
	fn := func() error { return c.next.VerifyRow(ctx, row, table, pkVals) }
	return c.execute(fn)
}

func (c *client) NewTx(ctx context.Context) (t Tx, err error) {
	//TODO implement me
	fn := func() error {
		t, err = c.next.NewTx(ctx)
		return err
	}
	err = c.execute(fn)
	return t, err
}

func (c *client) execute(f func() error) error {
	log.Println("executor middleware")
	for i := 0; i < maxBadConnRetries; i++ {
		c.mutex.RLock()
		err := f()
		if err == nil {
			c.mutex.RUnlock()
			log.Println("executor middleware Success")
			return nil
		}
		c.mutex.RUnlock()
		if err != sessions.ErrSessionNotFound && err != ErrNotConnected && !strings.Contains(err.Error(), "session not found") {
			log.Printf("Unable to retry connection from executor middleware, error %v", err)
			return err
		}

		log.Printf("Session Not Found error, try to reOpen session, iteration %d, error %v", i, err)
		c.mutex.Lock()
		_ = c.connectionOpener()
		c.mutex.Unlock()
	}

	log.Println("CRITICAL Err Max conn retries")
	return ErrMaxRetries
}
