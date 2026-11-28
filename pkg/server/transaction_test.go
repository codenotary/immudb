/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package server

import (
	"context"
	"math"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestImmuServer_Transaction(t *testing.T) {
	dir := t.TempDir()

	s := DefaultServer()

	s.WithOptions(DefaultOptions().WithDir(dir).WithMaintenance(true))

	_, err := s.NewTx(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.NewTx(context.Background(), &schema.NewTxRequest{Mode: schema.TxMode_ReadWrite})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.Commit(context.Background(), &emptypb.Empty{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.Rollback(context.Background(), &emptypb.Empty{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.TxSQLExec(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.TxSQLExec(context.Background(), &schema.SQLExecRequest{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	err = s.TxSQLQuery(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = s.TxSQLQuery(&schema.SQLQueryRequest{}, nil)
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)
}

func TestSessionTransactionKeepsItsDatabasePermissions(t *testing.T) {
	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(t.TempDir()).WithPort(0).WithMetricsServer(false).WithPgsqlServer(false))
	require.NoError(t, s.Initialize())

	openSession := func(user, password, db string) string {
		resp, err := s.OpenSession(context.Background(), &schema.OpenSessionRequest{
			Username:     []byte(user),
			Password:     []byte(password),
			DatabaseName: db,
		})
		require.NoError(t, err)
		return resp.GetSessionID()
	}
	withMD := func(kv ...string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs(kv...))
	}

	adminCtx := withMD("sessionid", openSession(auth.SysAdminUsername, auth.SysAdminPassword, DefaultDBName))

	for _, db := range []string{"dba", "dbb"} {
		_, err := s.CreateDatabaseV2(adminCtx, &schema.CreateDatabaseRequest{Name: db})
		require.NoError(t, err)
		_, err = s.UseDatabase(adminCtx, &schema.Database{DatabaseName: db})
		require.NoError(t, err)
		_, err = s.SQLExec(adminCtx, &schema.SQLExecRequest{Sql: "CREATE TABLE t (id INTEGER, PRIMARY KEY id)"})
		require.NoError(t, err)
	}

	_, err := s.CreateUser(adminCtx, &schema.CreateUserRequest{
		User: []byte("reader"), Password: []byte("Reader1!pass"), Permission: auth.PermissionR, Database: "dba",
	})
	require.NoError(t, err)
	_, err = s.ChangePermission(adminCtx, &schema.ChangePermissionRequest{
		Action: schema.PermissionAction_GRANT, Username: "reader", Database: "dbb", Permission: auth.PermissionRW,
	})
	require.NoError(t, err)

	readerSession := openSession("reader", "Reader1!pass", "dba")
	readerCtx := withMD("sessionid", readerSession)

	t.Run("switching database inside a transaction does not grant write on the first one", func(t *testing.T) {
		tx, err := s.NewTx(readerCtx, &schema.NewTxRequest{Mode: schema.TxMode_ReadWrite})
		require.NoError(t, err)
		txCtx := withMD("sessionid", readerSession, "transactionid", tx.TransactionID)

		_, err = s.UseDatabase(readerCtx, &schema.Database{DatabaseName: "dbb"})
		require.NoError(t, err)

		_, err = s.TxSQLExec(txCtx, &schema.SQLExecRequest{Sql: "INSERT INTO t (id) VALUES (1)"})
		require.ErrorContains(t, err, "READWRITE")
	})

	t.Run("snapshot beyond the last transaction is rejected", func(t *testing.T) {
		_, err := s.NewTx(adminCtx, &schema.NewTxRequest{
			Mode:                    schema.TxMode_ReadOnly,
			SnapshotMustIncludeTxID: &schema.NullableUint64{Value: math.MaxUint64},
		})
		require.ErrorIs(t, err, store.ErrTxNotFound)
	})
}
