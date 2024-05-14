/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSQLInteraction(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	ctx := context.Background()

	_, err := s.ListTables(ctx, &emptypb.Empty{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.SQLExec(ctx, nil)
	require.ErrorIs(t, err, ErrNotLoggedIn)

	err = s.SQLQuery(nil, &ImmuService_SQLQueryServerMock{ctx: ctx})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DescribeTable(ctx, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.DescribeTable(ctx, &schema.Table{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.VerifiableSQLGet(ctx, nil)
	require.ErrorIs(t, err, ErrNotLoggedIn)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}

	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	res, err := s.ListTables(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "BEGIN TRANSACTION"})
	require.ErrorIs(t, err, ErrTxNotProperlyClosed)

	xres, err := s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)"})
	require.NoError(t, err)
	require.Len(t, xres.Txs, 1)

	res, err = s.DescribeTable(ctx, &schema.Table{TableName: "table1"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	xres, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "INSERT INTO table1 (id) VALUES (1),(2),(3)"})
	require.NoError(t, err)
	require.Len(t, xres.Txs, 1)

	var nRows int
	err = s.SQLQuery(&schema.SQLQueryRequest{Sql: "SELECT * FROM table1"}, &ImmuService_SQLQueryServerMock{
		ctx: ctx,
		sendFunc: func(sr *schema.SQLQueryResult) error {
			nRows += len(sr.Rows)
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, nRows, 3)

	e, err := s.VerifiableSQLGet(ctx, &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}}},
		ProveSinceTx:  0,
	})
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = s.VerifiableSQLGet(ctx, &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}}},
		ProveSinceTx:  100,
	})
	require.ErrorIs(t, err, database.ErrIllegalState)
}

func TestSQLExecResult(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	ctx := context.Background()

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}

	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	xres, err := s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE TABLE table2 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)"})
	require.NoError(t, err)
	require.Len(t, xres.Txs, 1)

	xres, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "INSERT INTO table2 (name) VALUES ('first'),('second'),('third')"})
	require.NoError(t, err)
	require.Len(t, xres.Txs, 1)
	require.Equal(t, map[string]*schema.SQLValue{"table2": {Value: &schema.SQLValue_N{N: 1}}}, xres.FirstInsertedPks())
	require.Equal(t, map[string]*schema.SQLValue{"table2": {Value: &schema.SQLValue_N{N: 3}}}, xres.LastInsertedPk())
}

func TestSQLExecCreateDatabase(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	ctx := context.Background()

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}

	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE DATABASE db1;"})
	require.NoError(t, err)

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE DATABASE db1;"})
	require.ErrorContains(t, err, sql.ErrDatabaseAlreadyExists.Error())

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE DATABASE IF NOT EXISTS db1;"})
	require.NoError(t, err)

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE DATABASE IF NOT EXISTS db2;"})
	require.NoError(t, err)

	_, err = s.SQLExec(ctx, &schema.SQLExecRequest{Sql: "CREATE DATABASE db2;"})
	require.ErrorContains(t, err, sql.ErrDatabaseAlreadyExists.Error())
}

type ImmuService_SQLQueryServerMock struct {
	grpc.ServerStream
	sendFunc func(*schema.SQLQueryResult) error
	ctx      context.Context
}

func (s *ImmuService_SQLQueryServerMock) Send(res *schema.SQLQueryResult) error {
	return s.sendFunc(res)
}

func (s *ImmuService_SQLQueryServerMock) Context() context.Context {
	return s.ctx
}
