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

package servertest

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/golang/protobuf/ptypes/empty"
)

type ServerMock struct {
	Srv *server.ImmuServer

	PostSetFn           func(context.Context, *schema.SetRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableSetFn func(context.Context, *schema.VerifiableSetRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostSetReferenceFn           func(context.Context, *schema.ReferenceRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableSetReferenceFn func(context.Context, *schema.VerifiableReferenceRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostZAddFn           func(context.Context, *schema.ZAddRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableZAddFn func(context.Context, *schema.VerifiableZAddRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostExecAllFn func(context.Context, *schema.ExecAllRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)

	GetDbIndexFromCtx func(context.Context, string) (int64, error)
}

func (s *ServerMock) StreamExecAll(allServer schema.ImmuService_StreamExecAllServer) error {
	return s.Srv.StreamExecAll(allServer)
}

func (s *ServerMock) StreamGet(request *schema.KeyRequest, getServer schema.ImmuService_StreamGetServer) error {
	return s.Srv.StreamGet(request, getServer)
}

func (s *ServerMock) StreamSet(setServer schema.ImmuService_StreamSetServer) error {
	return s.Srv.StreamSet(setServer)
}

func (s *ServerMock) StreamVerifiableGet(request *schema.VerifiableGetRequest, getServer schema.ImmuService_StreamVerifiableGetServer) error {
	return s.Srv.StreamVerifiableGet(request, getServer)
}

func (s *ServerMock) StreamVerifiableSet(vSetServer schema.ImmuService_StreamVerifiableSetServer) error {
	return s.Srv.StreamVerifiableSet(vSetServer)
}

func (s *ServerMock) StreamScan(request *schema.ScanRequest, scanServer schema.ImmuService_StreamScanServer) error {
	return s.Srv.StreamScan(request, scanServer)
}

func (s *ServerMock) StreamZScan(request *schema.ZScanRequest, zscanServer schema.ImmuService_StreamZScanServer) error {
	return s.Srv.StreamZScan(request, zscanServer)
}

func (s *ServerMock) StreamHistory(request *schema.HistoryRequest, historyServer schema.ImmuService_StreamHistoryServer) error {
	return s.Srv.StreamHistory(request, historyServer)
}

func (s *ServerMock) ExportTx(req *schema.TxRequest, txsServer schema.ImmuService_ExportTxServer) error {
	return s.Srv.ExportTx(req, txsServer)
}

func (s *ServerMock) ReplicateTx(replicateTxServer schema.ImmuService_ReplicateTxServer) error {
	return s.Srv.ReplicateTx(replicateTxServer)
}

func (s *ServerMock) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	return s.Srv.ListUsers(ctx, req)
}

func (s *ServerMock) CreateUser(ctx context.Context, req *schema.CreateUserRequest) (*empty.Empty, error) {
	return s.Srv.CreateUser(ctx, req)
}

func (s *ServerMock) ChangePassword(ctx context.Context, req *schema.ChangePasswordRequest) (*empty.Empty, error) {
	return s.Srv.ChangePassword(ctx, req)
}

func (s *ServerMock) UpdateAuthConfig(ctx context.Context, req *schema.AuthConfig) (*empty.Empty, error) {
	return s.Srv.UpdateAuthConfig(ctx, req)
}

func (s *ServerMock) UpdateMTLSConfig(ctx context.Context, req *schema.MTLSConfig) (*empty.Empty, error) {
	return s.Srv.UpdateMTLSConfig(ctx, req)
}

func (s *ServerMock) Login(ctx context.Context, req *schema.LoginRequest) (*schema.LoginResponse, error) {
	return s.Srv.Login(ctx, req)
}

func (s *ServerMock) Logout(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return s.Srv.Logout(ctx, req)
}

func (s *ServerMock) Set(ctx context.Context, req *schema.SetRequest) (*schema.TxMetadata, error) {
	if s.PostSetFn == nil {
		return s.Srv.Set(ctx, req)
	}

	rsp, err := s.Srv.Set(ctx, req)
	return s.PostSetFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableSetFn == nil {
		return s.Srv.VerifiableSet(ctx, req)
	}

	rsp, err := s.Srv.VerifiableSet(ctx, req)
	return s.PostVerifiableSetFn(ctx, req, rsp, err)
}

func (s *ServerMock) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	return s.Srv.Get(ctx, req)
}

func (s *ServerMock) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	return s.Srv.VerifiableGet(ctx, req)
}

func (s *ServerMock) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	return s.Srv.GetAll(ctx, req)
}

func (s *ServerMock) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	if s.PostExecAllFn == nil {
		return s.Srv.ExecAll(ctx, req)
	}

	rsp, err := s.Srv.ExecAll(ctx, req)
	return s.PostExecAllFn(ctx, req, rsp, err)
}

func (s *ServerMock) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	return s.Srv.Scan(ctx, req)
}

func (s *ServerMock) Count(ctx context.Context, req *schema.KeyPrefix) (*schema.EntryCount, error) {
	return s.Srv.Count(ctx, req)
}

func (s *ServerMock) CountAll(ctx context.Context, req *empty.Empty) (*schema.EntryCount, error) {
	return s.Srv.CountAll(ctx, req)
}

func (s *ServerMock) TxById(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	return s.Srv.TxById(ctx, req)
}

func (s *ServerMock) VerifiableTxById(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	return s.Srv.VerifiableTxById(ctx, req)
}

func (s *ServerMock) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	return s.Srv.TxScan(ctx, req)
}

func (s *ServerMock) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	return s.Srv.History(ctx, req)
}

func (s *ServerMock) Health(ctx context.Context, req *empty.Empty) (*schema.HealthResponse, error) {
	return s.Srv.Health(ctx, req)
}

func (s *ServerMock) CurrentState(ctx context.Context, req *empty.Empty) (*schema.ImmutableState, error) {
	return s.Srv.CurrentState(ctx, req)
}

func (s *ServerMock) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	if s.PostSetReferenceFn == nil {
		return s.Srv.SetReference(ctx, req)
	}

	rsp, err := s.Srv.SetReference(ctx, req)
	return s.PostSetReferenceFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableSetReferenceFn == nil {
		return s.Srv.VerifiableSetReference(ctx, req)
	}

	rsp, err := s.Srv.VerifiableSetReference(ctx, req)
	return s.PostVerifiableSetReferenceFn(ctx, req, rsp, err)
}

func (s *ServerMock) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxMetadata, error) {
	if s.PostZAddFn == nil {
		return s.Srv.ZAdd(ctx, req)
	}

	rsp, err := s.Srv.ZAdd(ctx, req)
	return s.PostZAddFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableZAddFn == nil {
		return s.Srv.VerifiableZAdd(ctx, req)
	}

	rsp, err := s.Srv.VerifiableZAdd(ctx, req)
	return s.PostVerifiableZAddFn(ctx, req, rsp, err)
}

func (s *ServerMock) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	return s.Srv.ZScan(ctx, req)
}

func (s *ServerMock) CreateDatabase(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	return s.Srv.CreateDatabase(ctx, req)
}

func (s *ServerMock) DatabaseList(ctx context.Context, req *empty.Empty) (*schema.DatabaseListResponse, error) {
	return s.Srv.DatabaseList(ctx, req)
}

func (s *ServerMock) UseDatabase(ctx context.Context, req *schema.Database) (*schema.UseDatabaseReply, error) {
	return s.Srv.UseDatabase(ctx, req)
}

func (s *ServerMock) UpdateDatabase(ctx context.Context, req *schema.DatabaseSettings) (*empty.Empty, error) {
	return s.Srv.UpdateDatabase(ctx, req)
}

func (s *ServerMock) CleanIndex(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return s.Srv.CleanIndex(ctx, req)
}

func (s *ServerMock) CompactIndex(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return s.Srv.CompactIndex(ctx, req)
}

func (s *ServerMock) ChangePermission(ctx context.Context, req *schema.ChangePermissionRequest) (*empty.Empty, error) {
	return s.Srv.ChangePermission(ctx, req)
}

func (s *ServerMock) SetActiveUser(ctx context.Context, req *schema.SetActiveUserRequest) (*empty.Empty, error) {
	return s.Srv.SetActiveUser(ctx, req)
}

func (s *ServerMock) getDbIndexFromCtx(ctx context.Context, methodname string) (int64, error) {
	return s.GetDbIndexFromCtx(ctx, methodname)
}

func (s *ServerMock) Stop() error {
	return s.Srv.Stop()
}

func (s *ServerMock) Initialize() error {
	return s.Srv.Initialize()
}

func (s *ServerMock) SQLExec(ctx context.Context, req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	return s.Srv.SQLExec(ctx, req)
}

func (s *ServerMock) UseSnapshot(ctx context.Context, req *schema.UseSnapshotRequest) (*empty.Empty, error) {
	return s.Srv.UseSnapshot(ctx, req)
}

func (s *ServerMock) SQLQuery(ctx context.Context, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	return s.Srv.SQLQuery(ctx, req)
}

func (s *ServerMock) ListTables(ctx context.Context, req *empty.Empty) (*schema.SQLQueryResult, error) {
	return s.Srv.ListTables(ctx, req)
}

func (s *ServerMock) DescribeTable(ctx context.Context, req *schema.Table) (*schema.SQLQueryResult, error) {
	return s.Srv.DescribeTable(ctx, req)
}

func (s *ServerMock) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	return s.Srv.VerifiableSQLGet(ctx, req)
}
