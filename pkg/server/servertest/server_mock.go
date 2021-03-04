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
	srv *server.ImmuServer

	PostSetFn           func(context.Context, *schema.SetRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableSetFn func(context.Context, *schema.VerifiableSetRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostSetReferenceFn           func(context.Context, *schema.ReferenceRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableSetReferenceFn func(context.Context, *schema.VerifiableReferenceRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostZAddFn           func(context.Context, *schema.ZAddRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
	PostVerifiableZAddFn func(context.Context, *schema.VerifiableZAddRequest, *schema.VerifiableTx, error) (*schema.VerifiableTx, error)

	PostExecAllFn func(context.Context, *schema.ExecAllRequest, *schema.TxMetadata, error) (*schema.TxMetadata, error)
}

func (s *ServerMock) StreamGet(request *schema.KeyRequest, getServer schema.ImmuService_StreamGetServer) error {
	return s.srv.StreamGet(request, getServer)
}

func (s *ServerMock) StreamSet(setServer schema.ImmuService_StreamSetServer) error {
	return s.srv.StreamSet(setServer)
}

func (s *ServerMock) StreamVerifiableGet(request *schema.VerifiableGetRequest, getServer schema.ImmuService_StreamVerifiableGetServer) error {
	return s.srv.StreamVerifiableGet(request, getServer)
}

func (s *ServerMock) StreamVerifiableSet(vSetServer schema.ImmuService_StreamVerifiableSetServer) error {
	return s.srv.StreamVerifiableSet(vSetServer)
}

func (s *ServerMock) StreamScan(request *schema.ScanRequest, scanServer schema.ImmuService_StreamScanServer) error {
	return s.srv.StreamScan(request, scanServer)
}

func (s *ServerMock) StreamZScan(request *schema.ZScanRequest, zscanServer schema.ImmuService_StreamZScanServer) error {
	return s.srv.StreamZScan(request, zscanServer)
}

func (s *ServerMock) StreamHistory(request *schema.HistoryRequest, historyServer schema.ImmuService_StreamHistoryServer) error {
	return s.srv.StreamHistory(request, historyServer)
}

func (s *ServerMock) ListUsers(ctx context.Context, req *empty.Empty) (*schema.UserList, error) {
	return s.srv.ListUsers(ctx, req)
}

func (s *ServerMock) CreateUser(ctx context.Context, req *schema.CreateUserRequest) (*empty.Empty, error) {
	return s.srv.CreateUser(ctx, req)
}

func (s *ServerMock) ChangePassword(ctx context.Context, req *schema.ChangePasswordRequest) (*empty.Empty, error) {
	return s.srv.ChangePassword(ctx, req)
}

func (s *ServerMock) UpdateAuthConfig(ctx context.Context, req *schema.AuthConfig) (*empty.Empty, error) {
	return s.srv.UpdateAuthConfig(ctx, req)
}

func (s *ServerMock) UpdateMTLSConfig(ctx context.Context, req *schema.MTLSConfig) (*empty.Empty, error) {
	return s.srv.UpdateMTLSConfig(ctx, req)
}

func (s *ServerMock) Login(ctx context.Context, req *schema.LoginRequest) (*schema.LoginResponse, error) {
	return s.srv.Login(ctx, req)
}

func (s *ServerMock) Logout(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return s.srv.Logout(ctx, req)
}

func (s *ServerMock) Set(ctx context.Context, req *schema.SetRequest) (*schema.TxMetadata, error) {
	if s.PostSetFn == nil {
		return s.srv.Set(ctx, req)
	}

	rsp, err := s.srv.Set(ctx, req)
	return s.PostSetFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableSetFn == nil {
		return s.srv.VerifiableSet(ctx, req)
	}

	rsp, err := s.srv.VerifiableSet(ctx, req)
	return s.PostVerifiableSetFn(ctx, req, rsp, err)
}

func (s *ServerMock) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	return s.srv.Get(ctx, req)
}

func (s *ServerMock) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	return s.srv.VerifiableGet(ctx, req)
}

func (s *ServerMock) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	return s.srv.GetAll(ctx, req)
}

func (s *ServerMock) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	if s.PostExecAllFn == nil {
		return s.srv.ExecAll(ctx, req)
	}

	rsp, err := s.srv.ExecAll(ctx, req)
	return s.PostExecAllFn(ctx, req, rsp, err)
}

func (s *ServerMock) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	return s.srv.Scan(ctx, req)
}

func (s *ServerMock) Count(ctx context.Context, req *schema.KeyPrefix) (*schema.EntryCount, error) {
	return s.srv.Count(ctx, req)
}

func (s *ServerMock) CountAll(ctx context.Context, req *empty.Empty) (*schema.EntryCount, error) {
	return s.srv.CountAll(ctx, req)
}

func (s *ServerMock) TxById(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	return s.srv.TxById(ctx, req)
}

func (s *ServerMock) VerifiableTxById(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	return s.srv.VerifiableTxById(ctx, req)
}

func (s *ServerMock) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	return s.srv.TxScan(ctx, req)
}

func (s *ServerMock) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	return s.srv.History(ctx, req)
}

func (s *ServerMock) Health(ctx context.Context, req *empty.Empty) (*schema.HealthResponse, error) {
	return s.srv.Health(ctx, req)
}

func (s *ServerMock) CurrentState(ctx context.Context, req *empty.Empty) (*schema.ImmutableState, error) {
	return s.srv.CurrentState(ctx, req)
}

func (s *ServerMock) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	if s.PostSetReferenceFn == nil {
		return s.srv.SetReference(ctx, req)
	}

	rsp, err := s.srv.SetReference(ctx, req)
	return s.PostSetReferenceFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableSetReferenceFn == nil {
		return s.srv.VerifiableSetReference(ctx, req)
	}

	rsp, err := s.srv.VerifiableSetReference(ctx, req)
	return s.PostVerifiableSetReferenceFn(ctx, req, rsp, err)
}

func (s *ServerMock) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxMetadata, error) {
	if s.PostZAddFn == nil {
		return s.srv.ZAdd(ctx, req)
	}

	rsp, err := s.srv.ZAdd(ctx, req)
	return s.PostZAddFn(ctx, req, rsp, err)
}

func (s *ServerMock) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	if s.PostVerifiableZAddFn == nil {
		return s.srv.VerifiableZAdd(ctx, req)
	}

	rsp, err := s.srv.VerifiableZAdd(ctx, req)
	return s.PostVerifiableZAddFn(ctx, req, rsp, err)
}

func (s *ServerMock) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	return s.srv.ZScan(ctx, req)
}

func (s *ServerMock) CreateDatabase(ctx context.Context, req *schema.Database) (*empty.Empty, error) {
	return s.srv.CreateDatabase(ctx, req)
}

func (s *ServerMock) DatabaseList(ctx context.Context, req *empty.Empty) (*schema.DatabaseListResponse, error) {
	return s.srv.DatabaseList(ctx, req)
}

func (s *ServerMock) UseDatabase(ctx context.Context, req *schema.Database) (*schema.UseDatabaseReply, error) {
	return s.srv.UseDatabase(ctx, req)
}

func (s *ServerMock) CleanIndex(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return s.srv.CleanIndex(ctx, req)
}

func (s *ServerMock) ChangePermission(ctx context.Context, req *schema.ChangePermissionRequest) (*empty.Empty, error) {
	return s.srv.ChangePermission(ctx, req)
}

func (s *ServerMock) SetActiveUser(ctx context.Context, req *schema.SetActiveUserRequest) (*empty.Empty, error) {
	return s.srv.SetActiveUser(ctx, req)
}
