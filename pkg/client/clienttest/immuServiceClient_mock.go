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

// Package clienttest ...
package clienttest

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

var (
	_ schema.ImmuServiceClient = (*ImmuServiceClientMock)(nil)
)

// ImmuServiceClientMock ...
type ImmuServiceClientMock struct {
	schema.ImmuServiceClient

	ListUsersF        func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.UserList, error)
	GetUserF          func(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) error
	CreateUserF       func(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	ChangePasswordF   func(ctx context.Context, in *schema.ChangePasswordRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	DeactivateUserF   func(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	UpdateAuthConfigF func(ctx context.Context, in *schema.AuthConfig, opts ...grpc.CallOption) (*empty.Empty, error)
	UpdateMTLSConfigF func(ctx context.Context, in *schema.MTLSConfig, opts ...grpc.CallOption) (*empty.Empty, error)
	LoginF            func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error)
	LogoutF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error)

	SetF           func(ctx context.Context, in *schema.SetRequest, opts ...grpc.CallOption) (*schema.TxHeader, error)
	VerifiableSetF func(ctx context.Context, in *schema.VerifiableSetRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error)
	GetF           func(ctx context.Context, in *schema.KeyRequest, opts ...grpc.CallOption) (*schema.Entry, error)
	VerifiableGetF func(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error)

	GetAllF                 func(ctx context.Context, in *schema.KeyListRequest, opts ...grpc.CallOption) (*schema.Entries, error)
	ExecAllF                func(ctx context.Context, in *schema.ExecAllRequest, opts ...grpc.CallOption) (*schema.TxHeader, error)
	ScanF                   func(ctx context.Context, in *schema.ScanRequest, opts ...grpc.CallOption) (*schema.Entries, error)
	CountF                  func(ctx context.Context, in *schema.KeyPrefix, opts ...grpc.CallOption) (*schema.EntryCount, error)
	CountAllF               func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.EntryCount, error)
	TxByIdF                 func(ctx context.Context, in *schema.TxRequest, opts ...grpc.CallOption) (*schema.Tx, error)
	VerifiableTxByIdF       func(ctx context.Context, in *schema.VerifiableTxRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error)
	HistoryF                func(ctx context.Context, in *schema.HistoryRequest, opts ...grpc.CallOption) (*schema.Entries, error)
	HealthF                 func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error)
	CurrentStateF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.ImmutableState, error)
	SetReferenceF           func(ctx context.Context, in *schema.ReferenceRequest, opts ...grpc.CallOption) (*schema.TxHeader, error)
	VerifiableSetReferenceF func(ctx context.Context, in *schema.VerifiableReferenceRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error)
	ZAddF                   func(ctx context.Context, in *schema.ZAddRequest, opts ...grpc.CallOption) (*schema.TxHeader, error)
	VerifiableZAddF         func(ctx context.Context, in *schema.VerifiableZAddRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error)
	ZScanF                  func(ctx context.Context, in *schema.ZScanRequest, opts ...grpc.CallOption) (*schema.ZEntries, error)
	CreateDatabaseF         func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error)
	CreateDatabaseWithF     func(ctx context.Context, in *schema.DatabaseSettings, opts ...grpc.CallOption) (*empty.Empty, error)
	UseDatabaseF            func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error)
	UpdateDatabaseF         func(ctx context.Context, in *schema.DatabaseSettings, opts ...grpc.CallOption) (*empty.Empty, error)
	ChangePermissionF       func(ctx context.Context, in *schema.ChangePermissionRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	SetActiveUserF          func(ctx context.Context, in *schema.SetActiveUserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	DatabaseListF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error)
	OpenSessionF            func(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error)
}

func (icm *ImmuServiceClientMock) ListUsers(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.UserList, error) {
	return icm.ListUsersF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) GetUser(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) error {
	return icm.GetUserF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) CreateUser(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.CreateUserF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) ChangePassword(ctx context.Context, in *schema.ChangePasswordRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.ChangePasswordF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) DeactivateUser(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.DeactivateUserF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) UpdateAuthConfig(ctx context.Context, in *schema.AuthConfig, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.UpdateAuthConfigF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) UpdateMTLSConfig(ctx context.Context, in *schema.MTLSConfig, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.UpdateMTLSConfigF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Login(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
	return icm.LoginF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Logout(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.LogoutF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Set(ctx context.Context, in *schema.SetRequest, opts ...grpc.CallOption) (*schema.TxHeader, error) {
	return icm.SetF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) VerifiableSet(ctx context.Context, in *schema.VerifiableSetRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error) {
	return icm.VerifiableSetF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Get(ctx context.Context, in *schema.KeyRequest, opts ...grpc.CallOption) (*schema.Entry, error) {
	return icm.GetF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) VerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error) {
	return icm.VerifiableGetF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) GetAll(ctx context.Context, in *schema.KeyListRequest, opts ...grpc.CallOption) (*schema.Entries, error) {
	return icm.GetAllF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) ExecAll(ctx context.Context, in *schema.ExecAllRequest, opts ...grpc.CallOption) (*schema.TxHeader, error) {
	return icm.ExecAllF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Scan(ctx context.Context, in *schema.ScanRequest, opts ...grpc.CallOption) (*schema.Entries, error) {
	return icm.ScanF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Count(ctx context.Context, in *schema.KeyPrefix, opts ...grpc.CallOption) (*schema.EntryCount, error) {
	return icm.CountF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) CountAll(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.EntryCount, error) {
	return icm.CountAllF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) TxById(ctx context.Context, in *schema.TxRequest, opts ...grpc.CallOption) (*schema.Tx, error) {
	return icm.TxByIdF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) VerifiableTxById(ctx context.Context, in *schema.VerifiableTxRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error) {
	return icm.VerifiableTxByIdF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) History(ctx context.Context, in *schema.HistoryRequest, opts ...grpc.CallOption) (*schema.Entries, error) {
	return icm.HistoryF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Health(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
	return icm.HealthF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) CurrentState(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.ImmutableState, error) {
	return icm.CurrentStateF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) SetReference(ctx context.Context, in *schema.ReferenceRequest, opts ...grpc.CallOption) (*schema.TxHeader, error) {
	return icm.SetReferenceF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) VerifiableSetReference(ctx context.Context, in *schema.VerifiableReferenceRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error) {
	return icm.VerifiableSetReferenceF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) ZAdd(ctx context.Context, in *schema.ZAddRequest, opts ...grpc.CallOption) (*schema.TxHeader, error) {
	return icm.ZAddF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) VerifiableZAdd(ctx context.Context, in *schema.VerifiableZAddRequest, opts ...grpc.CallOption) (*schema.VerifiableTx, error) {
	return icm.VerifiableZAddF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) ZScan(ctx context.Context, in *schema.ZScanRequest, opts ...grpc.CallOption) (*schema.ZEntries, error) {
	return icm.ZScanF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) CreateDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.CreateDatabaseF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) CreateDatabaseWith(ctx context.Context, in *schema.DatabaseSettings, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.CreateDatabaseWithF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) UseDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
	return icm.UseDatabaseF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) UpdateDatabase(ctx context.Context, in *schema.DatabaseSettings, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.UpdateDatabaseF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) ChangePermission(ctx context.Context, in *schema.ChangePermissionRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.ChangePermissionF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) SetActiveUser(ctx context.Context, in *schema.SetActiveUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.SetActiveUserF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) DatabaseList(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
	return icm.DatabaseListF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) OpenSession(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error) {
	return icm.OpenSessionF(ctx, in, opts...)
}
