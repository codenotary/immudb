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
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc"
)

var (
	_ client.ImmuClient = (*ImmuClientMock)(nil)
)

// ImmuClientMock ...
type ImmuClientMock struct {
	immuclient.ImmuClient

	GetOptionsF           func() *client.Options
	IsConnectedF          func() bool
	HealthCheckF          func(context.Context) error
	WaitForHealthCheckF   func(context.Context) error
	ConnectF              func(context.Context) (*grpc.ClientConn, error)
	DisconnectF           func() error
	LoginF                func(context.Context, []byte, []byte) (*schema.LoginResponse, error)
	LogoutF               func(context.Context) error
	VerifiedGetF          func(context.Context, []byte, ...client.GetOption) (*schema.Entry, error)
	VerifiedGetAtF        func(context.Context, []byte, uint64) (*schema.Entry, error)
	VerifiedSetF          func(context.Context, []byte, []byte) (*schema.TxHeader, error)
	VerifiableGetF        func(context.Context, *schema.VerifiableGetRequest, ...grpc.CallOption) (*schema.VerifiableEntry, error)
	SetF                  func(context.Context, []byte, []byte) (*schema.TxHeader, error)
	SetAllF               func(context.Context, *schema.SetRequest) (*schema.TxHeader, error)
	SetReferenceF         func(context.Context, []byte, []byte, uint64) (*schema.TxHeader, error)
	VerifiedSetReferenceF func(context.Context, []byte, []byte, uint64) (*schema.TxHeader, error)
	ZAddF                 func(context.Context, []byte, float64, []byte, uint64) (*schema.TxHeader, error)
	VerifiedZAddF         func(context.Context, []byte, float64, []byte, uint64) (*schema.TxHeader, error)
	HistoryF              func(context.Context, *schema.HistoryRequest) (*schema.Entries, error)
	UseDatabaseF          func(context.Context, *schema.Database) (*schema.UseDatabaseReply, error)
	DumpF                 func(context.Context, io.WriteSeeker) (int64, error)
	CurrentStateF         func(context.Context) (*schema.ImmutableState, error)
	TxByIDF               func(context.Context, uint64) (*schema.Tx, error)
	GetF                  func(context.Context, []byte, ...client.GetOption) (*schema.Entry, error)
	VerifiedTxByIDF       func(context.Context, uint64) (*schema.Tx, error)
	ListUsersF            func(context.Context) (*schema.UserList, error)
	SetActiveUserF        func(context.Context, *schema.SetActiveUserRequest) error
	ChangePermissionF     func(context.Context, schema.PermissionAction, string, string, uint32) error
	ZScanF                func(context.Context, *schema.ZScanRequest) (*schema.ZEntries, error)
	ScanF                 func(context.Context, *schema.ScanRequest) (*schema.Entries, error)
	CountF                func(context.Context, []byte) (*schema.EntryCount, error)
	CreateDatabaseF       func(context.Context, *schema.DatabaseSettings) error
	CreateDatabaseV2F     func(context.Context, string, *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error)
	UpdateDatabaseF       func(context.Context, *schema.DatabaseSettings) error
	UpdateDatabaseV2F     func(context.Context, string, *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error)
	DatabaseListF         func(context.Context) (*schema.DatabaseListResponse, error)
	ChangePasswordF       func(context.Context, []byte, []byte, []byte) error
	CreateUserF           func(context.Context, []byte, []byte, uint32, string) error
}

// GetOptions ...
func (icm *ImmuClientMock) GetOptions() *client.Options {
	return icm.GetOptionsF()
}

// IsConnected ...
func (icm *ImmuClientMock) IsConnected() bool {
	return icm.IsConnectedF()
}

// HealthCheck ...
func (icm *ImmuClientMock) HealthCheck(ctx context.Context) error {
	return icm.HealthCheckF(ctx)
}

// WaitForHealthCheck ...
func (icm *ImmuClientMock) WaitForHealthCheck(ctx context.Context) (err error) {
	return icm.WaitForHealthCheckF(ctx)
}

// Connect ...
func (icm *ImmuClientMock) Connect(ctx context.Context) (clientConn *grpc.ClientConn, err error) {
	return icm.ConnectF(ctx)
}

// Disconnect ...
func (icm *ImmuClientMock) Disconnect() error {
	return icm.DisconnectF()
}

// Login ...
func (icm *ImmuClientMock) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	return icm.LoginF(ctx, user, pass)
}

// Logout ...
func (icm *ImmuClientMock) Logout(ctx context.Context) error {
	return icm.LogoutF(ctx)
}

// VerifiedGet ...
func (icm *ImmuClientMock) VerifiedGet(ctx context.Context, key []byte, opts ...client.GetOption) (*schema.Entry, error) {
	return icm.VerifiedGetF(ctx, key, opts...)
}

// VerifiedGetAt ...
func (icm *ImmuClientMock) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return icm.VerifiedGetAtF(ctx, key, tx)
}

// VerifiedSet ...
func (icm *ImmuClientMock) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return icm.VerifiedSetF(ctx, key, value)
}

// VerifiedSet ...
func (icm *ImmuClientMock) VerifiableGet(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error) {
	return icm.VerifiableGetF(ctx, in, opts...)
}

// Set ...
func (icm *ImmuClientMock) Set(ctx context.Context, key []byte, value []byte) (*schema.TxHeader, error) {
	return icm.SetF(ctx, key, value)
}

func (icm *ImmuClientMock) SetAll(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	return icm.SetAllF(ctx, req)
}

// SetReference ...
func (icm *ImmuClientMock) SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return icm.SetReferenceF(ctx, key, referencedKey, 0)
}

// VerifiedSetReference ...
func (icm *ImmuClientMock) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxHeader, error) {
	return icm.VerifiedSetReferenceF(ctx, key, referencedKey, 0)
}

// SetReferenceAt ...
func (icm *ImmuClientMock) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error) {
	return icm.SetReferenceF(ctx, key, referencedKey, atTx)
}

// VerifiedSetReferenceAt ...
func (icm *ImmuClientMock) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxHeader, error) {
	return icm.VerifiedSetReferenceF(ctx, key, referencedKey, atTx)
}

// ZAdd ...
func (icm *ImmuClientMock) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return icm.ZAddF(ctx, set, score, key, 0)
}

// SafeZAdd ...
func (icm *ImmuClientMock) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxHeader, error) {
	return icm.VerifiedZAddF(ctx, set, score, key, 0)
}

// ZAddAt ...
func (icm *ImmuClientMock) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error) {
	return icm.ZAddF(ctx, set, score, key, atTx)
}

// VerifiedZAddAt ...
func (icm *ImmuClientMock) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxHeader, error) {
	return icm.VerifiedZAddF(ctx, set, score, key, atTx)
}

// History ...
func (icm *ImmuClientMock) History(ctx context.Context, options *schema.HistoryRequest) (*schema.Entries, error) {
	return icm.HistoryF(ctx, options)
}

// UseDatabase ...
func (icm *ImmuClientMock) UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
	return icm.UseDatabaseF(ctx, d)
}

// UpdateDatabase ...
func (icm *ImmuClientMock) UpdateDatabase(ctx context.Context, s *schema.DatabaseSettings) error {
	return icm.UpdateDatabaseF(ctx, s)
}

// UpdateDatabaseV2 ...
func (icm *ImmuClientMock) UpdateDatabaseV2(ctx context.Context, db string, setttings *schema.DatabaseNullableSettings) (*schema.UpdateDatabaseResponse, error) {
	return icm.UpdateDatabaseV2F(ctx, db, setttings)
}

// Dump ...
func (icm *ImmuClientMock) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	return icm.DumpF(ctx, writer)
}

// CurrentState ...
func (icm *ImmuClientMock) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	return icm.CurrentStateF(ctx)
}

// Get ...
func (icm *ImmuClientMock) Get(ctx context.Context, key []byte, opts ...client.GetOption) (*schema.Entry, error) {
	return icm.GetF(ctx, key, opts...)
}

// TxByID ...
func (icm *ImmuClientMock) TxByID(ctx context.Context, ID uint64) (*schema.Tx, error) {
	return icm.TxByIDF(ctx, ID)
}

// VerifiedTxByID ...
func (icm *ImmuClientMock) VerifiedTxByID(ctx context.Context, tx uint64) (*schema.Tx, error) {
	return icm.VerifiedTxByIDF(ctx, tx)
}

// ListUsers ...
func (icm *ImmuClientMock) ListUsers(ctx context.Context) (*schema.UserList, error) {
	return icm.ListUsersF(ctx)
}

// SetActiveUser ...
func (icm *ImmuClientMock) SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) error {
	return icm.SetActiveUserF(ctx, u)
}

// ChangePermission ...
func (icm *ImmuClientMock) ChangePermission(ctx context.Context, action schema.PermissionAction, username string, database string, permissions uint32) error {
	return icm.ChangePermissionF(ctx, action, username, database, permissions)
}

// ZScan ...
func (icm *ImmuClientMock) ZScan(ctx context.Context, request *schema.ZScanRequest) (*schema.ZEntries, error) {
	return icm.ZScanF(ctx, request)
}

// Scan ...
func (icm *ImmuClientMock) Scan(ctx context.Context, request *schema.ScanRequest) (*schema.Entries, error) {
	return icm.ScanF(ctx, request)
}

// Count ...
func (icm *ImmuClientMock) Count(ctx context.Context, prefix []byte) (*schema.EntryCount, error) {
	return icm.CountF(ctx, prefix)
}

// CreateDatabase ...
func (icm *ImmuClientMock) CreateDatabase(ctx context.Context, db *schema.DatabaseSettings) error {
	return icm.CreateDatabaseF(ctx, db)
}

// CreateDatabaseV2 ...
func (icm *ImmuClientMock) CreateDatabaseV2(ctx context.Context, db string, setttings *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error) {
	return icm.CreateDatabaseV2F(ctx, db, setttings)
}

// DatabaseList ...
func (icm *ImmuClientMock) DatabaseList(ctx context.Context) (*schema.DatabaseListResponse, error) {
	return icm.DatabaseListF(ctx)
}

// ChangePassword ...
func (icm *ImmuClientMock) ChangePassword(ctx context.Context, user []byte, oldPass []byte, newPass []byte) error {
	return icm.ChangePasswordF(ctx, user, oldPass, newPass)
}

// CreateUser ...
func (icm *ImmuClientMock) CreateUser(ctx context.Context, user []byte, pass []byte, permission uint32, databasename string) error {
	return icm.CreateUserF(ctx, user, pass, permission, databasename)
}
