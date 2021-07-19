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
	VerifiedGetF          func(context.Context, []byte) (*schema.Entry, error)
	VerifiedGetAtF        func(context.Context, []byte, uint64) (*schema.Entry, error)
	VerifiedSetF          func(context.Context, []byte, []byte) (*schema.TxMetadata, error)
	SetF                  func(context.Context, []byte, []byte) (*schema.TxMetadata, error)
	SetReferenceF         func(context.Context, []byte, []byte, uint64) (*schema.TxMetadata, error)
	VerifiedSetReferenceF func(context.Context, []byte, []byte, uint64) (*schema.TxMetadata, error)
	ZAddF                 func(context.Context, []byte, float64, []byte, uint64) (*schema.TxMetadata, error)
	VerifiedZAddF         func(context.Context, []byte, float64, []byte, uint64) (*schema.TxMetadata, error)
	HistoryF              func(context.Context, *schema.HistoryRequest) (*schema.Entries, error)
	UseDatabaseF          func(context.Context, *schema.Database) (*schema.UseDatabaseReply, error)
	DumpF                 func(context.Context, io.WriteSeeker) (int64, error)
	CurrentStateF         func(context.Context) (*schema.ImmutableState, error)
	TxByIDF               func(context.Context, uint64) (*schema.Tx, error)
	GetF                  func(context.Context, []byte) (*schema.Entry, error)
	VerifiedTxByIDF       func(context.Context, uint64) (*schema.Tx, error)
	ListUsersF            func(context.Context) (*schema.UserList, error)
	SetActiveUserF        func(context.Context, *schema.SetActiveUserRequest) error
	ChangePermissionF     func(context.Context, schema.PermissionAction, string, string, uint32) error
	ZScanF                func(context.Context, *schema.ZScanRequest) (*schema.ZEntries, error)
	ScanF                 func(context.Context, *schema.ScanRequest) (*schema.Entries, error)
	CountF                func(context.Context, []byte) (*schema.EntryCount, error)
	CreateDatabaseF       func(context.Context, *schema.Database) error
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
func (icm *ImmuClientMock) VerifiedGet(ctx context.Context, key []byte) (*schema.Entry, error) {
	return icm.VerifiedGetF(ctx, key)
}

// VerifiedGetAt ...
func (icm *ImmuClientMock) VerifiedGetAt(ctx context.Context, key []byte, tx uint64) (*schema.Entry, error) {
	return icm.VerifiedGetAtF(ctx, key, tx)
}

// VerifiedSet ...
func (icm *ImmuClientMock) VerifiedSet(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	return icm.VerifiedSetF(ctx, key, value)
}

// Set ...
func (icm *ImmuClientMock) Set(ctx context.Context, key []byte, value []byte) (*schema.TxMetadata, error) {
	return icm.SetF(ctx, key, value)
}

// SetReference ...
func (icm *ImmuClientMock) SetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return icm.SetReferenceF(ctx, key, referencedKey, 0)
}

// VerifiedSetReference ...
func (icm *ImmuClientMock) VerifiedSetReference(ctx context.Context, key []byte, referencedKey []byte) (*schema.TxMetadata, error) {
	return icm.VerifiedSetReferenceF(ctx, key, referencedKey, 0)
}

// SetReferenceAt ...
func (icm *ImmuClientMock) SetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error) {
	return icm.SetReferenceF(ctx, key, referencedKey, atTx)
}

// VerifiedSetReferenceAt ...
func (icm *ImmuClientMock) VerifiedSetReferenceAt(ctx context.Context, key []byte, referencedKey []byte, atTx uint64) (*schema.TxMetadata, error) {
	return icm.VerifiedSetReferenceF(ctx, key, referencedKey, atTx)
}

// ZAdd ...
func (icm *ImmuClientMock) ZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return icm.ZAddF(ctx, set, score, key, 0)
}

// SafeZAdd ...
func (icm *ImmuClientMock) VerifiedZAdd(ctx context.Context, set []byte, score float64, key []byte) (*schema.TxMetadata, error) {
	return icm.VerifiedZAddF(ctx, set, score, key, 0)
}

// ZAddAt ...
func (icm *ImmuClientMock) ZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error) {
	return icm.ZAddF(ctx, set, score, key, atTx)
}

// VerifiedZAddAt ...
func (icm *ImmuClientMock) VerifiedZAddAt(ctx context.Context, set []byte, score float64, key []byte, atTx uint64) (*schema.TxMetadata, error) {
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
	return icm.UpdateDatabase(ctx, s)
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
func (icm *ImmuClientMock) Get(ctx context.Context, key []byte) (*schema.Entry, error) {
	return icm.GetF(ctx, key)
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
func (icm *ImmuClientMock) CreateDatabase(ctx context.Context, db *schema.Database) error {
	return icm.CreateDatabaseF(ctx, db)
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
