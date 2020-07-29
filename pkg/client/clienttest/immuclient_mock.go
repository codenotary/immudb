/*
Copyright 2019-2020 vChain, Inc.

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
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// ImmuClientMock ...
type ImmuClientMock struct {
	immuclient.ImmuClient

	GetOptionsF         func() *client.Options
	IsConnectedF        func() bool
	WaitForHealthCheckF func(context.Context) error
	ConnectF            func(context.Context) (*grpc.ClientConn, error)
	DisconnectF         func() error
	LoginF              func(context.Context, []byte, []byte) (*schema.LoginResponse, error)
	LogoutF             func(context.Context) error
	SafeGetF            func(context.Context, []byte, ...grpc.CallOption) (*client.VerifiedItem, error)
	SafeSetF            func(context.Context, []byte, []byte) (*client.VerifiedIndex, error)
	SetF                func(context.Context, []byte, []byte) (*schema.Index, error)
	SafeReferenceF      func(context.Context, []byte, []byte) (*client.VerifiedIndex, error)
	SafeZAddF           func(context.Context, []byte, float64, []byte) (*client.VerifiedIndex, error)
	HistoryF            func(context.Context, []byte) (*schema.StructuredItemList, error)
	UseDatabaseF        func(context.Context, *schema.Database) (*schema.UseDatabaseReply, error)
	DumpF               func(context.Context, io.WriteSeeker) (int64, error)
	CurrentRootF        func(context.Context) (*schema.Root, error)
	ByIndexF            func(context.Context, uint64) (*schema.StructuredItem, error)
	GetF                func(context.Context, []byte) (*schema.StructuredItem, error)
	RawSafeGetF         func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error)
	RawBySafeIndexF     func(context.Context, uint64) (*client.VerifiedItem, error)
	ListUsersF          func(context.Context) (*schema.UserList, error)
	SetActiveUserF      func(context.Context, *schema.SetActiveUserRequest) (*empty.Empty, error)
	ChangePermissionF   func(context.Context, *schema.ChangePermissionRequest) (*schema.Error, error)
}

// GetOptions ...
func (icm *ImmuClientMock) GetOptions() *client.Options {
	return icm.GetOptionsF()
}

// IsConnected ...
func (icm *ImmuClientMock) IsConnected() bool {
	return icm.IsConnectedF()
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

// SafeGet ...
func (icm *ImmuClientMock) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*client.VerifiedItem, error) {
	return icm.SafeGetF(ctx, key)
}

// SafeSet ...
func (icm *ImmuClientMock) SafeSet(ctx context.Context, key []byte, value []byte) (*client.VerifiedIndex, error) {
	return icm.SafeSetF(ctx, key, value)
}

// Set ...
func (icm *ImmuClientMock) Set(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
	return icm.SetF(ctx, key, value)
}

// SafeReference ...
func (icm *ImmuClientMock) SafeReference(ctx context.Context, reference []byte, key []byte) (*client.VerifiedIndex, error) {
	return icm.SafeReferenceF(ctx, reference, key)
}

// SafeZAdd ...
func (icm *ImmuClientMock) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*client.VerifiedIndex, error) {
	return icm.SafeZAddF(ctx, set, score, key)
}

// History ...
func (icm *ImmuClientMock) History(ctx context.Context, key []byte) (*schema.StructuredItemList, error) {
	return icm.HistoryF(ctx, key)
}

// UseDatabase ...
func (icm *ImmuClientMock) UseDatabase(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
	return icm.UseDatabaseF(ctx, d)
}

// Dump ...
func (icm *ImmuClientMock) Dump(ctx context.Context, writer io.WriteSeeker) (int64, error) {
	return icm.DumpF(ctx, writer)
}

// CurrentRoot ...
func (icm *ImmuClientMock) CurrentRoot(ctx context.Context) (*schema.Root, error) {
	return icm.CurrentRootF(ctx)
}

// ByIndex ...
func (icm *ImmuClientMock) ByIndex(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
	return icm.ByIndexF(ctx, index)
}

// Get ...
func (icm *ImmuClientMock) Get(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
	return icm.GetF(ctx, key)
}

// RawSafeGet ...
func (icm *ImmuClientMock) RawSafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
	return icm.RawSafeGetF(ctx, key, opts...)
}

// RawBySafeIndex ...
func (icm *ImmuClientMock) RawBySafeIndex(ctx context.Context, index uint64) (*client.VerifiedItem, error) {
	return icm.RawBySafeIndexF(ctx, index)
}

// ListUsers ...
func (icm *ImmuClientMock) ListUsers(ctx context.Context) (*schema.UserList, error) {
	return icm.ListUsersF(ctx)
}

// SetActiveUser ...
func (icm *ImmuClientMock) SetActiveUser(ctx context.Context, u *schema.SetActiveUserRequest) (*empty.Empty, error) {
	return icm.SetActiveUserF(ctx, u)
}

// ChangePermission ...
func (icm *ImmuClientMock) ChangePermission(ctx context.Context, r *schema.ChangePermissionRequest) (*schema.Error, error) {
	return icm.ChangePermissionF(ctx, r)
}
