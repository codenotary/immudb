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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// ImmuServiceClientMock ...
type ImmuServiceClientMock struct {
	schema.ImmuServiceClient

	ListUsersF        func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.UserList, error)
	GetUserF          func(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) error
	CreateUserF       func(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	ChangePasswordF   func(ctx context.Context, in *schema.ChangePasswordRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	SetPermissionF    func(ctx context.Context, in *schema.Item, opts ...grpc.CallOption) (*empty.Empty, error)
	DeactivateUserF   func(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	UpdateAuthConfigF func(ctx context.Context, in *schema.AuthConfig, opts ...grpc.CallOption) (*empty.Empty, error)
	UpdateMTLSConfigF func(ctx context.Context, in *schema.MTLSConfig, opts ...grpc.CallOption) (*empty.Empty, error)
	PrintTreeF        func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Tree, error)
	LoginF            func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error)
	LogoutF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error)
	SetF              func(ctx context.Context, in *schema.KeyValue, opts ...grpc.CallOption) (*schema.Index, error)
	SetSVF            func(ctx context.Context, in *schema.StructuredKeyValue, opts ...grpc.CallOption) (*schema.Index, error)
	SafeSetF          func(ctx context.Context, in *schema.SafeSetOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	SafeSetSVF        func(ctx context.Context, in *schema.SafeSetSVOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	GetF              func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.Item, error)
	GetSVF            func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.StructuredItem, error)
	SafeGetF          func(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeItem, error)
	SafeGetSVF        func(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeStructuredItem, error)
	SetBatchF         func(ctx context.Context, in *schema.KVList, opts ...grpc.CallOption) (*schema.Index, error)
	SetBatchSVF       func(ctx context.Context, in *schema.SKVList, opts ...grpc.CallOption) (*schema.Index, error)
	GetBatchF         func(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.ItemList, error)
	GetBatchSVF       func(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.StructuredItemList, error)
	ScanF             func(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error)
	ScanSVF           func(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.StructuredItemList, error)
	CountF            func(ctx context.Context, in *schema.KeyPrefix, opts ...grpc.CallOption) (*schema.ItemsCount, error)
	CurrentRootF      func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error)
	InclusionF        func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.InclusionProof, error)
	ConsistencyF      func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.ConsistencyProof, error)
	ByIndexF          func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.Item, error)
	BySafeIndexF      func(ctx context.Context, in *schema.SafeIndexOptions, opts ...grpc.CallOption) (*schema.SafeItem, error)
	ByIndexSVF        func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.StructuredItem, error)
	HistoryF          func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.ItemList, error)
	HistorySVF        func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.StructuredItemList, error)
	HealthF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error)
	ReferenceF        func(ctx context.Context, in *schema.ReferenceOptions, opts ...grpc.CallOption) (*schema.Index, error)
	SafeReferenceF    func(ctx context.Context, in *schema.SafeReferenceOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	ZAddF             func(ctx context.Context, in *schema.ZAddOptions, opts ...grpc.CallOption) (*schema.Index, error)
	ZScanF            func(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error)
	ZScanSVF          func(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.StructuredItemList, error)
	SafeZAddF         func(ctx context.Context, in *schema.SafeZAddOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	IScanF            func(ctx context.Context, in *schema.IScanOptions, opts ...grpc.CallOption) (*schema.Page, error)
	IScanSVF          func(ctx context.Context, in *schema.IScanOptions, opts ...grpc.CallOption) (*schema.SPage, error)
	DumpF             func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (schema.ImmuService_DumpClient, error)
	CreateDatabaseF   func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error)
	UseDatabaseF      func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error)
	ChangePermissionF func(ctx context.Context, in *schema.ChangePermissionRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	SetActiveUserF    func(ctx context.Context, in *schema.SetActiveUserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	DatabaseListF     func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error)
}

func (iscm ImmuServiceClientMock) CurrentRoot(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error) {
	return iscm.CurrentRootF(ctx, in, opts...)
}

func (iscm ImmuServiceClientMock) Login(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
	return iscm.LoginF(ctx, in, opts...)
}

func (iscm ImmuServiceClientMock) Health(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
	return iscm.HealthF(ctx, in, opts...)
}

func (iscm ImmuServiceClientMock) DatabaseList(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
	return iscm.DatabaseListF(ctx, in, opts...)
}

func (iscm ImmuServiceClientMock) UseDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
	return iscm.UseDatabaseF(ctx, in, opts...)
}

func (iscm ImmuServiceClientMock) Logout(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return iscm.LogoutF(ctx, in, opts...)
}
