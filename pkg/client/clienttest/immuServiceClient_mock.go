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
	SafeSetF          func(ctx context.Context, in *schema.SafeSetOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	GetF              func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.Item, error)
	SafeGetF          func(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeItem, error)
	SetBatchF         func(ctx context.Context, in *schema.KVList, opts ...grpc.CallOption) (*schema.Index, error)
	GetBatchF         func(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.ItemList, error)
	ScanF             func(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error)
	CountF            func(ctx context.Context, in *schema.KeyPrefix, opts ...grpc.CallOption) (*schema.ItemsCount, error)
	CountAllF         func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.ItemsCount, error)
	CurrentRootF      func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error)
	InclusionF        func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.InclusionProof, error)
	ConsistencyF      func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.ConsistencyProof, error)
	ByIndexF          func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.Item, error)
	BySafeIndexF      func(ctx context.Context, in *schema.SafeIndexOptions, opts ...grpc.CallOption) (*schema.SafeItem, error)
	HistoryF          func(ctx context.Context, in *schema.HistoryOptions, opts ...grpc.CallOption) (*schema.ItemList, error)
	HealthF           func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error)
	ReferenceF        func(ctx context.Context, in *schema.ReferenceOptions, opts ...grpc.CallOption) (*schema.Index, error)
	SafeReferenceF    func(ctx context.Context, in *schema.SafeReferenceOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	ZAddF             func(ctx context.Context, in *schema.ZAddOptions, opts ...grpc.CallOption) (*schema.Index, error)
	ZScanF            func(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.ZItemList, error)
	SafeZAddF         func(ctx context.Context, in *schema.SafeZAddOptions, opts ...grpc.CallOption) (*schema.Proof, error)
	IScanF            func(ctx context.Context, in *schema.IScanOptions, opts ...grpc.CallOption) (*schema.Page, error)
	DumpF             func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (schema.ImmuService_DumpClient, error)
	CreateDatabaseF   func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error)
	UseDatabaseF      func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error)
	ChangePermissionF func(ctx context.Context, in *schema.ChangePermissionRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	SetActiveUserF    func(ctx context.Context, in *schema.SetActiveUserRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	DatabaseListF     func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error)
	SetBatchOpsF      func(ctx context.Context, in *schema.BatchOps, opts ...grpc.CallOption) (*schema.Index, error)
}

func (iscm *ImmuServiceClientMock) CurrentRoot(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error) {
	return iscm.CurrentRootF(ctx, in, opts...)
}

func (iscm *ImmuServiceClientMock) Login(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
	return iscm.LoginF(ctx, in, opts...)
}

func (iscm *ImmuServiceClientMock) Health(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
	return iscm.HealthF(ctx, in, opts...)
}

func (iscm *ImmuServiceClientMock) DatabaseList(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
	return iscm.DatabaseListF(ctx, in, opts...)
}

func (iscm *ImmuServiceClientMock) UseDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
	return iscm.UseDatabaseF(ctx, in, opts...)
}

func (iscm *ImmuServiceClientMock) Logout(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return iscm.LogoutF(ctx, in, opts...)
}

// SafeGet ...
func (icm *ImmuServiceClientMock) SafeGet(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeItem, error) {
	return icm.SafeGetF(ctx, in, opts...)
}

// SafeSet ...
func (icm *ImmuServiceClientMock) SafeSet(ctx context.Context, in *schema.SafeSetOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return icm.SafeSetF(ctx, in, opts...)
}

// Set ...
func (icm *ImmuServiceClientMock) Set(ctx context.Context, in *schema.KeyValue, opts ...grpc.CallOption) (*schema.Index, error) {
	return icm.SetF(ctx, in, opts...)
}

// ZAdd ...
func (icm *ImmuServiceClientMock) ZAdd(ctx context.Context, in *schema.ZAddOptions, opts ...grpc.CallOption) (*schema.Index, error) {
	return icm.ZAddF(ctx, in, opts...)
}

// SafeZAdd ...
func (icm *ImmuServiceClientMock) SafeZAdd(ctx context.Context, in *schema.SafeZAddOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return icm.SafeZAddF(ctx, in, opts...)
}

// History ...
func (icm *ImmuServiceClientMock) History(ctx context.Context, in *schema.HistoryOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return icm.HistoryF(ctx, in, opts...)
}

// Get ...
func (icm *ImmuServiceClientMock) Get(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.Item, error) {
	return icm.GetF(ctx, in, opts...)
}

// ZScan ...
func (icm *ImmuServiceClientMock) ZScan(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.ZItemList, error) {
	return icm.ZScanF(ctx, in, opts...)
}

// Scan ...
func (icm *ImmuServiceClientMock) Scan(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return icm.ScanF(ctx, in, opts...)
}

// CreateDatabase ...
func (icm *ImmuServiceClientMock) CreateDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.CreateDatabaseF(ctx, in, opts...)
}

// CreateUser ...
func (icm *ImmuServiceClientMock) CreateUser(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.CreateUserF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) SetBatch(ctx context.Context, in *schema.KVList, opts ...grpc.CallOption) (*schema.Index, error) {
	return icm.SetBatchF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) GetBatch(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return icm.GetBatchF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) SetBatchOps(ctx context.Context, in *schema.BatchOps, opts ...grpc.CallOption) (*schema.Index, error) {
	return icm.SetBatchOpsF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Inclusion(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.InclusionProof, error) {
	return icm.InclusionF(ctx, in, opts...)
}

func (icm *ImmuServiceClientMock) Consistency(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.ConsistencyProof, error) {
	return icm.ConsistencyF(ctx, in, opts...)
}
func (icm *ImmuServiceClientMock) ByIndex(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.Item, error) {
	return icm.ByIndexF(ctx, in, opts...)
}

func NewImmuServiceClientMock() *ImmuServiceClientMock {
	bs := &ImmuServiceClientMock{
		HealthF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
			return &schema.HealthResponse{}, nil
		},
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{}, nil
		},
		SafeGetF: func(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeItem, error) {
			return &schema.SafeItem{}, nil
		},
		SafeSetF: func(ctx context.Context, in *schema.SafeSetOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
			return &schema.Proof{}, nil
		},
		SetF: func(ctx context.Context, in *schema.KeyValue, opts ...grpc.CallOption) (*schema.Index, error) {
			return &schema.Index{}, nil
		},
		UseDatabaseF: func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
			return &schema.UseDatabaseReply{}, nil
		},
		CreateDatabaseF: func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error) {
			return &empty.Empty{}, nil
		},
		CurrentRootF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error) {
			return &schema.Root{}, nil
		},
		GetF: func(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.Item, error) {
			return &schema.Item{}, nil
		},
		ScanF: func(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
			return &schema.ItemList{}, nil
		},
		HistoryF: func(ctx context.Context, in *schema.HistoryOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
			return &schema.ItemList{}, nil
		},
		ZAddF: func(ctx context.Context, in *schema.ZAddOptions, opts ...grpc.CallOption) (*schema.Index, error) {
			return &schema.Index{}, nil
		},
		SafeZAddF: func(ctx context.Context, in *schema.SafeZAddOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
			return &schema.Proof{}, nil
		},
		ZScanF: func(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.ZItemList, error) {
			return &schema.ZItemList{}, nil
		},
		CreateUserF: func(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
			return &empty.Empty{}, nil
		},
		SetBatchF: func(ctx context.Context, in *schema.KVList, opts ...grpc.CallOption) (*schema.Index, error) {
			return &schema.Index{}, nil
		},
		GetBatchF: func(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.ItemList, error) {
			return &schema.ItemList{}, nil
		},
		SetBatchOpsF: func(ctx context.Context, in *schema.BatchOps, opts ...grpc.CallOption) (*schema.Index, error) {
			return &schema.Index{}, nil
		},
		InclusionF: func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.InclusionProof, error) {
			return &schema.InclusionProof{}, nil
		},
		ConsistencyF: func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.ConsistencyProof, error) {
			return &schema.ConsistencyProof{}, nil
		},
		ByIndexF: func(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.Item, error) {
			return &schema.Item{}, nil
		},
	}
	return bs
}
