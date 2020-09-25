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

package rootservice

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestRootService(t *testing.T) {

	ic := &immuServiceClientMock{}

	cache := &cacheMock{}

	logger := &mockLogger{}

	immudbRootProvider := NewImmudbRootProvider(ic)
	immudbUuidProvider := NewImmudbUuidProvider(ic)
	rs := NewRootService(cache, logger, immudbRootProvider, immudbUuidProvider)

	root, err := rs.GetRoot(context.TODO(), "uuid")
	assert.Nil(t, err)
	assert.IsType(t, &schema.Root{}, root)

	err = rs.SetRoot(&schema.Root{}, "uuid")
	assert.Nil(t, err)
}

type cacheMock struct{}

func (m *cacheMock) Get(serverUuid string, databasename string) (*schema.Root, error) {
	return nil, nil
}
func (m *cacheMock) Set(root *schema.Root, serverUuid string, databasename string) error {
	return nil
}

type mockLogger struct{}

func (l *mockLogger) Errorf(f string, v ...interface{}) {}

func (l *mockLogger) Warningf(f string, v ...interface{}) {}

func (l *mockLogger) Infof(f string, v ...interface{}) {}

func (l *mockLogger) Debugf(f string, v ...interface{}) {}

func (l *mockLogger) CloneWithLevel(level logger.LogLevel) logger.Logger { return l }

type immuServiceClientMock struct{}

func (m *immuServiceClientMock) ListUsers(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.UserList, error) {
	return &schema.UserList{}, nil
}
func (m *immuServiceClientMock) GetUser(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) error {
	return nil
}
func (m *immuServiceClientMock) CreateUser(ctx context.Context, in *schema.CreateUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) ChangePassword(ctx context.Context, in *schema.ChangePasswordRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) SetPermission(ctx context.Context, in *schema.Item, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) DeactivateUser(ctx context.Context, in *schema.UserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) UpdateAuthConfig(ctx context.Context, in *schema.AuthConfig, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) UpdateMTLSConfig(ctx context.Context, in *schema.MTLSConfig, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) PrintTree(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Tree, error) {
	return &schema.Tree{}, nil
}
func (m *immuServiceClientMock) Login(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
	return &schema.LoginResponse{}, nil
}
func (m *immuServiceClientMock) Logout(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) Set(ctx context.Context, in *schema.KeyValue, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) SetSV(ctx context.Context, in *schema.StructuredKeyValue, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) SafeSet(ctx context.Context, in *schema.SafeSetOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return &schema.Proof{}, nil
}
func (m *immuServiceClientMock) SafeSetSV(ctx context.Context, in *schema.SafeSetSVOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return &schema.Proof{}, nil
}
func (m *immuServiceClientMock) Get(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.Item, error) {
	return &schema.Item{}, nil
}
func (m *immuServiceClientMock) GetSV(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.StructuredItem, error) {
	return &schema.StructuredItem{}, nil
}
func (m *immuServiceClientMock) SafeGet(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeItem, error) {
	return &schema.SafeItem{}, nil
}
func (m *immuServiceClientMock) SafeGetSV(ctx context.Context, in *schema.SafeGetOptions, opts ...grpc.CallOption) (*schema.SafeStructuredItem, error) {
	return &schema.SafeStructuredItem{}, nil
}
func (m *immuServiceClientMock) SetBatch(ctx context.Context, in *schema.KVList, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) SetBatchSV(ctx context.Context, in *schema.SKVList, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) GetBatch(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return &schema.ItemList{}, nil
}
func (m *immuServiceClientMock) GetBatchSV(ctx context.Context, in *schema.KeyList, opts ...grpc.CallOption) (*schema.StructuredItemList, error) {
	return &schema.StructuredItemList{}, nil
}
func (m *immuServiceClientMock) Scan(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return &schema.ItemList{}, nil
}
func (m *immuServiceClientMock) ScanSV(ctx context.Context, in *schema.ScanOptions, opts ...grpc.CallOption) (*schema.StructuredItemList, error) {
	return &schema.StructuredItemList{}, nil
}
func (m *immuServiceClientMock) Count(ctx context.Context, in *schema.KeyPrefix, opts ...grpc.CallOption) (*schema.ItemsCount, error) {
	return &schema.ItemsCount{}, nil
}
func (m *immuServiceClientMock) CurrentRoot(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error) {
	return &schema.Root{}, nil
}
func (m *immuServiceClientMock) Inclusion(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.InclusionProof, error) {
	return &schema.InclusionProof{}, nil
}
func (m *immuServiceClientMock) Consistency(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.ConsistencyProof, error) {
	return &schema.ConsistencyProof{}, nil
}
func (m *immuServiceClientMock) ByIndex(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.Item, error) {
	return &schema.Item{}, nil
}
func (m *immuServiceClientMock) BySafeIndex(ctx context.Context, in *schema.SafeIndexOptions, opts ...grpc.CallOption) (*schema.SafeItem, error) {
	return &schema.SafeItem{}, nil
}
func (m *immuServiceClientMock) ByIndexSV(ctx context.Context, in *schema.Index, opts ...grpc.CallOption) (*schema.StructuredItem, error) {
	return &schema.StructuredItem{}, nil
}
func (m *immuServiceClientMock) History(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return &schema.ItemList{}, nil
}
func (m *immuServiceClientMock) HistorySV(ctx context.Context, in *schema.Key, opts ...grpc.CallOption) (*schema.StructuredItemList, error) {
	return &schema.StructuredItemList{}, nil
}
func (m *immuServiceClientMock) Health(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{}, nil
}
func (m *immuServiceClientMock) Reference(ctx context.Context, in *schema.ReferenceOptions, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) SafeReference(ctx context.Context, in *schema.SafeReferenceOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return &schema.Proof{}, nil
}
func (m *immuServiceClientMock) ZAdd(ctx context.Context, in *schema.ZAddOptions, opts ...grpc.CallOption) (*schema.Index, error) {
	return &schema.Index{}, nil
}
func (m *immuServiceClientMock) ZScan(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.ItemList, error) {
	return &schema.ItemList{}, nil
}
func (m *immuServiceClientMock) ZScanSV(ctx context.Context, in *schema.ZScanOptions, opts ...grpc.CallOption) (*schema.StructuredItemList, error) {
	return &schema.StructuredItemList{}, nil
}
func (m *immuServiceClientMock) SafeZAdd(ctx context.Context, in *schema.SafeZAddOptions, opts ...grpc.CallOption) (*schema.Proof, error) {
	return &schema.Proof{}, nil
}
func (m *immuServiceClientMock) IScan(ctx context.Context, in *schema.IScanOptions, opts ...grpc.CallOption) (*schema.Page, error) {
	return &schema.Page{}, nil
}
func (m *immuServiceClientMock) IScanSV(ctx context.Context, in *schema.IScanOptions, opts ...grpc.CallOption) (*schema.SPage, error) {
	return &schema.SPage{}, nil
}
func (m *immuServiceClientMock) Dump(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (schema.ImmuService_DumpClient, error) {
	return nil, nil
}
func (m *immuServiceClientMock) CreateDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) UseDatabase(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
	return &schema.UseDatabaseReply{}, nil
}
func (m *immuServiceClientMock) ChangePermission(ctx context.Context, in *schema.ChangePermissionRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) SetActiveUser(ctx context.Context, in *schema.SetActiveUserRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
func (m *immuServiceClientMock) DatabaseList(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
	return &schema.DatabaseListResponse{}, nil
}
