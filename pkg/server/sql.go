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
package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
)

func (s *ImmuServer) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "VerifiableSQLGet")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).VerifiableSQLGet(req)
}

func (s *ImmuServer) SQLExec(ctx context.Context, req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "SQLExec")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SQLExec(req)
}

func (s *ImmuServer) UseSnapshot(ctx context.Context, req *schema.UseSnapshotRequest) (*empty.Empty, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "UseSnapshot")
	if err != nil {
		return nil, err
	}

	return new(empty.Empty), s.dbList.GetByIndex(ind).UseSnapshot(req)
}

func (s *ImmuServer) SQLQuery(ctx context.Context, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "SQLQuery")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SQLQuery(req)
}

func (s *ImmuServer) ListTables(ctx context.Context, _ *empty.Empty) (*schema.SQLQueryResult, error) {
	ind, err := s.getDbIndexFromCtx(ctx, "ListTables")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ListTables()
}

func (s *ImmuServer) DescribeTable(ctx context.Context, req *schema.Table) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(ctx, "DescribeTable")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).DescribeTable(req.TableName)
}
