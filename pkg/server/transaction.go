/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

// BeginTx creates a new transaction. Only one read-write transaction per session can be active at a time.
func (s *ImmuServer) NewTx(ctx context.Context, request *schema.NewTxRequest) (*schema.NewTxResponse, error) {
	if request == nil {
		return nil, ErrIllegalArguments
	}
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	sess, err := s.SessManager.GetSessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := sess.NewTransaction(ctx, request.Mode)
	if err != nil {
		return nil, err
	}

	return &schema.NewTxResponse{TransactionID: tx.GetID()}, nil
}

func (s *ImmuServer) Commit(ctx context.Context, _ *empty.Empty) (*schema.CommittedSQLTx, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	cTxs, err := s.SessManager.CommitTransaction(tx)
	if err != nil {
		return nil, err
	}

	cTx := cTxs[0]
	lastPKs := make(map[string]*schema.SQLValue, len(cTx.LastInsertedPKs()))
	for k, n := range cTx.LastInsertedPKs() {
		lastPKs[k] = &schema.SQLValue{Value: &schema.SQLValue_N{N: n}}
	}

	return &schema.CommittedSQLTx{
		Header:          schema.TxHeaderToProto(cTx.TxHeader()),
		UpdatedRows:     uint32(cTx.UpdatedRows()),
		LastInsertedPKs: lastPKs,
	}, nil
}

func (s *ImmuServer) Rollback(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return new(empty.Empty), s.SessManager.RollbackTransaction(tx)
}

func (s *ImmuServer) TxSQLExec(ctx context.Context, request *schema.SQLExecRequest) (*empty.Empty, error) {
	if request == nil {
		return nil, ErrIllegalArguments
	}

	if s.Options.GetMaintenance() {
		return new(empty.Empty), ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return new(empty.Empty), err
	}

	if tx.GetMode() != schema.TxMode_ReadWrite {
		return new(empty.Empty), ErrReadWriteTxNotOngoing
	}

	res := tx.SQLExec(request)

	if tx.IsClosed() {
		s.SessManager.DeleteTransaction(tx)
	}

	return new(empty.Empty), res
}

func (s *ImmuServer) TxSQLQuery(ctx context.Context, request *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if request == nil {
		return nil, ErrIllegalArguments
	}
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return tx.SQLQuery(request)
}
