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
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/golang/protobuf/ptypes/empty"
)

// BeginTx creates a new transaction. Only one read-write transaction per session can be active at a time.
func (s *ImmuServer) BeginTx(ctx context.Context, request *schema.BeginTxRequest) (*schema.BeginTxResponse, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	sessionID, err := sessions.GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := s.SessManager.GetSession(sessionID)
	if err != nil {
		return nil, err
	}

	db, err := s.getDBFromCtx(ctx, "SQLExec")
	if err != nil {
		return nil, err
	}

	SQLTx, _, err := db.SQLExec(&schema.SQLExecRequest{Sql: "BEGIN TRANSACTION;"}, nil)
	if err != nil {
		return nil, err
	}

	tx, err := sess.NewTransaction(SQLTx, request.Mode, db)
	if err != nil {
		return nil, err
	}

	return &schema.BeginTxResponse{TransactionID: tx.GetID()}, nil
}

func (s *ImmuServer) Commit(ctx context.Context, e *empty.Empty) (*schema.CommittedSQLTx, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	db := tx.GetDB()

	_, ctxs, err := db.SQLExec(&schema.SQLExecRequest{Sql: "COMMIT;"}, tx.GetSQLTx())
	if err != nil {
		return nil, err
	}

	commitedTx := ctxs[0]
	lastPKs := make(map[string]*schema.SQLValue, len(commitedTx.LastInsertedPKs()))

	for k, n := range commitedTx.LastInsertedPKs() {
		lastPKs[k] = &schema.SQLValue{Value: &schema.SQLValue_N{N: n}}
	}

	err = s.SessManager.DeleteTransaction(tx)
	if err != nil {
		return nil, err
	}

	return &schema.CommittedSQLTx{
		Header:          schema.TxHeaderToProto(commitedTx.TxHeader()),
		UpdatedRows:     uint32(commitedTx.UpdatedRows()),
		LastInsertedPKs: lastPKs,
	}, nil
}

func (s *ImmuServer) Rollback(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	db := tx.GetDB()

	_, _, err = db.SQLExec(&schema.SQLExecRequest{Sql: "ROLLBACK;"}, tx.GetSQLTx())
	if err != nil {
		return nil, err
	}

	err = s.SessManager.DeleteTransaction(tx)
	if err != nil {
		return nil, err
	}

	return new(empty.Empty), nil
}

func (s *ImmuServer) TxSQLExec(ctx context.Context, request *schema.SQLExecRequest) (*empty.Empty, error) {
	if s.Options.GetMaintenance() {
		return new(empty.Empty), ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return new(empty.Empty), err
	}

	if tx.GetMode() != schema.TxMode_READ_WRITE {
		return new(empty.Empty), ErrReadWriteTxNotOngoing
	}

	db := tx.GetDB()

	ntx, _, err := db.SQLExec(request, tx.GetSQLTx())
	if err != nil {
		return new(empty.Empty), err
	}

	tx.SetSQLTx(ntx)

	return new(empty.Empty), nil
}

func (s *ImmuServer) TxSQLQuery(ctx context.Context, request *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	db := tx.GetDB()

	return db.SQLQuery(request, tx.GetSQLTx())
}
