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
	db, err := s.getDBFromCtx(ctx, "VerifiableSQLGet")
	if err != nil {
		return nil, err
	}

	ventry, err := db.VerifiableSQLGet(req)
	if err != nil {
		return nil, err
	}

	if s.StateSigner != nil {
		hdr := schema.TxHeaderFromProto(ventry.VerifiableTx.DualProof.TargetTxHeader)
		alh := hdr.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetName(),
			TxId:   hdr.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		ventry.VerifiableTx.Signature = newState.Signature
	}

	return ventry, nil
}

func (s *ImmuServer) SQLExec(ctx context.Context, req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	if s.Options.GetMaintenance() {
		return nil, ErrNotAllowedInMaintenanceMode
	}

	db, err := s.getDBFromCtx(ctx, "SQLExec")
	if err != nil {
		return nil, err
	}

	tx, ctxs, err := db.SQLExec(req, nil)
	if err != nil {
		return nil, err
	}

	if tx != nil {
		tx.Cancel()
		err = ErrTxNotProperlyClosed
	}

	res := &schema.SQLExecResult{
		Txs:       make([]*schema.CommittedSQLTx, len(ctxs)),
		OngoingTx: tx != nil && !tx.Closed(),
	}

	for i, ctx := range ctxs {
		firstPKs := make(map[string]*schema.SQLValue, len(ctx.FirstInsertedPKs()))
		lastPKs := make(map[string]*schema.SQLValue, len(ctx.LastInsertedPKs()))

		for k, n := range ctx.LastInsertedPKs() {
			lastPKs[k] = &schema.SQLValue{Value: &schema.SQLValue_N{N: n}}
		}
		for k, n := range ctx.FirstInsertedPKs() {
			firstPKs[k] = &schema.SQLValue{Value: &schema.SQLValue_N{N: n}}
		}

		res.Txs[i] = &schema.CommittedSQLTx{
			Header:           schema.TxHeaderToProto(ctx.TxHeader()),
			UpdatedRows:      uint32(ctx.UpdatedRows()),
			LastInsertedPKs:  lastPKs,
			FirstInsertedPKs: firstPKs,
		}
	}

	return res, err
}

func (s *ImmuServer) SQLQuery(ctx context.Context, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	db, err := s.getDBFromCtx(ctx, "SQLQuery")
	if err != nil {
		return nil, err
	}

	return db.SQLQuery(req, nil)
}

func (s *ImmuServer) ListTables(ctx context.Context, _ *empty.Empty) (*schema.SQLQueryResult, error) {
	db, err := s.getDBFromCtx(ctx, "ListTables")
	if err != nil {
		return nil, err
	}

	return db.ListTables(nil)
}

func (s *ImmuServer) DescribeTable(ctx context.Context, req *schema.Table) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(ctx, "DescribeTable")
	if err != nil {
		return nil, err
	}

	return db.DescribeTable(req.TableName, nil)
}
