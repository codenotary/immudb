/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package server

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
)

func (s *ImmuServer) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableSQLGet")
	if err != nil {
		return nil, err
	}

	ventry, err := db.VerifiableSQLGet(ctx, req)
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

	tx, err := db.NewSQLTx(ctx, sql.DefaultTxOptions())
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	ntx, ctxs, err := db.SQLExec(ctx, tx, req)
	if err != nil {
		return nil, err
	}

	if ntx != nil {
		ntx.Cancel()
		err = ErrTxNotProperlyClosed
	}

	res := &schema.SQLExecResult{
		Txs:       make([]*schema.CommittedSQLTx, len(ctxs)),
		OngoingTx: ntx != nil && !ntx.Closed(),
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

	tx, err := db.NewSQLTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	return db.SQLQuery(ctx, tx, req)
}

func (s *ImmuServer) ListTables(ctx context.Context, _ *empty.Empty) (*schema.SQLQueryResult, error) {
	db, err := s.getDBFromCtx(ctx, "ListTables")
	if err != nil {
		return nil, err
	}

	return db.ListTables(ctx, nil)
}

func (s *ImmuServer) DescribeTable(ctx context.Context, req *schema.Table) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(ctx, "DescribeTable")
	if err != nil {
		return nil, err
	}

	return db.DescribeTable(ctx, nil, req.TableName)
}
