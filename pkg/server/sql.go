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

func (s *ImmuServer) UnarySQLQuery(ctx context.Context, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	var sqlRes *schema.SQLQueryResult
	err := s.sqlQuery(ctx, req, func(res *schema.SQLQueryResult) error {
		sqlRes = res
		return nil
	})
	return sqlRes, err
}

func (s *ImmuServer) sqlQuery(ctx context.Context, req *schema.SQLQueryRequest, send func(*schema.SQLQueryResult) error) error {
	db, err := s.getDBFromCtx(ctx, "SQLQuery")
	if err != nil {
		return err
	}

	tx, err := db.NewSQLTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return err
	}
	defer tx.Cancel()

	reader, err := db.SQLQuery(ctx, tx, req)
	if err != nil {
		return err
	}
	defer reader.Close()

	// NOTE: setting batchSize to a value strictly less than db.MaxResultSize() can result in more than one call to srv.Send
	// for transferring less than db.MaxResultSize() rows.
	// As a consequence, clients which are still using the old unary rpc version of SQLQuery will get stuck because
	// they don't know how to handle multiple messages.
	return s.streamRows(ctx, reader, db.MaxResultSize(), send)
}

func (s *ImmuServer) SQLQuery(req *schema.SQLQueryRequest, srv schema.ImmuService_SQLQueryServer) error {
	return s.sqlQuery(srv.Context(), req, srv.Send)
}

func (s *ImmuServer) streamRows(ctx context.Context, reader sql.RowReader, batchSize int, send func(*schema.SQLQueryResult) error) error {
	descriptors, err := reader.Columns(ctx)
	if err != nil {
		return err
	}

	rows := make([]*schema.Row, batchSize)

	cols := descriptorsToProtoColumns(descriptors)

	columnsSent := false
	err = sql.ReadRowsBatch(ctx, reader, batchSize, func(rowBatch []*sql.Row) error {
		res := &schema.SQLQueryResult{
			Rows: sqlRowsToProto(descriptors, rowBatch, rows),
		}

		// columns are only sent within the first message
		if !columnsSent {
			res.Columns = cols
			columnsSent = true
		}
		return send(res)
	})

	if err == nil && !columnsSent {
		return send(&schema.SQLQueryResult{Columns: cols})
	}
	return err
}

func descriptorsToProtoColumns(descriptors []sql.ColDescriptor) []*schema.Column {
	cols := make([]*schema.Column, len(descriptors))
	for i, des := range descriptors {
		cols[i] = &schema.Column{Name: des.Selector(), Type: des.Type}
	}
	return cols
}

func sqlRowsToProto(descriptors []sql.ColDescriptor, rows []*sql.Row, outRows []*schema.Row) []*schema.Row {
	if len(rows) == 0 {
		return nil
	}

	for i, sqlRow := range rows {
		row := &schema.Row{
			Columns: make([]string, len(descriptors)),
			Values:  make([]*schema.SQLValue, len(descriptors)),
		}

		for i := range descriptors {
			row.Columns[i] = descriptors[i].Selector()

			v := sqlRow.ValuesByPosition[i]
			_, isNull := v.(*sql.NullValue)
			if isNull {
				row.Values[i] = &schema.SQLValue{Value: &schema.SQLValue_Null{}}
			} else {
				row.Values[i] = schema.TypedValueToRowValue(v)
			}
		}
		outRows[i] = row
	}
	return outRows[:len(rows)]
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
