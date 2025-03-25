/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server/sessions"
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

	if request.Mode == schema.TxMode_WriteOnly {
		// only in key-value mode, in sql we read catalog and write to it
		return nil, sessions.ErrWriteOnlyTXNotAllowed
	}

	sess, err := s.SessManager.GetSessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	opts := sql.DefaultTxOptions().
		WithReadOnly(request.Mode == schema.TxMode_ReadOnly)

	if request.SnapshotMustIncludeTxID != nil {
		opts.WithSnapshotMustIncludeTxID(func(_ uint64) uint64 {
			return request.SnapshotMustIncludeTxID.GetValue()
		})
	}

	if request.SnapshotRenewalPeriod != nil {
		opts.WithSnapshotRenewalPeriod(time.Duration(request.SnapshotRenewalPeriod.GetValue()) * time.Millisecond)
	}

	opts.UnsafeMVCC = request.UnsafeMVCC

	tx, err := sess.NewTransaction(ctx, opts)
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

	cTxs, err := s.SessManager.CommitTransaction(ctx, tx)
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

	res := tx.SQLExec(ctx, request)

	if tx.IsClosed() {
		s.SessManager.DeleteTransaction(tx)
	}

	return new(empty.Empty), res
}

func (s *ImmuServer) TxSQLQuery(req *schema.SQLQueryRequest, srv schema.ImmuService_TxSQLQueryServer) error {
	if req == nil {
		return ErrIllegalArguments
	}
	if s.Options.GetMaintenance() {
		return ErrNotAllowedInMaintenanceMode
	}

	tx, err := s.SessManager.GetTransactionFromContext(srv.Context())
	if err != nil {
		return err
	}

	reader, err := tx.SQLQuery(srv.Context(), req)
	if err != nil {
		return err
	}
	defer reader.Close()

	return s.streamRows(context.Background(), reader, tx.Database().MaxResultSize(), srv.Send)
}
