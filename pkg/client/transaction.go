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

package client

import (
	"context"
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/metadata"
)

// Tx represents an open transaction
//
// Note: Currently this object only supports SQL transactions
type Tx interface {

	// Commit commits a transaction.
	Commit(ctx context.Context) (*schema.CommittedSQLTx, error)

	// Rollback rollbacks a transaction.
	Rollback(ctx context.Context) error

	// SQLExec performs a modifying SQL query within the transaction.
	// Such query does not return SQL result.
	SQLExec(ctx context.Context, sql string, params map[string]interface{}) error

	// SQLQuery performs a query (read-only) operation.
	//
	// Deprecated: use SQLQueryReader instead.
	SQLQuery(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLQueryResult, error)

	// SQLQueryReader submits an SQL query to the server and returns a reader object for efficient retrieval of all rows in the result set.
	SQLQueryReader(ctx context.Context, sql string, params map[string]interface{}) (SQLQueryRowReader, error)
}

type tx struct {
	ic            *immuClient
	transactionID string
}

func (c *tx) Commit(ctx context.Context) (*schema.CommittedSQLTx, error) {
	cmtx, err := c.ic.ServiceClient.Commit(c.populateCtx(ctx), new(empty.Empty))
	return cmtx, errors.FromError(err)
}

func (c *tx) Rollback(ctx context.Context) error {
	_, err := c.ic.ServiceClient.Rollback(c.populateCtx(ctx), new(empty.Empty))
	return errors.FromError(err)
}

func (c *immuClient) NewTx(ctx context.Context, opts ...TxOption) (Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	req := &schema.NewTxRequest{
		Mode: schema.TxMode_ReadWrite,
	}

	for _, opt := range opts {
		err := opt(req)
		if err != nil {
			return nil, err
		}
	}

	r, err := c.ServiceClient.NewTx(ctx, req)
	if err != nil {
		return nil, errors.FromError(err)
	}
	tx := &tx{
		ic:            c,
		transactionID: r.TransactionID,
	}
	return tx, nil
}

func (c *tx) SQLExec(ctx context.Context, sql string, params map[string]interface{}) error {
	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return errors.FromError(err)
	}
	_, err = c.ic.ServiceClient.TxSQLExec(c.populateCtx(ctx), &schema.SQLExecRequest{
		Sql:    sql,
		Params: namedParams,
	})
	return errors.FromError(err)
}

func (c *tx) SQLQuery(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLQueryResult, error) {
	stream, err := c.sqlQuery(ctx, sql, params, false)
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, errors.FromError(err)
	}

	if _, err := stream.Recv(); err != io.EOF {
		return res, errors.FromError(err)
	}
	return res, nil
}

func (c *tx) SQLQueryReader(ctx context.Context, sql string, params map[string]interface{}) (SQLQueryRowReader, error) {
	stream, err := c.sqlQuery(ctx, sql, params, true)
	if err != nil {
		return nil, err
	}
	return newSQLQueryRowReader(stream)
}

func (c *tx) sqlQuery(ctx context.Context, sql string, params map[string]interface{}, acceptStream bool) (schema.ImmuService_TxSQLQueryClient, error) {
	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return nil, errors.FromError(err)
	}
	stream, err := c.ic.ServiceClient.TxSQLQuery(c.populateCtx(ctx), &schema.SQLQueryRequest{
		Sql:          sql,
		Params:       namedParams,
		AcceptStream: acceptStream,
	})
	return stream, errors.FromError(err)
}

func (c *tx) GetTransactionID() string {
	return c.transactionID
}

func (tx *tx) populateCtx(ctx context.Context) context.Context {
	if tx.GetTransactionID() != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "transactionid", tx.GetTransactionID())
	}
	return ctx
}
