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

package client

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/metadata"
)

type Tx interface {
	Commit(ctx context.Context) (*schema.CommittedSQLTx, error)
	Rollback(ctx context.Context) error

	SQLExec(ctx context.Context, sql string, params map[string]interface{}) error
	SQLQuery(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLQueryResult, error)
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

func (c *immuClient) NewTx(ctx context.Context) (Tx, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	r, err := c.ServiceClient.NewTx(ctx, &schema.NewTxRequest{
		Mode: schema.TxMode_ReadWrite,
	})
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
	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return nil, errors.FromError(err)
	}
	res, err := c.ic.ServiceClient.TxSQLQuery(c.populateCtx(ctx), &schema.SQLQueryRequest{
		Sql:    sql,
		Params: namedParams,
	})
	return res, errors.FromError(err)
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
