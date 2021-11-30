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

package client

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/golang/protobuf/ptypes/empty"
)

type TxOptions struct {
	TxMode schema.TxMode
}

type Tx interface {
	Commit(ctx context.Context) (*schema.CommittedSQLTx, error)
	Rollback(ctx context.Context) error

	TxSQLExec(ctx context.Context, sql string, params map[string]interface{}) error
	TxSQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error)
}

type tx struct {
	ic            *immuClient
	transactionID string
}

func (c *tx) Commit(ctx context.Context) (*schema.CommittedSQLTx, error) {
	cmtx, err := c.ic.ServiceClient.Commit(ctx, new(empty.Empty))
	return cmtx, errors.FromError(err)
}

func (c *tx) Rollback(ctx context.Context) error {
	_, err := c.ic.ServiceClient.Rollback(ctx, new(empty.Empty))
	return errors.FromError(err)
}

func (c *immuClient) BeginTx(ctx context.Context, options *TxOptions) (Tx, error) {
	if options.TxMode == schema.TxMode_WRITE_ONLY {
		// only in key-value mode, in sql we read catalog and write to it
		return nil, ErrWriteOnlyTXNotAllowed
	}
	r, err := c.ServiceClient.BeginTx(ctx, &schema.BeginTxRequest{
		Mode: options.TxMode,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}
	c.SetTransactionID(r.TransactionID)
	tx := &tx{
		ic:            c,
		transactionID: r.TransactionID,
	}
	return tx, nil
}

func (c *tx) TxSQLExec(ctx context.Context, sql string, params map[string]interface{}) error {
	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return errors.FromError(err)
	}
	_, err = c.ic.ServiceClient.TxSQLExec(ctx, &schema.SQLExecRequest{
		Sql:    sql,
		Params: namedParams,
	})
	return errors.FromError(err)
}

func (c *tx) TxSQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error) {
	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return nil, errors.FromError(err)
	}
	res, err := c.ic.ServiceClient.TxSQLQuery(ctx, &schema.SQLQueryRequest{
		Sql:           sql,
		Params:        namedParams,
		ReuseSnapshot: !renewSnapshot,
	})
	return res, errors.FromError(err)
}

func (c *immuClient) GetTransactionID() string {
	c.RLock()
	defer c.RUnlock()
	return c.TransactionID
}

func (c *immuClient) SetTransactionID(transactionID string) {
	c.Lock()
	defer c.Unlock()
	c.TransactionID = transactionID
}
