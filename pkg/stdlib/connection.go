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

package stdlib

import (
	"context"
	"database/sql/driver"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

type Conn struct {
	name       string
	immuClient client.ImmuClient
	options    *client.Options
	driver     *Driver
	tx         client.Tx
}

// Conn returns the underlying client.ImmuClient
func (c *Conn) GetImmuClient() client.ImmuClient {
	return c.immuClient
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) Close() error {
	return c.immuClient.CloseSession(context.Background())
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if !c.immuClient.IsConnected() {
		return nil, driver.ErrBadConn
	}

	vals, err := namedValuesToSqlMap(argsV)
	if err != nil {
		return nil, err
	}

	if c.tx != nil {
		err = c.tx.SQLExec(ctx, query, vals)
		if err != nil {
			return nil, err
		}
		return RowsAffected{&schema.SQLExecResult{
			OngoingTx: true,
		}}, nil
	}

	execResult, err := c.immuClient.SQLExec(ctx, query, vals)
	if err != nil {
		return nil, err
	}

	return RowsAffected{execResult}, err
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.immuClient.IsConnected() {
		return nil, driver.ErrBadConn
	}
	queryResult := &schema.SQLQueryResult{}

	vals, err := namedValuesToSqlMap(argsV)
	if err != nil {
		return nil, err
	}

	if c.tx != nil {
		queryResult, err = c.tx.SQLQuery(ctx, query, vals)
		if err != nil {
			return nil, err
		}
		return &Rows{rows: queryResult.Rows, columns: queryResult.Columns}, nil
	}

	queryResult, err = c.immuClient.SQLQuery(ctx, query, vals, true)
	if err != nil {
		return nil, err
	}

	return &Rows{rows: queryResult.Rows, columns: queryResult.Columns}, nil
}

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// driver.Valuer interface is used instead
	return nil
}

func (c *Conn) ResetSession(ctx context.Context) error {
	if !c.immuClient.IsConnected() {
		return driver.ErrBadConn
	}
	return ErrNotImplemented
}
