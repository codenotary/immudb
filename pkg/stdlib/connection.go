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

package stdlib

import (
	"context"
	"database/sql/driver"
	"github.com/codenotary/immudb/pkg/client"
)

type Conn struct {
	name    string
	conn    client.ImmuClient
	options *client.Options
	driver  *Driver
}

// Conn returns the underlying client.ImmuClient
func (c *Conn) GetImmuClient() client.ImmuClient {
	return c.conn
}

func (c *Conn) GetDriver() *Driver {
	return c.driver
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) Close() error {
	defer c.GetDriver().UnregisterConnection(c.name)
	return c.conn.Disconnect()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrNotImplemented
}

func (c *Conn) ExecContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Result, error) {
	if !c.conn.IsConnected() {
		return nil, driver.ErrBadConn
	}

	vals, err := namedValuesToSqlMap(argsV)
	if err != nil {
		return nil, err
	}

	execResult, err := c.conn.SQLExec(ctx, query, vals)
	if err != nil {
		return nil, err
	}

	return RowsAffected{execResult}, err
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if !c.conn.IsConnected() {
		return nil, driver.ErrBadConn
	}

	vals, err := namedValuesToSqlMap(argsV)
	if err != nil {
		return nil, err
	}

	queryResult, err := c.conn.SQLQuery(ctx, query, vals, true)
	if err != nil {
		return nil, err
	}

	return &Rows{rows: queryResult.Rows}, nil
}

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// driver.Valuer interface is used instead
	return nil
}

func (c *Conn) ResetSession(ctx context.Context) error {
	if !c.conn.IsConnected() {
		return driver.ErrBadConn
	}
	return ErrNotImplemented
}
