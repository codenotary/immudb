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
)

type dbTx struct {
	*Conn
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.immuClient.IsConnected() {
		return nil, driver.ErrBadConn
	}

	tx, err := c.immuClient.NewTx(ctx)
	if err != nil {
		return nil, err
	}

	c.tx = tx

	return &dbTx{c}, nil
}

func (dbTx *dbTx) Commit() error {
	_, err := dbTx.tx.Commit(context.Background())
	dbTx.tx = nil
	return err
}

func (dbTx *dbTx) Rollback() error {
	err := dbTx.tx.Rollback(context.Background())
	dbTx.tx = nil
	return err
}
