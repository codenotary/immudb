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

package transactions

import (
	"context"
	"sync"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/rs/xid"
)

type transaction struct {
	mutex         sync.RWMutex
	transactionID string
	sqlTx         *sql.SQLTx
	db            database.DB
	sessionID     string
}

type Transaction interface {
	GetID() string
	IsClosed() bool
	Rollback() error
	Commit(ctx context.Context) ([]*sql.SQLTx, error)
	GetSessionID() string
	Database() database.DB
	SQLExec(ctx context.Context, request *schema.SQLExecRequest) error
	SQLQuery(ctx context.Context, request *schema.SQLQueryRequest) (sql.RowReader, error)
}

func NewTransaction(ctx context.Context, opts *sql.TxOptions, db database.DB, sessionID string) (*transaction, error) {
	if opts == nil {
		return nil, sql.ErrIllegalArguments
	}

	transactionID := xid.New().String()

	sqlTx, err := db.NewSQLTx(ctx, opts.WithExplicitClose(true))
	if err != nil {
		return nil, err
	}

	return &transaction{
		sqlTx:         sqlTx,
		transactionID: transactionID,
		db:            db,
		sessionID:     sessionID,
	}, nil
}

func (tx *transaction) GetID() string {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	return tx.transactionID
}

func (tx *transaction) IsClosed() bool {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	return tx.sqlTx == nil || tx.sqlTx.Closed()
}

func (tx *transaction) Rollback() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.sqlTx == nil || tx.sqlTx.Closed() {
		return sql.ErrNoOngoingTx
	}

	return tx.sqlTx.Cancel()
}

func (tx *transaction) Commit(ctx context.Context) ([]*sql.SQLTx, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.sqlTx == nil || tx.sqlTx.Closed() {
		return nil, sql.ErrNoOngoingTx
	}

	_, cTxs, err := tx.db.SQLExec(ctx, tx.sqlTx, &schema.SQLExecRequest{Sql: "COMMIT;"})
	if err != nil {
		return nil, err
	}

	return cTxs, nil
}

func (tx *transaction) GetSessionID() string {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	return tx.sessionID
}

func (tx *transaction) SQLExec(ctx context.Context, request *schema.SQLExecRequest) (err error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.sqlTx == nil || tx.sqlTx.Closed() {
		return sql.ErrNoOngoingTx
	}

	tx.sqlTx, _, err = tx.db.SQLExec(ctx, tx.sqlTx, request)

	return err
}

func (tx *transaction) SQLQuery(ctx context.Context, request *schema.SQLQueryRequest) (sql.RowReader, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.sqlTx == nil || tx.sqlTx.Closed() {
		return nil, sql.ErrNoOngoingTx
	}

	return tx.db.SQLQuery(ctx, tx.sqlTx, request)
}

func (tx *transaction) Database() database.DB {
	return tx.db
}
