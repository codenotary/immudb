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

package transactions

import (
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"sync"
)

type transaction struct {
	mutex         sync.RWMutex
	transactionID string
	sqlTx         *sql.SQLTx
	txMode        schema.TxMode
	db            database.DB
	sessionID     string
}

type Transaction interface {
	GetID() string
	GetMode() schema.TxMode
	Rollback() error
	Commit() ([]*sql.SQLTx, error)
	GetSessionID() string
	SQLExec(request *schema.SQLExecRequest) error
	SQLQuery(request *schema.SQLQueryRequest) (*schema.SQLQueryResult, error)
}

func NewTransaction(sqlTx *sql.SQLTx, transactionID string, mode schema.TxMode, db database.DB, sessionID string) *transaction {
	return &transaction{
		sqlTx:         sqlTx,
		transactionID: transactionID,
		txMode:        mode,
		db:            db,
		sessionID:     sessionID,
	}
}

func (tx *transaction) GetID() string {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	return tx.transactionID
}

func (tx *transaction) GetMode() schema.TxMode {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	return tx.txMode
}

func (tx *transaction) Rollback() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	// here could happen that a committed transaction is rolled back by the sessions guard. This check prevent a panic
	if tx.sqlTx == nil {
		return nil
	}
	defer func() { tx.sqlTx = nil }()
	return tx.sqlTx.Cancel()
}
func (tx *transaction) Commit() ([]*sql.SQLTx, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	defer func() { tx.sqlTx = nil }()
	_, cTxs, err := tx.db.SQLExec(&schema.SQLExecRequest{Sql: "COMMIT;"}, tx.sqlTx)
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

func (tx *transaction) SQLExec(request *schema.SQLExecRequest) (err error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	tx.sqlTx, _, err = tx.db.SQLExec(request, tx.sqlTx)
	return err
}

func (tx *transaction) SQLQuery(request *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()
	return tx.db.SQLQuery(request, tx.sqlTx)
}
