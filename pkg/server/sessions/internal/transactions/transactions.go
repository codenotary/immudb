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
	"sync"
)

type transaction struct {
	sync.Mutex
	transactionID string
	sqlTx         *sql.SQLTx
	txMode        schema.TxMode
}

type Transaction interface {
	GetID() string
	GetSQLTx() *sql.SQLTx
	SetSQLTx(tx *sql.SQLTx)
	GetMode() schema.TxMode
	Rollback() error
}

func NewTransaction(sqlTx *sql.SQLTx, transactionID string, mode schema.TxMode) *transaction {
	return &transaction{
		sqlTx:         sqlTx,
		transactionID: transactionID,
		txMode:        mode,
	}
}

func (tx *transaction) GetID() string {
	tx.Lock()
	defer tx.Unlock()
	return tx.transactionID
}

func (tx *transaction) GetSQLTx() *sql.SQLTx {
	tx.Lock()
	defer tx.Unlock()
	return tx.sqlTx
}

func (tx *transaction) GetMode() schema.TxMode {
	tx.Lock()
	defer tx.Unlock()
	return tx.txMode
}

func (tx *transaction) SetSQLTx(sqlTx *sql.SQLTx) {
	tx.Lock()
	defer tx.Unlock()
	tx.sqlTx = sqlTx
}

func (tx *transaction) Rollback() error {
	tx.Lock()
	defer tx.Unlock()
	return tx.sqlTx.Cancel()
}
