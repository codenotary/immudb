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
package server

import (
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

func TestDummyClosedDatabase(t *testing.T) {
	cdb := &closedDB{name: "closeddb1", opts: database.DefaultOption()}

	require.Equal(t, cdb.name, cdb.GetName())
	require.Equal(t, cdb.opts, cdb.GetOptions())

	require.False(t, cdb.IsReplica())

	require.Equal(t, 1000, cdb.MaxResultSize())

	err := cdb.UseTimeFunc(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	waitingCount, _ := cdb.Health()
	require.Equal(t, 0, waitingCount)

	_, err = cdb.CurrentState()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Size()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Set(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSet(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Get(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableGet(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.GetAll(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Delete(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SetReference(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSetReference(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Scan(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.History(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ExecAll(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Count(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.CountAll()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ZAdd(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableZAdd(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ZScan(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.NewSQLTx(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, _, err = cdb.SQLExec(nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, _, err = cdb.SQLExecPrepared(nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.InferParameters("", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.InferParametersPrepared(nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQuery(nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQueryPrepared(nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQueryRowReader(nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSQLGet(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ListTables(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.DescribeTable("", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.WaitForTx(0, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.WaitForIndexingUpto(0, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.TxByID(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ExportTxByID(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ReplicateTx(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableTxByID(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.TxScan(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.FlushIndex(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.CompactIndex()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	require.True(t, cdb.IsClosed())

	err = cdb.Close()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)
}
