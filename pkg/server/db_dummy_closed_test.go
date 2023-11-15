/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"context"
	"crypto/sha256"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

func TestDummyClosedDatabase(t *testing.T) {
	cdb := &closedDB{name: "closeddb1", opts: database.DefaultOption()}

	require.Equal(t, "data/closeddb1", cdb.Path())

	cdb.AsReplica(false, false, 0)

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

	_, err = cdb.Set(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSet(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Get(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableGet(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.GetAll(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Delete(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SetReference(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSetReference(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Scan(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.History(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ExecAll(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.Count(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.CountAll(context.Background())
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ZAdd(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableZAdd(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ZScan(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.NewSQLTx(nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, _, err = cdb.SQLExec(context.Background(), nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, _, err = cdb.SQLExecPrepared(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.InferParameters(context.Background(), nil, "")
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.InferParametersPrepared(context.Background(), nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQuery(context.Background(), nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQueryPrepared(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SQLQueryRowReader(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableSQLGet(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ListTables(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.DescribeTable(context.Background(), nil, "")
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.WaitForTx(context.Background(), 0, true)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.WaitForIndexingUpto(context.Background(), 0)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.TxByID(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	require.False(t, cdb.IsSyncReplicationEnabled())

	cdb.SetSyncReplication(true)

	_, _, _, err = cdb.ExportTxByID(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ReplicateTx(context.Background(), nil, false, false)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.AllowCommitUpto(1, sha256.Sum256(nil))
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.DiscardPrecommittedTxsSince(1)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.VerifiableTxByID(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.TxScan(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.FlushIndex(nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.CompactIndex()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)
	require.True(t, cdb.IsClosed())

	err = cdb.Truncate(0)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.CreateCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.GetCollection(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.GetCollections(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.UpdateCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.DeleteCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.AddField(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.RemoveField(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.CreateIndex(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.DeleteIndex(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.InsertDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ReplaceDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.AuditDocument(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.SearchDocuments(context.Background(), nil, 0)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.CountDocuments(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.ProofDocument(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	_, err = cdb.DeleteDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	err = cdb.Close()
	require.ErrorIs(t, err, store.ErrAlreadyClosed)
}
