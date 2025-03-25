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

package database

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyReplica(t *testing.T) {
	rootPath := t.TempDir()

	options := DefaultOptions().WithDBRootPath(rootPath).AsReplica(true)

	replica, err := NewDB("db", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	err = replica.Close()
	require.NoError(t, err)

	replica, err = OpenDB("db", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	_, err = replica.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte("key1"), Value: []byte("value1")}}})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = replica.ExecAll(context.Background(), &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
		}},
	)
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = replica.SetReference(context.Background(), &schema.ReferenceRequest{
		Key:           []byte("key"),
		ReferencedKey: []byte("refkey"),
	})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = replica.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:   []byte("set"),
		Score: 1,
		Key:   []byte("key"),
	})
	require.ErrorIs(t, err, ErrIsReplica)

	_, _, err = replica.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER, title VARCHAR, PRIMARY KEY id)"})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = replica.SQLQuery(context.Background(), nil, &schema.SQLQueryRequest{Sql: "SELECT * FROM mytable"})
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	_, err = replica.DescribeTable(context.Background(), nil, "mytable")
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	res, err := replica.ListTables(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	_, err = replica.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "mytable",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
	})
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)
}

func TestSwitchToReplica(t *testing.T) {
	rootPath := t.TempDir()

	options := DefaultOptions().WithDBRootPath(rootPath).AsReplica(false)

	replica := makeDbWith(t, "db", options)

	_, _, err := replica.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER, title VARCHAR, PRIMARY KEY id)"})
	require.NoError(t, err)

	_, _, err = replica.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "INSERT INTO mytable(id, title) VALUES (1, 'TITLE1')"})
	require.NoError(t, err)

	replica.AsReplica(true, false, 0)

	state, err := replica.CurrentState()
	require.NoError(t, err)

	err = replica.DiscardPrecommittedTxsSince(state.TxId)
	require.Error(t, err, store.ErrIllegalArguments)

	_, err = replica.ListTables(context.Background(), nil)
	require.NoError(t, err)

	_, err = replica.DescribeTable(context.Background(), nil, "mytable")
	require.NoError(t, err)

	reader, err := replica.SQLQuery(context.Background(), nil, &schema.SQLQueryRequest{Sql: "SELECT * FROM mytable"})
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	_, err = replica.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "mytable",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
	})
	require.NoError(t, err)
}
