/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestSQLExecAndQuery(t *testing.T) {
	db := makeDb(t)

	db.maxResultSize = 2

	_, _, err := db.SQLExecPrepared(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = db.SQLExec(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "invalid sql statement"})
	require.ErrorContains(t, err, "syntax error")

	_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "CREATE DATABASE db1"})
	require.ErrorIs(t, err, sql.ErrNoSupported)

	_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "USE DATABASE db1"})
	require.ErrorIs(t, err, sql.ErrNoSupported)

	ntx, ctxs, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: `
		CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)
	`})
	require.NoError(t, err)
	require.Nil(t, ntx)
	require.Len(t, ctxs, 1)

	res, err := db.ListTables(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	_, err = db.DescribeTable(context.Background(), nil, "table2")
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	res, err = db.DescribeTable(context.Background(), nil, "table1")
	require.NoError(t, err)
	require.Len(t, res.Rows, 4)

	ntx, ctxs, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: `
		INSERT INTO table1(title, active, payload) VALUES ('title1', null, null), ('title2', true, null), ('title3', false, x'AADD'), ('title4', false, x'ABCD')
	`})
	require.NoError(t, err)
	require.Nil(t, ntx)
	require.Len(t, ctxs, 1)

	params := make([]*schema.NamedParam, 1)
	params[0] = &schema.NamedParam{Name: "active", Value: &schema.SQLValue{Value: &schema.SQLValue_B{B: true}}}

	_, err = db.SQLQueryAll(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.SQLQueryAll(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: "invalid sql statement"})
	require.ErrorContains(t, err, "syntax error")

	_, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: "CREATE INDEX ON table1(title)"})
	require.ErrorIs(t, err, sql.ErrExpectingDQLStmt)

	q := "SELECT * FROM table1 LIMIT 1"
	rows, err := db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: q, Params: params})
	require.NoError(t, err)
	require.Len(t, rows, 1)

	q = "SELECT t.id, t.id as id2, title, active, payload FROM table1 t WHERE id <= 4 AND active != @active"
	rows, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: q, Params: params})
	require.ErrorIs(t, err, ErrResultSizeLimitReached)
	require.Len(t, rows, 2)

	rows, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: q, Params: params, AcceptStream: true})
	require.NoError(t, err)
	require.Len(t, rows, 3)

	inferredParams, err := db.InferParameters(context.Background(), nil, q)
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	stmts, err := sql.ParseSQLString(q)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	inferredParams, err = db.InferParametersPrepared(context.Background(), nil, stmts[0])
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	_, err = db.VerifiableSQLGet(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 5,
	})
	require.ErrorIs(t, err, store.ErrIllegalState)

	_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table2",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_B{B: true}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, sql.ErrInvalidValue)

	_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 5}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	ve, err := db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, ve)

	ve, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
			AtTx:     ctxs[0].TxHeader().ID,
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, ve)

	_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 5}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestVerifiableSQLGet(t *testing.T) {
	db := makeDb(t)

	_, _, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "CREATE DATABASE db1"})
	require.ErrorIs(t, err, sql.ErrNoSupported)

	_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "USE DATABASE db1"})
	require.ErrorIs(t, err, sql.ErrNoSupported)

	t.Run("correctly handle verified get when incorrect number of primary key values is given", func(t *testing.T) {
		_, _, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: `
			CREATE TABLE table1(
				pk1 INTEGER,
				pk2 INTEGER,
				PRIMARY KEY (pk1, pk2))
		`})
		require.NoError(t, err)

		_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: `
			INSERT INTO table1(pk1, pk2) VALUES (1,11), (2,22), (3,33)
		`})
		require.NoError(t, err)

		_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{SqlGetRequest: &schema.SQLGetRequest{
			Table: "table1",
			PkValues: []*schema.SQLValue{{
				Value: &schema.SQLValue_N{N: 1},
			}},
		}})
		require.ErrorIs(t, err, ErrIllegalArguments)
		require.Contains(t, err.Error(), "incorrect number of primary key values")

		_, err = db.VerifiableSQLGet(context.Background(), &schema.VerifiableSQLGetRequest{SqlGetRequest: &schema.SQLGetRequest{
			Table: "table1",
			PkValues: []*schema.SQLValue{
				{Value: &schema.SQLValue_N{N: 1}},
				{Value: &schema.SQLValue_N{N: 11}},
				{Value: &schema.SQLValue_N{N: 111}},
			},
		}})
		require.ErrorIs(t, err, ErrIllegalArguments)
		require.Contains(t, err.Error(), "incorrect number of primary key values")
	})
}

// TestSQLExec_InsertThenUpdateSameRow_Issue2092 reproduces the regression
// reported in codenotary/immudb#2092: an INSERT followed by an UPDATE on the
// same row, sent as a single SQLExec call (multi-statement SQL string in one
// gRPC SQLExecRequest), trips store.ErrCannotUpdateKeyTransiency.
//
// The same SQL works fine when split across two SQLExec calls (one tx per
// call). The failure shape matches 35bb7962 ("disjoint keyRef spaces") but
// that fix only covered the indexer-walk path inside OngoingTx.set; the SQL
// engine's per-non-PK-index transient write inside doUpsert reaches a sibling
// site that is still allocating colliding keyRefs on the second statement.
//
// This test goes through the same db.SQLExec entry point that pkg/server uses,
// so passing it confirms the fix at the layer where production hits the bug.
func TestSQLExec_InsertThenUpdateSameRow_Issue2092(t *testing.T) {
	t.Run("plain table no secondary index", func(t *testing.T) {
		db := makeDb(t)

		_, _, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "CREATE TABLE t2 (id INTEGER AUTO_INCREMENT, k1 VARCHAR[64], v VARCHAR[64], PRIMARY KEY id);",
		})
		require.NoError(t, err)

		_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "INSERT INTO t2 (k1, v) VALUES ('a', 'orig'); UPDATE t2 SET v = 'upd' WHERE k1 = 'a';",
		})
		require.NoError(t, err,
			"INSERT + UPDATE on the same row in a single SQLExec call must commit cleanly — see #2092")
	})

	t.Run("table with UNIQUE composite index", func(t *testing.T) {
		db := makeDb(t)

		_, _, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: `CREATE TABLE t (
				id INTEGER AUTO_INCREMENT,
				k1 VARCHAR[64],
				k2 VARCHAR[64],
				v  VARCHAR[64],
				PRIMARY KEY id
			);
			CREATE UNIQUE INDEX ON t(k1, k2);`,
		})
		require.NoError(t, err)

		_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "INSERT INTO t (k1, k2, v) VALUES ('a', 'b', 'orig'); UPDATE t SET v = 'upd' WHERE k1 = 'a' AND k2 = 'b';",
		})
		require.NoError(t, err,
			"INSERT + UPDATE on the same row in a single SQLExec call (UNIQUE composite index variant) must commit cleanly — see #2092")
	})

	// Control: split into two SQLExec calls — must keep working (one tx per
	// call so the per-tx transient state is gone before the UPDATE runs).
	t.Run("control split across two SQLExec calls", func(t *testing.T) {
		db := makeDb(t)

		_, _, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "CREATE TABLE t2 (id INTEGER AUTO_INCREMENT, k1 VARCHAR[64], v VARCHAR[64], PRIMARY KEY id);",
		})
		require.NoError(t, err)

		_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "INSERT INTO t2 (k1, v) VALUES ('a', 'orig');",
		})
		require.NoError(t, err)

		_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{
			Sql: "UPDATE t2 SET v = 'upd' WHERE k1 = 'a';",
		})
		require.NoError(t, err)
	})
}
