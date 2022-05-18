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
package database

import (
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestSQLExecAndQuery(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.maxResultSize = 2

	_, _, err := db.SQLExecPrepared(nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = db.SQLExec(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = db.SQLExec(&schema.SQLExecRequest{Sql: "invalid sql statement"}, nil)
	require.Error(t, err)

	_, _, err = db.SQLExec(&schema.SQLExecRequest{Sql: "CREATE DATABASE db1"}, nil)
	require.Error(t, err)

	_, _, err = db.SQLExec(&schema.SQLExecRequest{Sql: "USE DATABASE db1"}, nil)
	require.Error(t, err)

	ntx, ctxs, err := db.SQLExec(&schema.SQLExecRequest{Sql: `
		CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)
	`}, nil)
	require.NoError(t, err)
	require.Nil(t, ntx)
	require.Len(t, ctxs, 1)

	res, err := db.ListTables(nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	_, err = db.DescribeTable("table2", nil)
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	res, err = db.DescribeTable("table1", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 4)

	ntx, ctxs, err = db.SQLExec(&schema.SQLExecRequest{Sql: `
		INSERT INTO table1(title, active, payload) VALUES ('title1', null, null), ('title2', true, null), ('title3', false, x'AADD')
	`}, nil)
	require.NoError(t, err)
	require.Nil(t, ntx)
	require.Len(t, ctxs, 1)

	params := make([]*schema.NamedParam, 1)
	params[0] = &schema.NamedParam{Name: "active", Value: &schema.SQLValue{Value: &schema.SQLValue_B{B: true}}}

	_, err = db.SQLQueryPrepared(nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLQuery(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: "invalid sql statement"}, nil)
	require.Error(t, err)

	_, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: "CREATE INDEX ON table1(title)"}, nil)
	require.Equal(t, sql.ErrExpectingDQLStmt, err)

	q := "SELECT * FROM table1 LIMIT 1"
	res, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: q, Params: params}, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	q = "SELECT t.id, t.id as id2, title, active, payload FROM table1 t WHERE id <= 3 AND active != @active"
	res, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: q, Params: params}, nil)
	require.ErrorIs(t, err, ErrResultSizeLimitReached)
	require.Len(t, res.Rows, 2)

	inferredParams, err := db.InferParameters(q, nil)
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	stmts, err := sql.ParseString(q)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	inferredParams, err = db.InferParametersPrepared(stmts[0], nil)
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	_, err = db.VerifiableSQLGet(nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 4,
	})
	require.Equal(t, store.ErrIllegalState, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table2",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_B{B: true}}},
		},
		ProveSinceTx: 0,
	})
	require.ErrorIs(t, err, sql.ErrInvalidValue)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 4}}},
		},
		ProveSinceTx: 0,
	})
	require.Equal(t, store.ErrKeyNotFound, err)

	ve, err := db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, ve)

	ve, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
			AtTx:     ctxs[0].TxHeader().ID,
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, ve)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    "table1",
			PkValues: []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 4}}},
		},
		ProveSinceTx: 0,
	})
	require.Equal(t, store.ErrKeyNotFound, err)

}
