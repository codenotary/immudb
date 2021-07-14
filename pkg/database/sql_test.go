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

	_, err := db.SQLExecPrepared(nil, nil, false)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLExec(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLExec(&schema.SQLExecRequest{Sql: "invalid sql statement"})
	require.Error(t, err)

	_, err = db.SQLExec(&schema.SQLExecRequest{Sql: "CREATE DATABASE db1"})
	require.Error(t, err)

	_, err = db.SQLExec(&schema.SQLExecRequest{Sql: "USE DATABASE db1"})
	require.Error(t, err)

	md, err := db.SQLExec(&schema.SQLExecRequest{Sql: `
		CREATE TABLE table1(id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)
	`})
	require.NoError(t, err)
	require.Len(t, md.Ctxs, 1)
	require.Len(t, md.Dtxs, 0)

	res, err := db.ListTables()
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	_, err = db.DescribeTable("table2")
	require.Equal(t, sql.ErrTableDoesNotExist, err)

	res, err = db.DescribeTable("table1")
	require.NoError(t, err)
	require.Len(t, res.Rows, 4)

	md, err = db.SQLExec(&schema.SQLExecRequest{Sql: `
		INSERT INTO table1(id, title, active, payload) VALUES (1, 'title1', null, null), (2, 'title2', true, null), (3, 'title3', false, x'AADD')
	`})
	require.NoError(t, err)
	require.Len(t, md.Ctxs, 0)
	require.Len(t, md.Dtxs, 1)

	params := make([]*schema.NamedParam, 1)
	params[0] = &schema.NamedParam{Name: "active", Value: &schema.SQLValue{Value: &schema.SQLValue_B{B: true}}}

	err = db.UseSnapshot(nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = db.UseSnapshot(&schema.UseSnapshotRequest{SinceTx: 0})
	require.NoError(t, err)

	_, err = db.SQLQueryPrepared(nil, nil, false)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLQuery(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: "invalid sql statement"})
	require.Error(t, err)

	_, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: "CREATE INDEX ON table1(title)"})
	require.Equal(t, ErrIllegalArguments, err)

	q := "SELECT t.id, t.id as id2, title, active, payload FROM (table1 as t) WHERE id <= 3 AND active != @active"
	res, err = db.SQLQuery(&schema.SQLQueryRequest{Sql: q, Params: params})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	inferredParams, err := db.InferParameters(q)
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	stmts, err := sql.ParseString(q)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	inferredParams, err = db.InferParametersPrepared(stmts[0])
	require.NoError(t, err)
	require.Len(t, inferredParams, 1)
	require.Equal(t, sql.BooleanType, inferredParams["active"])

	_, err = db.VerifiableSQLGet(nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValue: &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}}},
		ProveSinceTx:  4,
	})
	require.Equal(t, store.ErrIllegalState, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table2", PkValue: &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}}},
		ProveSinceTx:  0,
	})
	require.Equal(t, sql.ErrTableDoesNotExist, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValue: &schema.SQLValue{Value: &schema.SQLValue_B{B: true}}},
		ProveSinceTx:  0,
	})
	require.Equal(t, sql.ErrInvalidValue, err)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValue: &schema.SQLValue{Value: &schema.SQLValue_N{N: 4}}},
		ProveSinceTx:  0,
	})
	require.Equal(t, store.ErrKeyNotFound, err)

	ve, err := db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValue: &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}}},
		ProveSinceTx:  0,
	})
	require.NoError(t, err)
	require.NotNil(t, ve)

	_, err = db.VerifiableSQLGet(&schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: "table1", PkValue: &schema.SQLValue{Value: &schema.SQLValue_N{N: 4}}},
		ProveSinceTx:  0,
	})
	require.Equal(t, store.ErrKeyNotFound, err)

}
