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
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestSQLExecAndQuery(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	md, err := db.SQLExec(&schema.SQLExecRequest{Sql: `
		CREATE DATABASE db1
		USE DATABASE db1
		CREATE TABLE table1(id INTEGER, title STRING, PRIMARY KEY id)
	`})
	require.NoError(t, err)
	require.Len(t, md.Ctxs, 2)
	require.Len(t, md.Dtxs, 0)

	md, err = db.SQLExec(&schema.SQLExecRequest{Sql: `
		UPSERT INTO table1(id, title) VALUES (1, 'title1'), (2, 'title2'), (3, 'title3')
	`})
	require.NoError(t, err)
	require.Len(t, md.Ctxs, 0)
	require.Len(t, md.Dtxs, 1)

	time.Sleep(100 * time.Millisecond)

	//	params := []*schema.NamedParam{&schema.NamedParam{Name: "maxID", Value: 3}}
	var params []*schema.NamedParam

	res, err := db.SQLQuery(&schema.SQLQueryRequest{Sql: "SELECT id, title FROM table1 WHERE id < 3", Params: params, Limit: 10})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
}
