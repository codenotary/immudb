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
package sql

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

const sqlPrefix = 2

var prefix = []byte{sqlPrefix}

func TestCreateDatabase(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	_, err = engine.ExecStmt("CREATE DATABASE db2")
	require.NoError(t, err)
}

func TestUseDatabase(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1")
	require.NoError(t, err)

	require.Equal(t, "db1", engine.implicitDatabase)

	_, err = engine.ExecStmt("USE DATABASE db2")
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	require.Equal(t, "db1", engine.implicitDatabase)
}

func TestCreateTable(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)")
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (name STRING, PRIMARY KEY id)")
	require.Equal(t, ErrInvalidPK, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (name STRING, PRIMARY KEY name)")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, PRIMARY KEY id)")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)")
	require.Equal(t, ErrTableAlreadyExists, err)
}

func TestInsertInto(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, PRIMARY KEY id)")
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1)")
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'some title')")
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (2, 'another title')")
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1, 'yat')")
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, id) VALUES (1, 2)")
	require.Equal(t, ErrDuplicatedColumn, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES ('1')")
	require.Equal(t, ErrInvalidValue, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (title) VALUES ('interesting title')")
	require.Equal(t, ErrPKCanNotBeNull, err)
}

func TestQuery(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, PRIMARY KEY id)")
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title) VALUES (%d, 'title%d')", i, i))
		require.NoError(t, err)
	}

	time.Sleep(10 * time.Millisecond)

	r, err := engine.QueryStmt("SELECT id, title FROM table1")
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 2)

		require.Equal(t, uint64(i), row.Values["id"])
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values["title"])
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title FROM table1 ORDER BY id DESC")
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 2)

		require.Equal(t, uint64(rowCount-1-i), row.Values["id"])
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values["title"])
	}

	err = r.Close()
	require.NoError(t, err)
}
