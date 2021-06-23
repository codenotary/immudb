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
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

const sqlPrefix = 2

var prefix = []byte{sqlPrefix}

func TestCreateDatabase(t *testing.T) {
	catalogStore, err := store.Open("catalog_create_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_create_db")

	dataStore, err := store.Open("sqldata_create_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_db")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db2", nil, true)
	require.NoError(t, err)

	err = engine.CloseSnapshot()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)

	err = engine.CloseSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.RenewSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestUseDatabase(t *testing.T) {
	catalogStore, err := store.Open("catalog_use_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_use_db")

	dataStore, err := store.Open("sqldata_use_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_use_db")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	err = engine.EnsureCatalogReady()
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	db, err := engine.DatabaseInUse()
	require.NoError(t, err)
	require.Equal(t, "db1", db.name)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db2", nil, true)
	require.Equal(t, ErrDatabaseDoesNotExist, err)
}

func TestCreateTable(t *testing.T) {
	catalogStore, err := store.Open("catalog_create_table", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_create_table")

	dataStore, err := store.Open("sqldata_create_table", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_table")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrInvalidPK, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY name)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrTableAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE TABLE IF NOT EXISTS table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)
}

func TestDumpCatalogTo(t *testing.T) {
	catalogStore, err := store.Open("dump_catalog_catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("dump_catalog_catalog")

	dataStore, err := store.Open("dump_catalog_data", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("dump_catalog_data")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	dumpedCatalogStore, err := store.Open("dumped_catalog_catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("dumped_catalog_catalog")

	err = engine.DumpCatalogTo("", "", nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = engine.DumpCatalogTo("db1", "db2", dumpedCatalogStore)
	require.NoError(t, err)

	err = engine.DumpCatalogTo("db2", "db2", dumpedCatalogStore)
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	err = engine.Close()
	require.NoError(t, err)

	err = engine.DumpCatalogTo("db1", "db2", dumpedCatalogStore)
	require.Equal(t, ErrAlreadyClosed, err)

	engine, err = NewEngine(dumpedCatalogStore, dataStore, prefix)
	require.NoError(t, err)

	require.False(t, engine.catalog.ExistDatabase("db1"))
	require.True(t, engine.catalog.ExistDatabase("db2"))

	err = engine.Close()
	require.NoError(t, err)
}

func TestAddColumn(t *testing.T) {
	catalogStore, err := store.Open("catalog_add_column", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_add_column")

	dataStore, err := store.Open("sqldata_add_column", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_add_column")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrInvalidPK, err)

	_, _, err = engine.ExecStmt("ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil, true)
	require.Equal(t, ErrNoSupported, err)
}

func TestCreateIndex(t *testing.T) {
	catalogStore, err := store.Open("catalog_create_index", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_create_index")

	dataStore, err := store.Open("sqldata_create_index", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_index")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, name VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	db := engine.catalog.Databases()[0]

	table, err := db.GetTableByName("table1")
	require.NoError(t, err)

	require.Len(t, table.indexes, 0)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	col, err := table.GetColumnByName("name")
	require.NoError(t, err)

	_, indexed := table.indexes[col.id]
	require.True(t, indexed)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(id)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	col, err = table.GetColumnByName("age")
	require.NoError(t, err)

	_, indexed = table.indexes[col.id]
	require.True(t, indexed)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table2(name)", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	require.Len(t, table.indexes, 2)

	_, _, err = engine.ExecStmt("INSERT INTO table1(id, name, age) VALUES (1, 'name1', 50)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(active)", nil, true)
	require.Equal(t, ErrLimitedIndex, err)
}

func TestUpsertInto(t *testing.T) {
	catalogStore, err := store.Open("catalog_upsert", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_upsert")

	dataStore, err := store.Open("sqldata_upsert", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_upsert")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN NOT NULL, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, true)
	require.Equal(t, ErrNotNullableColumnCannotBeNull, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, age) VALUES (1, 50)", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", nil, true)
	require.Equal(t, ErrMissingParameter, err)

	params := make(map[string]interface{}, 1)
	params["id"] = [4]byte{1, 2, 3, 4}
	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", params, true)
	require.Equal(t, ErrUnsupportedParameter, err)

	params = make(map[string]interface{}, 1)
	params["id"] = []byte{1, 2, 3}
	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", params, true)
	require.Equal(t, ErrInvalidValue, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, @title)", nil, true)
	require.Equal(t, ErrMissingParameter, err)

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, @title)", params, true)
	require.Equal(t, ErrInvalidValue, err)

	_, _, err = engine.ExecStmt("UPSERT INTO Table1 (id, active) VALUES (1, true)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (Id, Title, Active) VALUES (1, 'some title', false)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, active) VALUES (2, 'another title', true)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1, 'yat')", nil, true)
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, id) VALUES (1, 2)", nil, true)
	require.Equal(t, ErrDuplicatedColumn, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES ('1')", nil, true)
	require.Equal(t, ErrInvalidValue, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (NULL)", nil, true)
	require.Equal(t, ErrPKCanNotBeNull, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, active) VALUES (2, NULL, true)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (title) VALUES ('interesting title')", nil, true)
	require.Equal(t, ErrPKCanNotBeNull, err)
}

func TestTransactions(t *testing.T) {
	catalogStore, err := store.Open("catalog_tx", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_tx")

	dataStore, err := store.Open("sqldata_tx", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_tx")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(`CREATE TABLE table1 (
									id INTEGER, 
									title VARCHAR, 
									PRIMARY KEY id
								)`, nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			CREATE INDEX ON table2(title)
		COMMIT
		`, nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, _, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			CREATE TABLE table2 (id INTEGER, title VARCHAR, age INTEGER, PRIMARY KEY id);
			CREATE INDEX ON table2(title);
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			CREATE INDEX ON table2(age);
			INSERT INTO table2 (id, title, age) VALUES (1, 'title1', 40);
		COMMIT
		`, nil, true)
	require.Equal(t, ErrDDLorDMLTxOnly, err)
}

func TestUseSnapshot(t *testing.T) {
	catalogStore, err := store.Open("catalog_snap", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_snap")

	dataStore, err := store.Open("sqldata_snap", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_snap")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE SNAPSHOT SINCE TX 1", nil, true)
	require.Equal(t, ErrNoSupported, err)

	err = engine.UseSnapshot(1, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(0, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(1, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(1, 2)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	err = engine.UseSnapshot(1, 0)
	require.NoError(t, err)

	err = engine.CloseSnapshot()
	require.NoError(t, err)

	err = engine.UseSnapshot(0, 1)
	require.NoError(t, err)

	err = engine.UseSnapshot(1, 1)
	require.NoError(t, err)
}

func TestEncodeRawValue(t *testing.T) {
	_, err := EncodeRawValue(uint64(1), IntegerType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue(true, IntegerType, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = EncodeRawValue(true, BooleanType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue(uint64(1), BooleanType, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = EncodeRawValue("title", VarcharType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue(uint64(1), VarcharType, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = EncodeRawValue([]byte{}, BLOBType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue(nil, BLOBType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue(uint64(1), BLOBType, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = EncodeRawValue(uint64(1), "invalid type", true)
	require.Equal(t, ErrInvalidValue, err)
}

func TestClosing(t *testing.T) {
	catalogStore, err := store.Open("catalog_closing", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_closing")

	dataStore, err := store.Open("sqldata_closing", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_closing")

	_, err = NewEngine(nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.UseDatabase("db1")
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.DatabaseInUse()
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.Snapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.RenewSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.CloseSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestQuery(t *testing.T) {
	catalogStore, err := store.Open("catalog_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_q")

	dataStore, err := store.Open("sqldata_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_q")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.QueryStmt("SELECT * FROM table1", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id FROM db2.table1", nil, true)
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	_, err = engine.QueryStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, ts INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["id"] = 0

	r, err := engine.QueryStmt("SELECT id FROM db1.table1 WHERE id >= @id", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT * FROM db1.table1", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	rowCount := 10

	start := time.Now().UnixNano()

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, ts, title, active, payload) VALUES (%d, NOW(), 'title%d', %v, x'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	_, err = engine.QueryStmt("SELECT DISTINCT id1 FROM table1", nil, true)
	require.Equal(t, ErrNoSupported, err)

	r, err = engine.QueryStmt("SELECT id1 FROM table1", nil, true)
	require.NoError(t, err)

	row, err := r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)
	require.Nil(t, row)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt(fmt.Sprintf("SELECT t1.id AS D, ts, Title, payload, Active FROM (table1 AS T1) WHERE id >= 0 LIMIT %d AS table1", rowCount), nil, true)
	require.NoError(t, err)

	colsBySel, err := r.colsBySelector()
	require.NoError(t, err)
	require.Len(t, colsBySel, 5)

	require.Equal(t, "db1", r.ImplicitDB())
	require.Equal(t, "table1", r.ImplicitTable())

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 5)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 5)
		require.Less(t, uint64(start), row.Values[EncodeSelector("", "db1", "table1", "ts")].Value())
		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "d")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, i%2 == 0, row.Values[EncodeSelector("", "db1", "table1", "active")].Value())

		encPayload := []byte(fmt.Sprintf("blob%d", i))
		require.Equal(t, []byte(encPayload), row.Values[EncodeSelector("", "db1", "table1", "payload")].Value())
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active, payload FROM table1 ORDER BY title", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	r, err = engine.QueryStmt("SELECT Id, Title, Active, payload FROM Table1 ORDER BY Id DESC", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, (rowCount-1-i)%2 == 0, row.Values[EncodeSelector("", "db1", "table1", "active")].Value())

		encPayload := []byte(fmt.Sprintf("blob%d", rowCount-1-i))
		require.Equal(t, []byte(encPayload), row.Values[EncodeSelector("", "db1", "table1", "payload")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id FROM table1 WHERE id", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrInvalidCondition, err)

	err = r.Close()
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["some_param1"] = true

	r, err = engine.QueryStmt("SELECT id FROM table1 WHERE active = @some_param1", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrMissingParameter, err)

	r.SetParameters(params)

	row, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(2), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param"] = true

	encPayloadPrefix := hex.EncodeToString([]byte("blob"))

	r, err = engine.QueryStmt(fmt.Sprintf("SELECT id, title, active FROM table1 WHERE active = @some_param AND title > 'title' AND payload >= x'%s' AND title LIKE 't", encPayloadPrefix), params, true)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i += 2 {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 3)

		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, params["some_param"], row.Values[EncodeSelector("", "db1", "table1", "active")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT * FROM table1 WHERE id = 0", nil, true)
	require.NoError(t, err)

	cols, err = r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.Values, 5)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE id / 0", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrDivisionByZero, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE id + 1/1 > 1 * (1 - 0)", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE id = 0 AND NOT active OR active", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestQueryWithNullables(t *testing.T) {
	catalogStore, err := store.Open("catalog_nullable", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_nullable")

	dataStore, err := store.Open("sqldata_nullable", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_nullable")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, ts INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("INSERT INTO table1 (id, ts, title) VALUES (1, TIME(), 'title1')", nil, true)
	require.Equal(t, ErrNoSupported, err)

	rowCount := 10

	start := time.Now().UnixNano()

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, ts, title) VALUES (%d, NOW(), 'title%d')", i, i), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, ts, title, active FROM table1 WHERE NOT(active != NULL)", nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)
		require.Less(t, uint64(start), row.Values[EncodeSelector("", "db1", "table1", "ts")].Value())
		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, &NullValue{t: BooleanType}, row.Values[EncodeSelector("", "db1", "table1", "active")])
	}

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestOrderBy(t *testing.T) {
	catalogStore, err := store.Open("catalog_orderby", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_orderby")

	dataStore, err := store.Open("sqldata_orderby", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_orderby")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY id, title DESC", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM (SELECT id, title, age FROM table1) ORDER BY id", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM (SELECT id, title, age FROM table1 AS t1) ORDER BY age DESC", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM table2 ORDER BY title", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY amount", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	params := make(map[string]interface{}, 1)
	params["age"] = nil
	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (1, 'title', @age)", params, true)
	require.Equal(t, ErrIndexedColumnCanNotBeNull, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title')", nil, true)
	require.Equal(t, ErrIndexedColumnCanNotBeNull, err)

	rowCount := 1

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = 40 + i

		_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY title", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 3)

		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64(40+i), row.Values[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 3)

		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64(40+i), row.Values[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age DESC", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 3)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64(40-(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestQueryWithRowFiltering(t *testing.T) {
	catalogStore, err := store.Open("catalog_where", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_where")

	dataStore, err := store.Open("sqldata_where", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_where")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, active FROM table1 WHERE false", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE false OR true", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 < 2", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 >= 2", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 = true", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNotComparableValues, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE NOT table1.active", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE table1.id > 4", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title) VALUES (%d, 'title%d')", rowCount, rowCount), nil, true)
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title FROM table1 WHERE active = null AND payload = null", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title FROM table1 WHERE active = null AND payload = null AND active = payload", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNotComparableValues, err)

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestAggregations(t *testing.T) {
	catalogStore, err := store.Open("catalog_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_agg")

	dataStore, err := store.Open("sqldata_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_agg")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	rowCount := 10
	base := 30

	for i := 1; i <= rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i

		_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT COUNT() FROM table1 WHERE id < i", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id FROM table1 WHERE false", nil, true)
	require.NoError(t, err)

	row, err := r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT COUNT(), SUM(age), MIN(title), MAX(age), AVG(age), MIN(active), MAX(active), MIN(payload) FROM table1 WHERE false", nil, true)
	require.NoError(t, err)

	row, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(0), row.Values[EncodeSelector("", "db1", "table1", "col0")].Value())
	require.Equal(t, uint64(0), row.Values[EncodeSelector("", "db1", "table1", "col1")].Value())
	require.Equal(t, "", row.Values[EncodeSelector("", "db1", "table1", "col2")].Value())
	require.Equal(t, uint64(0), row.Values[EncodeSelector("", "db1", "table1", "col3")].Value())
	require.Equal(t, uint64(0), row.Values[EncodeSelector("", "db1", "table1", "col4")].Value())

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT COUNT() AS c, SUM(age), MIN(age), MAX(age), AVG(age) FROM table1 AS t1", nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.Values, 5)

	require.Equal(t, uint64(rowCount), row.Values[EncodeSelector("", "db1", "t1", "c")].Value())

	require.Equal(t, uint64((1+2*base+rowCount)*rowCount/2), row.Values[EncodeSelector("", "db1", "t1", "col1")].Value())

	require.Equal(t, uint64(1+base), row.Values[EncodeSelector("", "db1", "t1", "col2")].Value())

	require.Equal(t, uint64(base+rowCount), row.Values[EncodeSelector("", "db1", "t1", "col3")].Value())

	require.Equal(t, uint64(base+rowCount/2), row.Values[EncodeSelector("", "db1", "t1", "col4")].Value())

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestGroupByHaving(t *testing.T) {
	catalogStore, err := store.Open("catalog_having", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_having")

	dataStore, err := store.Open("sqldata_having", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_having")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(active)", nil, true)
	require.NoError(t, err)

	rowCount := 10
	base := 40

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 4)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i
		params["active"] = i%2 == 0

		_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age, active) VALUES (@id, @title, @age, @active)", params, true)
		require.NoError(t, err)
	}

	_, err = engine.QueryStmt("SELECT active, COUNT(), SUM(age1) FROM table1 WHERE active != null HAVING AVG(age) >= MIN(age)", nil, true)
	require.Equal(t, ErrHavingClauseRequiresGroupClause, err)

	r, err := engine.QueryStmt("SELECT active, COUNT(), SUM(age1) FROM table1 WHERE active != null GROUP BY active HAVING AVG(age) >= MIN(age)", nil, true)
	require.NoError(t, err)

	r.SetParameters(nil)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT active, COUNT(), SUM(age1) FROM table1 WHERE AVG(age) >= MIN(age) GROUP BY active", nil, true)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT active, COUNT(id) FROM table1 GROUP BY active", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrLimitedCount, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT active, COUNT() FROM table1 GROUP BY active HAVING AVG(age) >= MIN(age1)", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT active, COUNT() as c, MIN(age), MAX(age), AVG(age), SUM(age) FROM table1 GROUP BY active HAVING COUNT() <= SUM(age) AND MIN(age) <= MAX(age) AND AVG(age) <= MAX(age) AND MAX(age) < SUM(age) AND AVG(age) >= MIN(age) AND SUM(age) > 0 ORDER BY active DESC", nil, true)
	require.NoError(t, err)

	_, err = r.Columns()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 6)

		require.Equal(t, i == 0, row.Values[EncodeSelector("", "db1", "table1", "active")].Value())

		require.Equal(t, uint64(rowCount/2), row.Values[EncodeSelector("", "db1", "table1", "c")].Value())

		if i%2 == 0 {
			require.Equal(t, uint64(base), row.Values[EncodeSelector("", "db1", "table1", "col2")].Value())
			require.Equal(t, uint64(base+rowCount-2), row.Values[EncodeSelector("", "db1", "table1", "col3")].Value())
		} else {
			require.Equal(t, uint64(base+1), row.Values[EncodeSelector("", "db1", "table1", "col2")].Value())
			require.Equal(t, uint64(base+rowCount-1), row.Values[EncodeSelector("", "db1", "table1", "col3")].Value())
		}
	}

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestJoins(t *testing.T) {
	catalogStore, err := store.Open("catalog_innerjoin", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_innerjoin")

	dataStore, err := store.Open("sqldata_innerjoin", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_innerjoin")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, fkid2 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, amount INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)", i, i, rowCount-1-i, i), nil, true)
		require.NoError(t, err)

		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table2 (id, amount) VALUES (%d, %d)", rowCount-1-i, i*i), nil, true)
		require.NoError(t, err)

		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, table2.amount FROM table1 INNER JOIN table2 ON table1.fkid1 = table1.fkid1", nil, true)
	require.NoError(t, err)

	r.SetParameters(nil)

	_, err = r.Read()
	require.Equal(t, ErrJointColumnNotFound, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, table2.amount, table3.age FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id INNER JOIN table3 ON table1.fkid2 = table3.id WHERE table1.id >= 0 AND table3.age >= 30 ORDER BY id DESC", nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64((rowCount-1-i)*(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table2", "amount")].Value())
		require.Equal(t, uint64(30+(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table3", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)", rowCount, rowCount, rowCount, rowCount), nil, true)
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, table2.amount, table3.age FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id INNER JOIN table3 ON table1.fkid2 = table3.id ORDER BY id DESC", nil, true)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64((rowCount-1-i)*(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table2", "amount")].Value())
		require.Equal(t, uint64(30+(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table3", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestNestedJoins(t *testing.T) {
	catalogStore, err := store.Open("catalog_nestedjoins", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_nestedjoins")

	dataStore, err := store.Open("sqldata_nestedjoins", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_nestedjoins")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, amount INTEGER, fkid1 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1) VALUES (%d, 'title%d', %d)", i, i, rowCount-1-i), nil, true)
		require.NoError(t, err)

		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table2 (id, amount, fkid1) VALUES (%d, %d, %d)", rowCount-1-i, i*i, i), nil, true)
		require.NoError(t, err)

		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, t2.amount AS total_amount, t3.age FROM (table1 AS t1) INNER JOIN (table2 as t2) ON fkid1 = t2.id INNER JOIN (table3 as t3) ON t2.fkid1 = t3.id ORDER BY id DESC", nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "t1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "t1", "title")].Value())
		require.Equal(t, uint64((rowCount-1-i)*(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "t2", "total_amount")].Value())
		require.Equal(t, uint64(30+(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "t3", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestReOpening(t *testing.T) {
	catalogStore, err := store.Open("catalog_reopening", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_reopening")

	dataStore, err := store.Open("sqldata_reopening", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_reopening")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1; CREATE TABLE table1 (id INTEGER, name VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1; CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	engine, err = NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	exists := engine.catalog.ExistDatabase("db1")
	require.True(t, exists)

	db, err := engine.catalog.GetDatabaseByName("db1")
	require.NoError(t, err)

	exists = db.ExistTable("table1")
	require.True(t, exists)

	table, err := db.GetTableByName("table1")
	require.NoError(t, err)

	require.Equal(t, "id", table.pk.colName)

	require.Len(t, table.ColsByID(), 2)

	col, err := table.GetColumnByName("id")
	require.NoError(t, err)
	require.Equal(t, IntegerType, col.colType)

	col, err = table.GetColumnByName("name")
	require.NoError(t, err)
	require.Equal(t, VarcharType, col.colType)

	require.Len(t, table.indexes, 1)

	_, indexed := table.indexes[col.id]
	require.True(t, indexed)

	err = engine.Close()
	require.NoError(t, err)
}

func TestSubQuery(t *testing.T) {
	catalogStore, err := store.Open("catalog_subq", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_subq")

	dataStore, err := store.Open("sqldata_subq", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_subq")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title AS t FROM (SELECT id, title, active FROM table1 AS table2) WHERE active AND table2.id >= 0 AS t2", nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 2)

	for i := 0; i < rowCount; i += 2 {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 2)

		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "t2", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "t2", "t")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (0, 'title0')", nil, true)
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE active", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE title", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrInvalidCondition, err)

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestInferParameters(t *testing.T) {
	catalogStore, err := store.Open("catalog_infer_params", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params")

	dataStore, err := store.Open("catalog_infer_params", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	stmt := "CREATE DATABASE db1"

	params, err := engine.InferParameters(stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.ExecStmt(stmt, nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	params, err = engine.InferParameters("USE DATABASE db1")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("USE SNAPSHOT BEFORE TX 10")
	require.NoError(t, err)
	require.Len(t, params, 0)

	stmt = "CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"

	params, err = engine.InferParameters(stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.ExecStmt(stmt, nil, true)
	require.NoError(t, err)

	params, err = engine.InferParameters("ALTER TABLE mytableSE ADD COLUMN note VARCHAR")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("CREATE INDEX ON mytable(title)")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("BEGIN TRANSACTION INSERT INTO mytable(id, title) VALUES (@id, @title); COMMIT")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, VarcharType, params["title"])

	params, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (1, 'title1')")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (1, 'title1'), (@id2, @title2)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id2"])
	require.Equal(t, VarcharType, params["title2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE (id - 1) > (@id + (@id+1))")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters("SELECT * FROM mytable INNER JOIN mytable ON id = id WHERE id > @id")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > @id AND (NOT @active OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, BooleanType, params["active"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > ? AND (NOT ? OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > $2 AND (NOT $1 OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, IntegerType, params["param2"])

	params, err = engine.InferParameters("SELECT COUNT() FROM mytable GROUP BY active HAVING @param1 = COUNT()")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters("SELECT COUNT(), MIN(id) FROM mytable GROUP BY active HAVING @param1 < MIN(id)")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @active AND title LIKE 't+'")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["active"])

	err = engine.Close()
	require.NoError(t, err)
}

func TestInferParametersPrepared(t *testing.T) {
	catalogStore, err := store.Open("catalog_infer_params_prepared", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_prepared")

	dataStore, err := store.Open("catalog_infer_params_prepared", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_prepared")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	stmts, err := Parse(strings.NewReader("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	params, err := engine.InferParametersPreparedStmt(stmts[0])
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.ExecPreparedStmts(stmts, nil, true)
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
}

func TestInferParametersUnbounded(t *testing.T) {
	catalogStore, err := store.Open("catalog_infer_params_unbounded", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_unbounded")

	dataStore, err := store.Open("catalog_infer_params_unbounded", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_unbounded")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	params, err := engine.InferParameters("SELECT * FROM mytable WHERE @param1 = @param2")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, AnyType, params["param1"])
	require.Equal(t, AnyType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 AND @param2")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, AnyType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NOT NULL")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 AND active)")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 <= mytable.id)")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	err = engine.Close()
	require.NoError(t, err)
}

func TestInferParametersInvalidCases(t *testing.T) {
	catalogStore, err := store.Open("catalog_infer_params_invalid", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_invalid")

	dataStore, err := store.Open("catalog_infer_params_invalid", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_invalid")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (@param1, @param1)")
	require.Equal(t, ErrInferredMultipleTypes, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (@param1)")
	require.Equal(t, ErrIllegalArguments, err)

	_, err = engine.InferParameters("INSERT INTO mytable1(id, title) VALUES (@param1, @param2)")
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, note) VALUES (@param1, @param2)")
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, err = engine.InferParameters("SELECT DISTINCT title FROM mytable")
	require.Error(t, err)

	_, err = engine.InferParameters("SELECT * FROM mytable WHERE id > @param1 AND (@param1 OR active)")
	require.Equal(t, ErrInferredMultipleTypes, err)

	_, err = engine.InferParameters("BEGIN TRANSACTION INSERT INTO mytable(id, title) VALUES (@param1, @param1) COMMIT")
	require.Equal(t, ErrInferredMultipleTypes, err)

	err = engine.Close()
	require.NoError(t, err)
}
