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
	"errors"
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

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	err = engine.ReloadCatalog(nil)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	_, err = engine.ExecStmt("CREATE DATABASE db2", nil, true)
	require.NoError(t, err)

	err = engine.CloseSnapshot()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)
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

	err = engine.UseDatabase("db1")
	require.Equal(t, ErrCatalogNotReady, err)

	_, err = engine.DatabaseInUse()
	require.Equal(t, ErrCatalogNotReady, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.DatabaseInUse()
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	db, err := engine.DatabaseInUse()
	require.NoError(t, err)
	require.Equal(t, "db1", db.name)

	err = engine.UseDatabase("db2")
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	db, err = engine.DatabaseInUse()
	require.NoError(t, err)
	require.Equal(t, "db1", db.name)

	_, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db2", nil, true)
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

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrInvalidPK, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY name)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrTableAlreadyExists, err)

	_, err = engine.ExecStmt("CREATE TABLE IF NOT EXISTS table1 (id INTEGER, PRIMARY KEY id)", nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
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

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	exists, err := engine.ExistDatabase("db1")
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = engine.ExistDatabase("db2")
	require.NoError(t, err)
	require.True(t, exists)

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

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrInvalidPK, err)

	_, err = engine.ExecStmt("ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, name VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	db, err := engine.GetDatabaseByName("db1")
	require.NoError(t, err)
	require.NotNil(t, db)

	table, err := engine.GetTableByName("db1", "table1")
	require.NoError(t, err)
	require.Len(t, table.indexes, 1)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	col, err := table.GetColumnByName("name")
	require.NoError(t, err)

	indexed, err := table.IsIndexed(col.colName)
	require.NoError(t, err)
	require.True(t, indexed)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(id)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	col, err = table.GetColumnByName("age")
	require.NoError(t, err)

	indexed, err = table.IsIndexed(col.colName)
	require.NoError(t, err)
	require.True(t, indexed)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table2(name)", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, err = engine.ExecStmt("INSERT INTO table1(id, name, age) VALUES (1, 'name1', 50)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table1(name, age) VALUES ('name2', 10)", nil, true)
	require.ErrorIs(t, err, ErrPKCanNotBeNull)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(active)", nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, true)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN NOT NULL, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, true)
	require.Equal(t, ErrNotNullableColumnCannotBeNull, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, age) VALUES (1, 50)", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", nil, true)
	require.Equal(t, ErrMissingParameter, err)

	params := make(map[string]interface{}, 1)
	params["id"] = [4]byte{1, 2, 3, 4}
	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", params, true)
	require.Equal(t, ErrUnsupportedParameter, err)

	params = make(map[string]interface{}, 1)
	params["id"] = []byte{1, 2, 3}
	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (@id, 'title1')", params, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, @title)", nil, true)
	require.Equal(t, ErrMissingParameter, err)

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, @title)", params, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = engine.ExecStmt("UPSERT INTO Table1 (id, active) VALUES (1, true)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (Id, Title, Active) VALUES (1, 'some title', false)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, active) VALUES (2, 'another title', true)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1, 'yat')", nil, true)
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, id) VALUES (1, 2)", nil, true)
	require.Equal(t, ErrDuplicatedColumn, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES ('1')", nil, true)
	require.Equal(t, ErrInvalidValue, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (NULL)", nil, true)
	require.Equal(t, ErrPKCanNotBeNull, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, active) VALUES (2, NULL, true)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (title) VALUES ('interesting title')", nil, true)
	require.Equal(t, ErrPKCanNotBeNull, err)
}

func TestAutoIncrementPK(t *testing.T) {
	catalogStore, err := store.Open("catalog_auto_inc", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_auto_inc")

	dataStore, err := store.Open("sqldata_auto_inc", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_auto_inc")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	t.Run("invalid use of auto-increment", func(t *testing.T) {
		_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR AUTO_INCREMENT, PRIMARY KEY id)", nil, true)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER AUTO_INCREMENT, PRIMARY KEY id)", nil, true)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, err = engine.ExecStmt("CREATE TABLE table1 (id VARCHAR AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id)", nil, true)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)
	})

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	summary, err := engine.ExecStmt("INSERT INTO table1(title) VALUES ('name1')", nil, true)
	require.NoError(t, err)
	require.Empty(t, summary.DDTxs)
	require.Len(t, summary.DMTxs, 1)
	require.Equal(t, uint64(1), summary.DMTxs[0].ID)
	require.Len(t, summary.LastInsertedPKs, 1)
	require.Equal(t, uint64(1), summary.LastInsertedPKs["table1"])
	require.Equal(t, 1, summary.UpdatedRows)

	_, err = engine.ExecStmt("INSERT INTO table1(id, title) VALUES (2, 'name2')", nil, true)
	require.ErrorIs(t, err, ErrNoValueForAutoIncrementalColumn)

	_, err = engine.ExecStmt("UPSERT INTO table1(id, title) VALUES (2, 'name2')", nil, true)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	_, err = engine.ExecStmt("UPSERT INTO table1(id, title) VALUES (1, 'name11')", nil, true)
	require.NoError(t, err)

	err = engine.ReloadCatalog(nil)
	require.NoError(t, err)

	summary, err = engine.ExecStmt("INSERT INTO table1(title) VALUES ('name2')", nil, true)
	require.NoError(t, err)
	require.Empty(t, summary.DDTxs)
	require.Len(t, summary.DMTxs, 1)
	require.Equal(t, uint64(3), summary.DMTxs[0].ID)
	require.Len(t, summary.LastInsertedPKs, 1)
	require.Equal(t, uint64(2), summary.LastInsertedPKs["table1"])
	require.Equal(t, 1, summary.UpdatedRows)

	summary, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			INSERT INTO table1(title) VALUES ('name3');
			INSERT INTO table1(title) VALUES ('name4');
		COMMIT
	`, nil, true)
	require.NoError(t, err)
	require.Empty(t, summary.DDTxs)
	require.Len(t, summary.DMTxs, 1)
	require.Equal(t, uint64(4), summary.DMTxs[0].ID)
	require.Len(t, summary.LastInsertedPKs, 1)
	require.Equal(t, uint64(4), summary.LastInsertedPKs["table1"])
	require.Equal(t, 2, summary.UpdatedRows)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt(`CREATE TABLE table1 (
									id INTEGER, 
									title VARCHAR, 
									PRIMARY KEY id
								)`, nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			CREATE INDEX ON table2(title)
		COMMIT
		`, nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			CREATE TABLE table2 (id INTEGER, title VARCHAR, age INTEGER, PRIMARY KEY id);
			CREATE INDEX ON table2(title);
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt(`
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE SNAPSHOT SINCE TX 1", nil, true)
	require.Equal(t, ErrNoSupported, err)

	err = engine.UseSnapshot(1, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(0, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(1, 1)
	require.Equal(t, ErrTxDoesNotExist, err)

	err = engine.UseSnapshot(1, 2)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = engine.ExecStmt(`
		BEGIN TRANSACTION
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT
		`, nil, true)
	require.NoError(t, err)

	err = engine.UseSnapshot(1, 0)
	require.NoError(t, err)

	err = engine.RenewSnapshot()
	require.NoError(t, err)

	err = engine.CloseSnapshot()
	require.NoError(t, err)

	err = engine.UseSnapshot(0, 1)
	require.NoError(t, err)

	err = engine.UseSnapshot(1, 1)
	require.NoError(t, err)
}

func TestEncodeRawValue(t *testing.T) {
	b, err := EncodeRawValue(uint64(1), IntegerType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeRawValue(true, IntegerType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeRawValue(true, BooleanType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeRawValue(uint64(1), BooleanType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeRawValue("title", VarcharType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeRawValue(uint64(1), VarcharType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeRawValue([]byte{}, BLOBType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeRawValue(nil, BLOBType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeRawValue(uint64(1), BLOBType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeRawValue(uint64(1), "invalid type", true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeRawValue("012345678901234567890123456789012", VarcharType, true)
	require.ErrorIs(t, err, ErrInvalidPK)
	require.Nil(t, b)

	_, err = EncodeRawValue("01234567890123456789012345678902", VarcharType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue("012345678901234567890123456789012", VarcharType, false)
	require.NoError(t, err)

	b, err = EncodeRawValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}, BLOBType, true)
	require.ErrorIs(t, err, ErrInvalidPK)
	require.Nil(t, b)

	_, err = EncodeRawValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
	}, BLOBType, true)
	require.NoError(t, err)

	_, err = EncodeRawValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}, BLOBType, false)
	require.NoError(t, err)
}

func TestEncodeValue(t *testing.T) {
	b, err := EncodeValue(&Number{val: 1}, IntegerType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue(&Bool{val: true}, IntegerType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Bool{val: true}, BooleanType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeValue(&Number{val: 1}, BooleanType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Varchar{val: "title"}, VarcharType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeValue(&Number{val: 1}, VarcharType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Blob{val: []byte{}}, BLOBType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue(&Blob{val: nil}, BLOBType, true)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue(&Number{val: 1}, BLOBType, true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Number{val: 1}, "invalid type", true)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeValue(&Varchar{val: "012345678901234567890123456789012"}, VarcharType, true)
	require.ErrorIs(t, err, ErrInvalidPK)
	require.Nil(t, b)

	_, err = EncodeValue(&Varchar{val: "01234567890123456789012345678902"}, VarcharType, true)
	require.NoError(t, err)

	_, err = EncodeValue(&Varchar{val: "012345678901234567890123456789012"}, VarcharType, false)
	require.NoError(t, err)

	b, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}, BLOBType, true)
	require.ErrorIs(t, err, ErrInvalidPK)
	require.Nil(t, b)

	_, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
	}}, BLOBType, true)
	require.NoError(t, err)

	_, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}, BLOBType, false)
	require.NoError(t, err)
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

	_, err = engine.ExistDatabase("db1")
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.UseDatabase("db1")
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.GetDatabaseByName("db1")
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.GetTableByName("db1", "table1")
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.DatabaseInUse()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.UseSnapshot(0, 0)
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.RenewSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.CloseSnapshot()
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.InferParameters("CREATE DATABASE db1")
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = engine.InferParametersPreparedStmt(&TxStmt{})
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.EnsureCatalogReady(nil)
	require.Equal(t, ErrAlreadyClosed, err)

	err = engine.ReloadCatalog(nil)
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

	_, err = engine.QueryStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrCatalogNotReady, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.QueryStmt("SELECT * FROM table1", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.ExecStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id FROM db2.table1", nil, true)
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	_, err = engine.QueryStmt("SELECT id FROM table1", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, ts INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["id"] = 0

	r, err := engine.QueryStmt("SELECT id FROM db1.table1 WHERE id >= @id", nil, true)
	require.NoError(t, err)

	orderBy := r.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)
	require.Equal(t, "db1", orderBy[0].Database)

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
		_, err = engine.ExecStmt(fmt.Sprintf(`
			UPSERT INTO table1 (id, ts, title, active, payload)
			VALUES (%d, NOW(), 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	_, err = engine.QueryStmt("SELECT DISTINCT id1 FROM table1", nil, true)
	require.Equal(t, ErrNoSupported, err)

	t.Run("should fail reading due to non-existent column", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT id1 FROM table1", nil, true)
		require.NoError(t, err)

		row, err := r.Read()
		require.Equal(t, ErrColumnDoesNotExist, err)
		require.Nil(t, row)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with two-time table aliasing", func(t *testing.T) {
		r, err = engine.QueryStmt(fmt.Sprintf(`
			SELECT * FROM (table1 AS T1) WHERE t1.id >= 0 LIMIT %d AS mytable1
		`, rowCount), nil, true)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector()
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)

		require.Equal(t, "db1", r.ImplicitDB())
		require.Equal(t, "mytable1", r.ImplicitTable())

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.Values, 5)
			require.Less(t, uint64(start), row.Values[EncodeSelector("", "db1", "mytable1", "ts")].Value())
			require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "mytable1", "id")].Value())
			require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "mytable1", "title")].Value())
			require.Equal(t, i%2 == 0, row.Values[EncodeSelector("", "db1", "mytable1", "active")].Value())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.Values[EncodeSelector("", "db1", "mytable1", "payload")].Value())
		}

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with column and two-time table aliasing", func(t *testing.T) {
		r, err = engine.QueryStmt(fmt.Sprintf(`
			SELECT t1.id AS D, ts, Title, payload, Active FROM (table1 AS T1) WHERE t1.id >= 0 LIMIT %d AS mytable1
		`, rowCount), nil, true)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector()
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)

		require.Equal(t, "db1", r.ImplicitDB())
		require.Equal(t, "mytable1", r.ImplicitTable())

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.Values, 5)
			require.Less(t, uint64(start), row.Values[EncodeSelector("", "db1", "mytable1", "ts")].Value())
			require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "mytable1", "d")].Value())
			require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "mytable1", "title")].Value())
			require.Equal(t, i%2 == 0, row.Values[EncodeSelector("", "db1", "mytable1", "active")].Value())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.Values[EncodeSelector("", "db1", "mytable1", "payload")].Value())
		}

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	r, err = engine.QueryStmt("SELECT id, title, active, payload FROM table1 ORDER BY title", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)
	require.Nil(t, r)

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

	params = make(map[string]interface{})
	params["some_param1"] = true

	r, err = engine.QueryStmt("SELECT id FROM table1 WHERE active = @some_param1", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrMissingParameter, err)

	r.SetParameters(params)

	row, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, uint64(2), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param"] = true

	encPayloadPrefix := hex.EncodeToString([]byte("blob"))

	r, err = engine.QueryStmt(fmt.Sprintf(`
		SELECT id, title, active
		FROM table1
		WHERE active = @some_param AND title > 'title' AND payload >= x'%s' AND title LIKE 't`, encPayloadPrefix), params, true)
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

	cols, err := r.Columns()
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

	r, err = engine.QueryStmt("INVALID QUERY", nil, false)
	require.EqualError(t, err, "syntax error: unexpected IDENTIFIER")
	require.Nil(t, r)

	r, err = engine.QueryStmt("UPSERT INTO table1 (id) VALUES(1)", nil, false)
	require.ErrorIs(t, err, ErrExpectingDQLStmt)
	require.Nil(t, r)

	r, err = engine.QueryStmt("UPSERT INTO table1 (id) VALUES(1); UPSERT INTO table1 (id) VALUES(1)", nil, false)
	require.ErrorIs(t, err, ErrExpectingDQLStmt)
	require.Nil(t, r)

	r, err = engine.QueryPreparedStmt(nil, nil, false)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, r)

	params = make(map[string]interface{})
	params["null_param"] = nil

	r, err = engine.QueryStmt("SELECT id FROM table1 WHERE active = @null_param", params, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT * FROM table1", nil, true)
	require.ErrorIs(t, err, ErrAlreadyClosed)
	require.Nil(t, r)

	r, err = engine.QueryStmt("SELECT * FROM table1", nil, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)
	require.Nil(t, r)
}

func TestIndexing(t *testing.T) {
	catalogStore, err := store.Open("catalog_indexing", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_indexing")

	dataStore, err := store.Open("sqldata_indexing", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_indexing")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt(`CREATE TABLE table1 (
								id INTEGER AUTO_INCREMENT, 
								ts INTEGER, 
								title VARCHAR, 
								active BOOLEAN, 
								payload BLOB, 
								PRIMARY KEY id
							)`, nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1 (ts)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE UNIQUE INDEX ON table1 (title)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1 (active, title)", nil, true)
	require.NoError(t, err)

	t.Run("should use primary index by default", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "id", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.True(t, scanSpecs.index.isPrimary())
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, GreaterOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use primary index in descending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 ORDER BY id DESC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "id", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.True(t, scanSpecs.index.isPrimary())
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, LowerOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` descending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 ORDER BY ts DESC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.False(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, LowerOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE ts = 1629902962 ORDER BY ts", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.False(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Len(t, scanSpecs.valuesByColID, 1)
		require.Equal(t, uint64(1629902962), scanSpecs.valuesByColID[2].Value())
		require.Equal(t, 1, scanSpecs.fixedValuesCount)
		require.Equal(t, GreaterOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` with max value in desc order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title < 'title10' ORDER BY title DESC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.True(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Len(t, scanSpecs.valuesByColID, 1)
		require.Equal(t, "title10", scanSpecs.valuesByColID[3].Value())
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, LowerOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` ascending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title > 'title10' ORDER BY ts ASC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.False(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, GreaterOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` descending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY ts DESC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.False(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, LowerOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` descending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY title DESC", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.True(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, LowerOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order starting with 'title1'", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY title", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.True(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Len(t, scanSpecs.valuesByColID, 1)
		require.Equal(t, "title1", scanSpecs.valuesByColID[3].Value())
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, GreaterOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order", func(t *testing.T) {
		r, err := engine.QueryStmt("SELECT * FROM table1 WHERE title < 'title10' or title = 'title1' ORDER BY title", nil, true)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.index)
		require.False(t, scanSpecs.index.isPrimary())
		require.True(t, scanSpecs.index.unique)
		require.Len(t, scanSpecs.index.colIDs, 1)
		require.Empty(t, scanSpecs.valuesByColID)
		require.Zero(t, scanSpecs.fixedValuesCount)
		require.Equal(t, GreaterOrEqualTo, scanSpecs.cmp)

		err = r.Close()
		require.NoError(t, err)
	})

	err = engine.Close()
	require.NoError(t, err)
}

func TestExecCornerCases(t *testing.T) {
	catalogStore, err := store.Open("catalog_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_q")

	dataStore, err := store.Open("sqldata_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_q")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	summary, err := engine.ExecStmt("INVALID STATEMENT", nil, false)
	require.EqualError(t, err, "syntax error: unexpected IDENTIFIER")
	require.Nil(t, summary)

	err = engine.Close()
	require.NoError(t, err)

	summary, err = engine.ExecStmt("CREATE TABLE t1(id INTEGER, primary key id)", nil, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)
	require.Nil(t, summary)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, ts INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table1 (id, ts, title) VALUES (1, TIME(), 'title1')", nil, true)
	require.Equal(t, ErrNoSupported, err)

	rowCount := 10

	start := time.Now().UnixNano()

	for i := 0; i < rowCount; i++ {
		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, ts, title) VALUES (%d, NOW(), 'title%d')", i, i), nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, PRIMARY KEY id)", nil, true)
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

	_, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age", nil, true)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	params := make(map[string]interface{}, 1)
	params["age"] = nil
	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (1, 'title', @age)", params, true)
	require.Equal(t, ErrIndexedColumnCanNotBeNull, err)

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'title')", nil, true)
	require.Equal(t, ErrIndexedColumnCanNotBeNull, err)

	rowCount := 1

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = 40 + i

		_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY title", nil, true)
	require.NoError(t, err)

	orderBy := r.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "title", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)
	require.Equal(t, "db1", orderBy[0].Database)

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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, err = engine.ExecStmt(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, true)
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

	_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title) VALUES (%d, 'title%d')", rowCount, rowCount), nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	rowCount := 10
	base := 30

	for i := 1; i <= rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i

		_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
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
	require.Nil(t, row)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt(`
		SELECT COUNT(), SUM(age), MIN(title), MAX(age), AVG(age), MIN(active), MAX(active), MIN(payload)
		FROM table1 WHERE false`, nil, true)
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

func TestCount(t *testing.T) {
	catalogStore, err := store.Open("catalog_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_agg")

	dataStore, err := store.Open("sqldata_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_agg")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE t1(id INTEGER AUTO_INCREMENT, val1 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON t1(val1)", nil, true)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			_, err = engine.ExecStmt("INSERT INTO t1(val1) VALUES($1)", map[string]interface{}{"param1": j}, true)
			require.NoError(t, err)
		}
	}

	r, err := engine.QueryStmt("SELECT COUNT() as c FROM t1", nil, true)
	require.NoError(t, err)

	row, err := r.Read()
	require.NoError(t, err)
	require.EqualValues(t, uint64(30), row.Values["(db1.t1.c)"].Value())

	err = r.Close()
	require.NoError(t, err)

	_, err = engine.QueryStmt("SELECT COUNT() as c FROM t1 GROUP BY val1", nil, true)
	require.ErrorIs(t, err, ErrLimitedGroupBy)

	r, err = engine.QueryStmt("SELECT COUNT() as c FROM t1 GROUP BY val1 ORDER BY val1", nil, true)
	require.NoError(t, err)

	for j := 0; j < 3; j++ {
		row, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, uint64(10), row.Values["(db1.t1.c)"].Value())
	}

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE INDEX ON table1(active)", nil, true)
	require.NoError(t, err)

	rowCount := 10
	base := 40

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 4)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i
		params["active"] = i%2 == 0

		_, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age, active) VALUES (@id, @title, @age, @active)", params, true)
		require.NoError(t, err)
	}

	_, err = engine.QueryStmt("SELECT active, COUNT(), SUM(age1) FROM table1 WHERE active != null HAVING AVG(age) >= MIN(age)", nil, true)
	require.Equal(t, ErrHavingClauseRequiresGroupClause, err)

	r, err := engine.QueryStmt(`
		SELECT active, COUNT(), SUM(age1)
		FROM table1
		WHERE active != null
		GROUP BY active
		HAVING AVG(age) >= MIN(age)
		ORDER BY active`, nil, true)
	require.NoError(t, err)

	r.SetParameters(nil)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt(`
		SELECT active, COUNT(), SUM(age1)
		FROM table1
		WHERE AVG(age) >= MIN(age)
		GROUP BY active
		ORDER BY active`, nil, true)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT active, COUNT(id) FROM table1 GROUP BY active ORDER BY active", nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrLimitedCount, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt(`
		SELECT active, COUNT()
		FROM table1
		GROUP BY active
		HAVING AVG(age) >= MIN(age1)
		ORDER BY active`, nil, true)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt(`
		SELECT active, COUNT() as c, MIN(age), MAX(age), AVG(age), SUM(age)
		FROM table1
		GROUP BY active
		HAVING COUNT() <= SUM(age)   AND 
				MIN(age) <= MAX(age) AND 
				AVG(age) <= MAX(age) AND 
				MAX(age) < SUM(age)  AND 
				AVG(age) >= MIN(age) AND 
				SUM(age) > 0
		ORDER BY active DESC`, nil, true)

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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, fkid2 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, amount INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, err = engine.ExecStmt(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)`, i, i, rowCount-1-i, i), nil, true)
		require.NoError(t, err)

		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table2 (id, amount) VALUES (%d, %d)", rowCount-1-i, i*i), nil, true)
		require.NoError(t, err)

		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, true)
		require.NoError(t, err)
	}

	t.Run("should not find any matching row", func(t *testing.T) {
		r, err := engine.QueryStmt(`
		SELECT table1.title, table2.amount, table3.age
		FROM (SELECT * FROM table2 WHERE amount = 1)
		INNER JOIN table1 ON table2.id = table1.fkid1 AND table2.amount > 0
		INNER JOIN table3 ON table1.fkid2 = table3.id AND table3.age < 30`, nil, true)
		require.NoError(t, err)

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should find one matching row", func(t *testing.T) {
		r, err := engine.QueryStmt(`
		SELECT t1.title, t2.amount, t3.age
		FROM (SELECT id, amount FROM table2 WHERE amount = 1 AS t2)
		INNER JOIN (table1 as t1) ON t2.id = t1.fkid1 AND t2.amount > 0
		INNER JOIN (table3 as t3) ON t1.fkid2 = t3.id AND t3.age > 30`, nil, true)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.Len(t, row.Values, 3)

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every inserted row", func(t *testing.T) {
		r, err := engine.QueryStmt(`
			SELECT id, title, table2.amount, table3.age 
			FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id 
			INNER JOIN table3 ON table1.fkid2 = table3.id
			WHERE table1.id >= 0 AND table3.age >= 30
			ORDER BY id DESC`, nil, true)
		require.NoError(t, err)

		r.SetParameters(nil)

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
	})

	err = engine.Close()
	require.NoError(t, err)
}

func TestJoinsWithJointTable(t *testing.T) {
	catalogStore, err := store.Open("catalog_innerjoin_joint", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_innerjoin_joint")

	dataStore, err := store.Open("sqldata_innerjoin_joint", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_innerjoin_joint")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER AUTO_INCREMENT, amount INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table12 (id INTEGER AUTO_INCREMENT, fkid1 INTEGER, fkid2 INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table1 (name) VALUES ('name1'), ('name2'), ('name3')", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table2 (amount) VALUES (10), (20), (30)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table12 (fkid1, fkid2, active) VALUES (1,1,false),(1,2,true),(1,3,true)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table12 (fkid1, fkid2, active) VALUES (2,1,false),(2,2,false),(2,3,true)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("INSERT INTO table12 (fkid1, fkid2, active) VALUES (3,1,false),(3,2,false),(3,3,false)", nil, true)
	require.NoError(t, err)

	r, err := engine.QueryStmt(`
		SELECT t1.name, t2.amount, t12.active
		FROM (SELECT * FROM table1 where name = 'name1' AS t1)
		INNER JOIN (table12 AS t12) on t12.fkid1 = t1.id
		INNER JOIN (table2 AS t2)  on t12.fkid2 = t2.id
		WHERE t12.active = true
		AS q`, nil, true)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 3)

	for i := 0; i < 2; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 3)

		require.Equal(t, "name1", row.Values[EncodeSelector("", "db1", "q", "name")].Value())
		require.Equal(t, uint64(20+i*10), row.Values[EncodeSelector("", "db1", "q", "amount")].Value())
		require.Equal(t, true, row.Values[EncodeSelector("", "db1", "q", "active")].Value())
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, amount INTEGER, fkid1 INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1) VALUES (%d, 'title%d', %d)", i, i, rowCount-1-i), nil, true)
		require.NoError(t, err)

		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table2 (id, amount, fkid1) VALUES (%d, %d, %d)", rowCount-1-i, i*i, i), nil, true)
		require.NoError(t, err)

		_, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt(`
		SELECT id, title, t2.amount AS total_amount, t3.age
		FROM (table1 AS t1)
		INNER JOIN (table2 as t2) ON (fkid1 = t2.id AND title != NULL)
		INNER JOIN (table3 as t3) ON t2.fkid1 = t3.id
		ORDER BY id DESC`, nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1; CREATE TABLE table1 (id INTEGER, name VARCHAR, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, err = engine.ExecStmt("USE DATABASE db1; CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	engine, err = NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.ExistDatabase("db1")
	require.ErrorIs(t, err, ErrCatalogNotReady)

	_, err = engine.GetDatabaseByName("db1")
	require.ErrorIs(t, err, ErrCatalogNotReady)

	_, err = engine.GetTableByName("db1", "table1")
	require.ErrorIs(t, err, ErrCatalogNotReady)

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	exists, err := engine.ExistDatabase("db1")
	require.NoError(t, err)
	require.True(t, exists)

	db, err := engine.GetDatabaseByName("db1")
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

	indexed, err := table.IsIndexed(col.colName)
	require.NoError(t, err)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, err = engine.ExecStmt(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt(`
		SELECT id, title AS t
		FROM (SELECT id, title, active FROM table1 AS table2)
		WHERE active AND table2.id >= 0 AS t2`, nil, true)
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

	_, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (0, 'title0')", nil, true)
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

	_, err = engine.InferParameters(stmt)
	require.ErrorIs(t, err, ErrCatalogNotReady)

	_, err = engine.InferParametersPreparedStmt(&CreateDatabaseStmt{})
	require.ErrorIs(t, err, ErrCatalogNotReady)

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	_, err = engine.InferParameters(stmt)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	_, err = engine.InferParametersPreparedStmt(&CreateDatabaseStmt{})
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	_, err = engine.ExecStmt(stmt, nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.InferParameters("invalid sql stmt")
	require.EqualError(t, err, "syntax error: unexpected IDENTIFIER")

	_, err = engine.InferParametersPreparedStmt(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	params, err := engine.InferParameters(stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

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

	pstmt, err := Parse(strings.NewReader(stmt))
	require.NoError(t, err)
	require.Len(t, pstmt, 1)

	_, err = engine.InferParametersPreparedStmt(pstmt[0])
	require.NoError(t, err)

	_, err = engine.ExecStmt(stmt, nil, true)
	require.NoError(t, err)

	params, err = engine.InferParameters("ALTER TABLE mytableSE ADD COLUMN note VARCHAR")
	require.NoError(t, err)
	require.Len(t, params, 0)

	stmt = "CREATE INDEX ON mytable(active)"

	params, err = engine.InferParameters(stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, err = engine.ExecStmt(stmt, nil, true)
	require.NoError(t, err)

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

	params, err = engine.InferParameters("SELECT COUNT() FROM mytable GROUP BY active HAVING @param1 = COUNT() ORDER BY active")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters("SELECT COUNT(), MIN(id) FROM mytable GROUP BY active HAVING @param1 < MIN(id) ORDER BY active")
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	stmts, err := Parse(strings.NewReader("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	params, err := engine.InferParametersPreparedStmt(stmts[0])
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, err = engine.ExecPreparedStmts(stmts, nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
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

	_, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	err = engine.UseDatabase("db1")
	require.NoError(t, err)

	_, err = engine.ExecStmt("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, true)
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

func TestDecodeValueFailures(t *testing.T) {
	for _, d := range []struct {
		n string
		b []byte
		t SQLValueType
	}{
		{
			"Empty data", []byte{}, IntegerType,
		},
		{
			"Not enough bytes for length", []byte{1, 2}, IntegerType,
		},
		{
			"Not enough data", []byte{0, 0, 0, 3, 1, 2}, VarcharType,
		},
		{
			"Negative length", []byte{0x80, 0, 0, 0, 0}, VarcharType,
		},
		{
			"Too large integer", []byte{0, 0, 0, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9}, IntegerType,
		},
		{
			"Zero-length boolean", []byte{0, 0, 0, 0}, BooleanType,
		},
		{
			"Too large boolean", []byte{0, 0, 0, 2, 0, 0}, BooleanType,
		},
		{
			"Any type", []byte{0, 0, 0, 1, 1}, AnyType,
		},
	} {
		t.Run(d.n, func(t *testing.T) {
			_, _, err := DecodeValue(d.b, d.t)
			require.True(t, errors.Is(err, ErrCorruptedData))
		})
	}
}

func TestDecodeValueSuccess(t *testing.T) {
	for _, d := range []struct {
		n string
		b []byte
		t SQLValueType

		v    TypedValue
		offs int
	}{
		{
			"varchar",
			[]byte{0, 0, 0, 2, 'H', 'i'},
			VarcharType,
			&Varchar{val: "Hi"},
			6,
		},
		{
			"varchar padded",
			[]byte{0, 0, 0, 2, 'H', 'i', 1, 2, 3},
			VarcharType,
			&Varchar{val: "Hi"},
			6,
		},
		{
			"empty varchar",
			[]byte{0, 0, 0, 0},
			VarcharType,
			&Varchar{val: ""},
			4,
		},
		{
			"zero integer",
			[]byte{0, 0, 0, 0},
			IntegerType,
			&Number{val: 0},
			4,
		},
		{
			"byte",
			[]byte{0, 0, 0, 1, 123},
			IntegerType,
			&Number{val: 123},
			5,
		},
		{
			"byte padded",
			[]byte{0, 0, 0, 1, 123, 45, 6},
			IntegerType,
			&Number{val: 123},
			5,
		},
		{
			"large integer",
			[]byte{0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8},
			IntegerType,
			&Number{val: 0x102030405060708},
			12,
		},
		{
			"large integer padded",
			[]byte{0, 0, 0, 8, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1},
			IntegerType,
			&Number{val: 0x100000000000000},
			12,
		},
		{
			"boolean false",
			[]byte{0, 0, 0, 1, 0},
			BooleanType,
			&Bool{val: false},
			5,
		},
		{
			"boolean true",
			[]byte{0, 0, 0, 1, 1},
			BooleanType,
			&Bool{val: true},
			5,
		},
		{
			"boolean padded",
			[]byte{0, 0, 0, 1, 0, 1},
			BooleanType,
			&Bool{val: false},
			5,
		},
		{
			"blob",
			[]byte{0, 0, 0, 2, 'H', 'i'},
			BLOBType,
			&Blob{val: []byte{'H', 'i'}},
			6,
		},
		{
			"blob padded",
			[]byte{0, 0, 0, 2, 'H', 'i', 1, 2, 3},
			BLOBType,
			&Blob{val: []byte{'H', 'i'}},
			6,
		},
		{
			"empty blob",
			[]byte{0, 0, 0, 0},
			BLOBType,
			&Blob{val: []byte{}},
			4,
		},
	} {
		t.Run(d.n, func(t *testing.T) {
			v, offs, err := DecodeValue(d.b, d.t)
			require.NoError(t, err)
			require.EqualValues(t, d.offs, offs)

			cmp, err := d.v.Compare(v)
			require.NoError(t, err)
			require.Zero(t, cmp)
		})
	}
}

func TestTrimPrefix(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix")}

	for _, d := range []struct {
		n string
		k string
	}{
		{"empty key", ""},
		{"no engine prefix", "no-e-prefix"},
		{"no mapping prefix", "e-prefix-no-mapping-prefix"},
		{"short mapping prefix", "e-prefix-mapping"},
	} {
		t.Run(d.n, func(t *testing.T) {
			prefix, err := e.trimPrefix([]byte(d.k), []byte("-mapping-prefix"))
			require.Nil(t, prefix)
			require.ErrorIs(t, err, ErrIllegalMappedKey)
		})
	}

	for _, d := range []struct {
		n string
		k string
		p string
	}{
		{"correct prefix", "e-prefix-mapping-prefix-key", "-key"},
		{"exact prefix", "e-prefix-mapping-prefix", ""},
	} {
		t.Run(d.n, func(t *testing.T) {
			prefix, err := e.trimPrefix([]byte(d.k), []byte("-mapping-prefix"))
			require.NoError(t, err)
			require.NotNil(t, prefix)
			require.EqualValues(t, prefix, []byte(d.p))
		})
	}
}

func TestUnmapDatabaseId(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	id, err := e.unmapDatabaseID(nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = e.unmapDatabaseID([]byte{})
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = e.unmapDatabaseID([]byte("pref"))
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = e.unmapDatabaseID([]byte("e-prefix.a"))
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = e.unmapDatabaseID([]byte(
		"e-prefix.CATALOG.DATABASE.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, id)

	id, err = e.unmapDatabaseID(append(
		[]byte("e-prefix.CATALOG.DATABASE."),
		1, 2, 3, 4, 5, 6, 7, 8,
	))
	require.NoError(t, err)
	require.EqualValues(t, 0x0102030405060708, id)
}

func TestUnmapTableId(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, pkID, err := e.unmapTableID(nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, pkID)

	dbID, tableID, pkID, err = e.unmapTableID([]byte(
		"e-prefix.CATALOG.TABLE.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, pkID)

	dbID, tableID, pkID, err = e.unmapTableID(append(
		[]byte("e-prefix.CATALOG.TABLE."),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	))
	require.NoError(t, err)
	require.EqualValues(t, 0x0102030405060708, dbID)
	require.EqualValues(t, 0x1112131415161718, tableID)
	require.EqualValues(t, 0x2122232425262728, pkID)
}

func TestUnmapColSpec(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, colID, colType, err := e.unmapColSpec(nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = e.unmapColSpec([]byte(
		"e-prefix.CATALOG.COLUMN.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = e.unmapColSpec(append(
		[]byte("e-prefix.CATALOG.COLUMN."),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		0x00,
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = e.unmapColSpec(append(
		[]byte("e-prefix.CATALOG.COLUMN."),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
		'I', 'N', 'T', 'E', 'G', 'E', 'R',
	))

	require.NoError(t, err)
	require.EqualValues(t, 0x0102030405060708, dbID)
	require.EqualValues(t, 0x1112131415161718, tableID)
	require.EqualValues(t, 0x2122232425262728, colID)
	require.Equal(t, "INTEGER", colType)
}

func TestUnmapIndex(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, colID, err := e.unmapIndex(nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)

	dbID, tableID, colID, err = e.unmapIndex([]byte(
		"e-prefix.CATALOG.INDEX.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)

	dbID, tableID, colID, err = e.unmapIndex(append(
		[]byte("e-prefix.CATALOG.INDEX."),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	))

	require.NoError(t, err)
	require.EqualValues(t, 0x0102030405060708, dbID)
	require.EqualValues(t, 0x1112131415161718, tableID)
	require.EqualValues(t, 0x2122232425262728, colID)
}

func TestUnmapIndexEntry(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, indexID, encVals, encPKVal, err := e.unmapIndexEntry(PIndexPrefix, nil)
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, indexID)
	require.Nil(t, encVals)
	require.Nil(t, encPKVal)

	dbID, tableID, indexID, encVals, encPKVal, err = e.unmapIndexEntry(PIndexPrefix, []byte(
		"e-prefix.PINDEX.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, indexID)
	require.Nil(t, encVals)
	require.Nil(t, encPKVal)

	fullValue := append(
		[]byte("e-prefix.SINDEX."),
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		0, 0, 0, 3,
		'a', 'b', 'c',
	)

	for i := 13; i < len(fullValue); i++ {
		dbID, tableID, indexID, encVals, encPKVal, err = e.unmapIndexEntry(SIndexPrefix, fullValue[:i])
		require.ErrorIs(t, err, ErrCorruptedData)
		require.Zero(t, dbID)
		require.Zero(t, tableID)
		require.Zero(t, indexID)
		require.Nil(t, encVals)
		require.Nil(t, encPKVal)
	}

	dbID, tableID, indexID, encVals, encPKVal, err = e.unmapIndexEntry(SIndexPrefix, fullValue)
	require.NoError(t, err)
	require.EqualValues(t, 0x0102030405060708, dbID)
	require.EqualValues(t, 0x1112131415161718, tableID)
	require.EqualValues(t, uint64(2), indexID)
	require.Nil(t, encVals)
	require.EqualValues(t, []byte{0, 0, 0, 3, 'a', 'b', 'c'}, encPKVal)
}
