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

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db2", nil, true)
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

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	require.Equal(t, "db1", engine.implicitDB)

	_, _, err = engine.ExecStmt("USE DATABASE db2", nil, true)
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	require.Equal(t, "db1", engine.implicitDB)
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

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (name STRING, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrInvalidPK, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (name STRING, PRIMARY KEY name)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table2 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, true)
	require.Equal(t, ErrTableAlreadyExists, err)
}

func TestCreateIndex(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, name STRING, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	db := engine.catalog.Databases()[0]

	table, ok := db.tablesByName["table1"]
	require.True(t, ok)

	require.Len(t, table.indexes, 0)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	_, indexed := table.indexes[table.colsByName["name"].id]
	require.True(t, indexed)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(id)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	_, indexed = table.indexes[table.colsByName["age"].id]
	require.True(t, indexed)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table2(name)", nil, true)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(title)", nil, true)
	require.Equal(t, ErrColumnDoesNotExist, err)

	require.Len(t, table.indexes, 2)
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

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (1, 'some title')", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title) VALUES (2, 'another title')", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES (1, 'yat')", nil, true)
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, id) VALUES (1, 2)", nil, true)
	require.Equal(t, ErrDuplicatedColumn, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (id) VALUES ('1')", nil, true)
	require.Equal(t, ErrInvalidValue, err)

	_, _, err = engine.ExecStmt("UPSERT INTO table1 (title) VALUES ('interesting title')", nil, true)
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

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, ts INTEGER, title STRING, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	start := time.Now().UnixNano()

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, ts, title, active, payload) VALUES (%d, NOW(), 'title%d', %v, b'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, ts, title, payload, active FROM table1", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 5)
		require.Less(t, uint64(start), row.Values[EncodeSelector("", "db1", "table1", "ts")].Value())
		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, i%2 == 0, row.Values[EncodeSelector("", "db1", "table1", "active")].Value())

		encPayload := []byte(fmt.Sprintf("blob%d", i))
		require.Equal(t, []byte(encPayload), row.Values[EncodeSelector("", "db1", "table1", "payload")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active, payload FROM table1 ORDER BY id DESC", nil)
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

	params := make(map[string]interface{})
	params["some_param"] = true

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE active = @some_param", params)
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
}

func TestOrderBy(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	rowCount := 1

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = 40 + i

		_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age", nil)
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

	r, err = engine.QueryStmt("SELECT id, title, age FROM table1 ORDER BY age DESC", nil)
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
}

func TestQueryWithRowFiltering(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, b'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title, active FROM table1 WHERE false", nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE false OR true", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 < 2", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 >= 2", nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE 1 = true", nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNotComparableValues, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE NOT table1.active", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, active FROM table1 WHERE table1.id > 4", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)
}

func TestGroupByHaving(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, age INTEGER, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(age)", nil, true)
	require.NoError(t, err)

	itCount := 10
	rowCount := 10

	for i := 0; i < itCount; i++ {
		for j := 0; j < rowCount; j++ {
			params := make(map[string]interface{}, 3)
			params["id"] = i*10 + j
			params["title"] = fmt.Sprintf("title%d", i*10+j)
			params["age"] = 40 + j

			_, _, err = engine.ExecStmt("UPSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, true)
			require.NoError(t, err)
		}
	}

	r, err := engine.QueryStmt("SELECT age, COUNT(id) FROM table1 GROUP BY age HAVING COUNT(id) > 0 ORDER BY age", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 2)

		if uint64(itCount) != row.Values[EncodeSelector("COUNT", "db1", "table1", "id")].Value() {
			require.NotNil(t, row)
		}
		require.Equal(t, uint64(itCount), row.Values[EncodeSelector("COUNT", "db1", "table1", "id")].Value())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestJoins(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, fkid1 INTEGER, fkid2 INTEGER, PRIMARY KEY id)", nil, true)
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

	r, err := engine.QueryStmt("SELECT id, title, table2.amount, table3.age FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id INNER JOIN table3 ON table1.fkid2 = table3.id WHERE table1.id >= 0 AND table3.age >= 30 ORDER BY id DESC", nil)
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

	_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)", rowCount, rowCount, rowCount, rowCount), nil, true)
	require.NoError(t, err)

	r, err = engine.QueryStmt("SELECT id, title, table2.amount, table3.age FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id INNER JOIN table3 ON table1.fkid2 = table3.id ORDER BY id DESC", nil)
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

}

func TestNestedJoins(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, fkid1 INTEGER, PRIMARY KEY id)", nil, true)
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

	r, err := engine.QueryStmt("SELECT id, title, table2.amount AS amount, table3.age AS age FROM table1 INNER JOIN table2 ON fkid1 = table2.id INNER JOIN table3 ON table2.fkid1 = table3.id ORDER BY id DESC", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 4)

		require.Equal(t, uint64(rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.Values[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, uint64((rowCount-1-i)*(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table1", "amount")].Value())
		require.Equal(t, uint64(30+(rowCount-1-i)), row.Values[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestReOpening(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, name STRING, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE INDEX ON table1(name)", nil, true)
	require.NoError(t, err)

	engine, err = NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	exists := engine.catalog.ExistDatabase("db1")
	require.True(t, exists)

	db := engine.catalog.dbsByName["db1"]

	exists = db.ExistTable("table1")
	require.True(t, exists)

	table := db.tablesByName["table1"]

	require.Equal(t, "id", table.pk.colName)

	require.Len(t, table.colsByName, 2)

	require.Equal(t, IntegerType, table.colsByName["id"].colType)
	require.Equal(t, StringType, table.colsByName["name"].colType)

	require.Len(t, table.indexes, 1)

	_, indexed := table.indexes[table.colsByName["name"].id]
	require.True(t, indexed)
}

func TestSubQuery(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	dataStore, err := store.Open("sqldata", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("USE DATABASE db1", nil, true)
	require.NoError(t, err)

	_, _, err = engine.ExecStmt("CREATE TABLE table1 (id INTEGER, title STRING, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, true)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.ExecStmt(fmt.Sprintf("UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, b'%s')", i, i, i%2 == 0, encPayload), nil, true)
		require.NoError(t, err)
	}

	r, err := engine.QueryStmt("SELECT id, title AS t FROM (SELECT id, title, active FROM table1 as table2) WHERE active", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i += 2 {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.Values, 2)

		require.Equal(t, uint64(i), row.Values[EncodeSelector("", "db1", "table2", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.Values[EncodeSelector("", "db1", "table2", "t")].Value())
	}

	err = r.Close()
	require.NoError(t, err)
}
