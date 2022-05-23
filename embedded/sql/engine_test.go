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
package sql

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

var sqlPrefix = []byte{2}

func TestCreateDatabase(t *testing.T) {
	st, err := store.Open("sqldata_create_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_db")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	_, _, err = engine.Exec("CREATE DATABASE IF NOT EXISTS db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db2", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE IF NOT EXISTS db3", nil, nil)
	require.NoError(t, err)
}

func TestUseDatabase(t *testing.T) {
	st, err := store.Open("sqldata_use_db", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_use_db")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "db1", engine.CurrentDatabase())

	_, _, err = engine.Exec("USE DATABASE db2", nil, nil)
	require.ErrorIs(t, err, ErrDatabaseDoesNotExist)
	require.Equal(t, "db1", engine.CurrentDatabase())

	_, _, err = engine.Exec("CREATE DATABASE db2", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		USE db1;
		CREATE TABLE table1(id INTEGER, PRIMARY KEY id);
		USE db2;
		CREATE TABLE table1(id INTEGER, PRIMARY KEY id);
		`, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "db2", engine.CurrentDatabase())
}

func TestCreateTable(t *testing.T) {
	st, err := store.Open("sqldata_create_table", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_table")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.Exec("CREATE DATABASE IF NOT EXISTS db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1;", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY name)", nil, nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)

	_, _, err = engine.Exec("CREATE TABLE table1 (name VARCHAR[512], PRIMARY KEY name)", nil, nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)

	_, _, err = engine.Exec("CREATE TABLE table1 (name VARCHAR[32], PRIMARY KEY name)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table2 (id INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, nil)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS table1 (id INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS blob_table (id BLOB[2], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)
}

func TestTimestampType(t *testing.T) {
	st, err := store.Open("timestamp", store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()
	defer os.RemoveAll("timestamp")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS timestamp_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "db1", "timestamp_table", "ts")

	t.Run("must accept NOW() as a timestamp", func(t *testing.T) {
		tsBefore := time.Now().UTC()

		_, _, err = engine.Exec("INSERT INTO timestamp_table(ts) VALUES(NOW())", nil, nil)
		require.NoError(t, err)

		tsAfter := time.Now().UTC()

		_, err := engine.InferParameters("SELECT ts FROM timestamp_table WHERE ts < 1 + NOW()", nil)
		require.ErrorIs(t, err, ErrInvalidTypes)

		r, err := engine.Query("SELECT ts FROM timestamp_table WHERE ts < NOW() ORDER BY id DESC LIMIT 1", nil, nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.False(t, tsBefore.After(row.ValuesBySelector[sel].Value().(time.Time)))
		require.False(t, tsAfter.Before(row.ValuesBySelector[sel].Value().(time.Time)))

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
	})

	t.Run("must accept time.Time as timestamp parameter", func(t *testing.T) {
		_, _, err = engine.Exec(
			"INSERT INTO timestamp_table(ts) VALUES(@ts)", map[string]interface{}{
				"ts": time.Date(2021, 12, 1, 18, 06, 14, 0, time.UTC),
			},
			nil,
		)
		require.NoError(t, err)

		r, err := engine.Query("SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil, nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Date(2021, 12, 1, 18, 06, 14, 0, time.UTC), row.ValuesBySelector[sel].Value())
	})

	t.Run("must correctly validate timestamp equality", func(t *testing.T) {
		_, _, err = engine.Exec(
			"INSERT INTO timestamp_table(ts) VALUES(@ts)", map[string]interface{}{
				"ts": time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC),
			},
			nil,
		)
		require.NoError(t, err)

		r, err := engine.Query("SELECT ts FROM timestamp_table WHERE ts = @ts ORDER BY id", map[string]interface{}{
			"ts": time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC),
		}, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC), row.ValuesBySelector[sel].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query("SELECT ts FROM timestamp_table WHERE ts = @ts ORDER BY id", map[string]interface{}{
			"ts": "2021-12-06 10:14",
		}, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNotComparableValues)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestTimestampIndex(t *testing.T) {
	st, err := store.Open("timestamp_index", store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()
	defer os.RemoveAll("timestamp_index")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS timestamp_index (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON timestamp_index(ts)", nil, nil)
	require.NoError(t, err)

	for i := 100; i > 0; i-- {
		_, _, err = engine.Exec("INSERT INTO timestamp_index(ts) VALUES(@ts)", map[string]interface{}{"ts": time.Unix(int64(i), 0)}, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query("SELECT * FROM timestamp_index ORDER BY ts", nil, nil)
	require.NoError(t, err)
	defer r.Close()

	for i := 100; i > 0; i-- {
		row, err := r.Read()
		require.NoError(t, err)
		require.EqualValues(t, i, row.ValuesBySelector[EncodeSelector("", "db1", "timestamp_index", "id")].Value())
	}

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)
}

func TestTimestampCasts(t *testing.T) {
	st, err := store.Open("timestamp_casts", store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()
	defer os.RemoveAll("timestamp_casts")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS timestamp_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "db1", "timestamp_table", "ts")

	for _, d := range []struct {
		str string
		t   time.Time
	}{
		{"2021-12-03 16:14:21.1234", time.Date(2021, 12, 03, 16, 14, 21, 123400000, time.UTC)},
		{"2021-12-03 16:14", time.Date(2021, 12, 03, 16, 14, 0, 0, time.UTC)},
		{"2021-12-03", time.Date(2021, 12, 03, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(fmt.Sprintf("insert a timestamp value using a cast from '%s'", d.str), func(t *testing.T) {
			_, _, err = engine.Exec(
				fmt.Sprintf("INSERT INTO timestamp_table(ts) VALUES(CAST('%s' AS TIMESTAMP))", d.str), nil, nil)
			require.NoError(t, err)

			r, err := engine.Query("SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil, nil)
			require.NoError(t, err)
			defer r.Close()

			row, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
			require.Equal(t, d.t, row.ValuesBySelector[sel].Value())
		})
	}

	t.Run("insert a timestamp value using a cast from INTEGER", func(t *testing.T) {
		_, _, err = engine.Exec(
			"INSERT INTO timestamp_table(ts) VALUES(CAST(123456 AS TIMESTAMP))", nil, nil)
		require.NoError(t, err)

		r, err := engine.Query("SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil, nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Unix(123456, 0).UTC(), row.ValuesBySelector[sel].Value())
	})

	t.Run("test casting from null values", func(t *testing.T) {
		_, _, err = engine.Exec(
			`
			CREATE TABLE IF NOT EXISTS values_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, str VARCHAR, i INTEGER, PRIMARY KEY id);
			INSERT INTO values_table(ts, str,i) VALUES(NOW(), NULL, NULL);
		`, nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			`
			UPDATE values_table SET ts = CAST(str AS TIMESTAMP);
		`, nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			`
			UPDATE values_table SET ts = CAST(i AS TIMESTAMP);
		`, nil, nil)
		require.NoError(t, err)
	})

	t.Run("test casting invalid string", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO timestamp_table(ts) VALUES(CAST('not a datetime' AS TIMESTAMP))", nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
		require.Contains(t, err.Error(), "can not cast")

		_, _, err = engine.Exec("INSERT INTO timestamp_table(ts) VALUES(CAST(@ts AS TIMESTAMP))", map[string]interface{}{
			"ts": strings.Repeat("long string ", 1000),
		}, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
		require.Less(t, len(err.Error()), 100)
		require.Contains(t, err.Error(), "can not cast")
		require.Contains(t, err.Error(), "...")
	})

}

func TestAddColumn(t *testing.T) {
	st, err := store.Open("sqldata_add_column", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_add_column")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(name, surname) VALUES('John', 'Smith')", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN int INTEGER AUTO_INCREMENT", nil, nil)
	require.ErrorIs(t, err, ErrLimitedAutoIncrement)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN surname VARCHAR NOT NULL", nil, nil)
	require.ErrorIs(t, err, ErrNewColumnMustBeNullable)

	_, _, err = engine.Exec("ALTER TABLE table2 ADD COLUMN surname VARCHAR", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN value INTEGER[100]", nil, nil)
	require.ErrorIs(t, err, ErrLimitedMaxLen)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil, nil)
	require.ErrorIs(t, err, ErrColumnAlreadyExists)

	_, _, err = engine.Exec("INSERT INTO table1(name, surname) VALUES('John', 'Smith')", nil, nil)
	require.NoError(t, err)

	res, err := engine.Query("SELECT id, name, surname FROM table1", nil, nil)
	require.NoError(t, err)

	row, err := res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 1, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "John", row.ValuesByPosition[1].Value())
	require.EqualValues(t, "Smith", row.ValuesByPosition[2].Value())

	_, err = res.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = res.Close()
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)

	// Reopen store
	st, err = store.Open("sqldata_add_column", store.DefaultOptions())
	require.NoError(t, err)

	engine, err = NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
	require.NoError(t, err)

	res, err = engine.Query("SELECT id, name, surname FROM table1", nil, nil)
	require.NoError(t, err)

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 1, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "John", row.ValuesByPosition[1].Value())
	require.EqualValues(t, "Smith", row.ValuesByPosition[2].Value())

	_, err = res.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = res.Close()
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)
}

func TestRenameColumn(t *testing.T) {
	st, err := store.Open("sqldata_rename_column", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_rename_column")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("ALTER TABLE table1 RENAME COLUMN name TO surname", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR[50], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(name) VALUES('John'), ('Sylvia'), ('Robocop') ", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("ALTER TABLE table1 RENAME COLUMN name TO name", nil, nil)
	require.ErrorIs(t, err, ErrSameOldAndNewColumnName)

	_, _, err = engine.Exec("ALTER TABLE table1 RENAME COLUMN name TO id", nil, nil)
	require.ErrorIs(t, err, ErrColumnAlreadyExists)

	_, _, err = engine.Exec("ALTER TABLE table2 RENAME COLUMN name TO surname", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec("ALTER TABLE table1 RENAME COLUMN surname TO name", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("ALTER TABLE table1 RENAME COLUMN name TO surname", nil, nil)
	require.NoError(t, err)

	res, err := engine.Query("SELECT id, surname FROM table1 ORDER BY surname", nil, nil)
	require.NoError(t, err)

	row, err := res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 1, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "John", row.ValuesByPosition[1].Value())

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 3, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "Robocop", row.ValuesByPosition[1].Value())

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 2, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "Sylvia", row.ValuesByPosition[1].Value())

	_, err = res.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = res.Close()
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)

	// Reopen store
	st, err = store.Open("sqldata_rename_column", store.DefaultOptions())
	require.NoError(t, err)

	engine, err = NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
	require.NoError(t, err)

	res, err = engine.Query("SELECT id, surname FROM table1 ORDER BY surname", nil, nil)
	require.NoError(t, err)

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 1, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "John", row.ValuesByPosition[1].Value())

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 3, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "Robocop", row.ValuesByPosition[1].Value())

	row, err = res.Read()
	require.NoError(t, err)

	require.EqualValues(t, 2, row.ValuesByPosition[0].Value())
	require.EqualValues(t, "Sylvia", row.ValuesByPosition[1].Value())

	_, err = res.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = res.Close()
	require.NoError(t, err)

	err = st.Close()
	require.NoError(t, err)
}

func TestCreateIndex(t *testing.T) {
	st, err := store.Open("sqldata_create_index", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_create_index")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, name VARCHAR[256], age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX IF NOT EXISTS ON table1(name)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(id)", nil, nil)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX IF NOT EXISTS ON table1(id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(age)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.Equal(t, ErrIndexAlreadyExists, err)

	_, _, err = engine.Exec("CREATE INDEX ON table2(name)", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("INSERT INTO table1(id, name, age) VALUES (1, 'name1', 50)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(name, age) VALUES ('name2', 10)", nil, nil)
	require.ErrorIs(t, err, ErrPKCanNotBeNull)

	_, _, err = engine.Exec("CREATE INDEX ON table1(active)", nil, nil)
	require.Equal(t, ErrLimitedIndexCreation, err)
}

func TestUpsertInto(t *testing.T) {
	st, err := store.Open("sqldata_upsert", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_upsert")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
								id INTEGER,
								title VARCHAR,
								amount INTEGER,
								active BOOLEAN NOT NULL,
								PRIMARY KEY id)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(active)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX ON table1(amount, active)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil, nil)
	require.ErrorIs(t, err, ErrNotNullableColumnCannotBeNull)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, age) VALUES (1, 50)", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", nil, nil)
	require.ErrorIs(t, err, ErrMissingParameter)

	params := make(map[string]interface{}, 1)
	params["id"] = [4]byte{1, 2, 3, 4}
	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", params, nil)
	require.Equal(t, ErrUnsupportedParameter, err)

	params = make(map[string]interface{}, 1)
	params["id"] = []byte{1, 2, 3}
	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", params, nil)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Contains(t, err.Error(), "is not an integer")

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (1, @title, false)", nil, nil)
	require.ErrorIs(t, err, ErrMissingParameter)

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (1, @title, true)", params, nil)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Contains(t, err.Error(), "is not a string")

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	params["Title"] = uint64(2)
	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (1, @title, true)", params, nil)
	require.Equal(t, ErrDuplicatedParameters, err)

	_, ctxs, err := engine.Exec("UPSERT INTO table1 (id, amount, active) VALUES (1, 10, true)", nil, nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.Equal(t, ctxs[0].UpdatedRows(), 1)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, amount, active) VALUES (2, 10, true)", nil, nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	t.Run("row with pk 1 should have active in false", func(t *testing.T) {
		_, ctxs, err = engine.Exec("UPSERT INTO table1 (id, amount, active) VALUES (1, 20, false)", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, ctxs[0].UpdatedRows(), 1)

		r, err := engine.Query("SELECT amount, active FROM table1 WHERE id = 1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)
		require.Equal(t, int64(20), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "amount")].Value())
		require.False(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value().(bool))
		require.Len(t, row.ValuesByPosition, 2)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[EncodeSelector("", "db1", "table1", "amount")])
		require.Equal(t, row.ValuesByPosition[1], row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")])

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("row with pk 1 should have active in true", func(t *testing.T) {
		_, ctxs, err = engine.Exec("UPSERT INTO table1 (id, amount, active) VALUES (1, 10, true)", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, ctxs[0].UpdatedRows(), 1)

		r, err := engine.Query("SELECT amount, active FROM table1 WHERE id = 1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)
		require.Equal(t, int64(10), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "amount")].Value())
		require.True(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value().(bool))

		err = r.Close()
		require.NoError(t, err)
	})

	_, _, err = engine.Exec("UPSERT INTO table1 (Id, Title, Active) VALUES (1, 'some title', false)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (Id, Title, Amount, Active) VALUES (1, 'some title', 100, false)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, amount, active) VALUES (2, 'another title', 200, true)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id) VALUES (1, 'yat')", nil, nil)
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, id) VALUES (1, 2)", nil, nil)
	require.ErrorIs(t, err, ErrDuplicatedColumn)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, active) VALUES ('1', true)", nil, nil)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Contains(t, err.Error(), "is not an integer")

	_, _, err = engine.Exec("UPSERT INTO table1 (id, active) VALUES (NULL, false)", nil, nil)
	require.Equal(t, ErrPKCanNotBeNull, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title, active) VALUES (2, NULL, true)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (title, active) VALUES ('interesting title', true)", nil, nil)
	require.Equal(t, ErrPKCanNotBeNull, err)

	_, _, err = engine.Exec("CREATE TABLE IF NOT EXISTS blob_table (id BLOB[2], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)
}

func TestInsertIntoEdgeCases(t *testing.T) {
	st, err := store.Open("sqldata_insert", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_insert")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, payload BLOB[2], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1 (title)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1 (active)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1 (payload)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1')", nil, nil)
	require.NoError(t, err)

	t.Run("on conflict cases", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1')", nil, nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

		ntx, ctxs, err := engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1') ON CONFLICT DO NOTHING", nil, nil)
		require.NoError(t, err)
		require.Nil(t, ntx)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
		require.Nil(t, ctxs[0].TxHeader())
	})

	t.Run("varchar key cases", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title123456789', true, x'00A1')", nil, nil)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)

		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (2, 10, true, '00A1')", nil, nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("boolean key cases", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', 'true', x'00A1')", nil, nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("blob key cases", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', true, x'00A100A2')", nil, nil)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)

		_, _, err = engine.Exec("INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', true, '00A100A2')", nil, nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("insertion in table with varchar pk", func(t *testing.T) {
		_, _, err = engine.Exec("CREATE TABLE languages (code VARCHAR[255],name VARCHAR[255],PRIMARY KEY code)", nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec("INSERT INTO languages (code,name) VALUES ('code1', 'name1')", nil, nil)
		require.NoError(t, err)
	})
}

func TestAutoIncrementPK(t *testing.T) {
	st, err := store.Open("sqldata_auto_inc", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_auto_inc")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	t.Run("invalid use of auto-increment", func(t *testing.T) {
		_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR AUTO_INCREMENT, PRIMARY KEY id)", nil, nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER AUTO_INCREMENT, PRIMARY KEY id)", nil, nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, _, err = engine.Exec("CREATE TABLE table1 (id VARCHAR AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id)", nil, nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)
	})

	_, _, err = engine.Exec(`
			CREATE TABLE table1 (
				id INTEGER NOT NULL AUTO_INCREMENT,
				title VARCHAR,
				active BOOLEAN,
				PRIMARY KEY id
			)
	`, nil, nil)
	require.NoError(t, err)

	_, ctxs, err := engine.Exec("INSERT INTO table1(title) VALUES ('name1')", nil, nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].closed)
	require.Equal(t, int64(1), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, int64(1), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, 1, ctxs[0].UpdatedRows())

	_, _, err = engine.Exec("INSERT INTO table1(id, title) VALUES (1, 'name2')", nil, nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	_, _, err = engine.Exec("INSERT INTO table1(id, title) VALUES (1, 'name2') ON CONFLICT DO NOTHING", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1(id, title) VALUES (1, 'name11')", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(id, title) VALUES (3, 'name3')", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1(id, title) VALUES (5, 'name5')", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(id, title) VALUES (2, 'name2')", nil, nil)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = engine.Exec("UPSERT INTO table1(id, title) VALUES (2, 'name2')", nil, nil)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = engine.Exec("UPSERT INTO table1(id, title) VALUES (3, 'name33')", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1(id, title) VALUES (5, 'name55')", nil, nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	_, ctxs, err = engine.Exec("INSERT INTO table1(title) VALUES ('name6')", nil, nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].closed)
	require.Equal(t, int64(6), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, int64(6), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, 1, ctxs[0].UpdatedRows())

	_, ctxs, err = engine.Exec(
		`
		BEGIN TRANSACTION;
			INSERT INTO table1(title) VALUES ('name7');
			INSERT INTO table1(title) VALUES ('name8');
		COMMIT;
	`, nil, nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].closed)
	require.Equal(t, int64(7), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, int64(8), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, 2, ctxs[0].UpdatedRows())
}

func TestDelete(t *testing.T) {
	st, err := store.Open("sqldata_delete", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_delete")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("DELETE FROM table1", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
		id INTEGER,
		title VARCHAR[50],
		active BOOLEAN,
		PRIMARY KEY id
	)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(active)", nil, nil)
	require.NoError(t, err)

	params, err := engine.InferParameters("DELETE FROM table1 WHERE active = @active", nil)
	require.NoError(t, err)
	require.NotNil(t, params)
	require.Len(t, params, 1)
	require.Equal(t, params["active"], BooleanType)

	_, _, err = engine.Exec("DELETE FROM table2", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec("DELETE FROM table1 WHERE name = 'name1'", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	t.Run("delete on empty table should complete without issues", func(t *testing.T) {
		_, ctxs, err := engine.Exec("DELETE FROM table1", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)`, i, i, i%2 == 0), nil, nil)
		require.NoError(t, err)
	}

	t.Run("deleting with contradiction should not produce any change", func(t *testing.T) {
		_, ctxs, err := engine.Exec("DELETE FROM table1 WHERE false", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	t.Run("deleting active rows should remove half of the rows", func(t *testing.T) {
		_, ctxs, err := engine.Exec("DELETE FROM table1 WHERE active = @active", map[string]interface{}{"active": true}, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, rowCount/2, ctxs[0].UpdatedRows())

		r, err := engine.Query("SELECT COUNT(*) FROM table1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.Len(t, row.ValuesBySelector, 1)
		require.Equal(t, int64(rowCount/2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")].Value())
		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")])

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query("SELECT COUNT(*) FROM table1 WHERE active", nil, nil)
		require.NoError(t, err)

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")].Value())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestErrorDuringDelete(t *testing.T) {
	st, err := store.Open("err_during_delete", store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()
	defer os.RemoveAll("err_during_delete")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		create table mytable(name varchar[30], primary key name);
		insert into mytable(name) values('name1');
	`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("delete FROM mytable where name=name1", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("delete FROM mytable where name='name1'", nil, nil)
	require.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	st, err := store.Open("sqldata_update", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_update")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("UPDATE table1 SET title = 'title11' WHERE title = 'title'", nil, nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
		id INTEGER,
		title VARCHAR[50],
		active BOOLEAN,
		PRIMARY KEY id
	)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(active)", nil, nil)
	require.NoError(t, err)

	params, err := engine.InferParameters("UPDATE table1 SET active = @active", nil)
	require.NoError(t, err)
	require.NotNil(t, params)
	require.Len(t, params, 1)
	require.Equal(t, params["active"], BooleanType)

	_, _, err = engine.Exec("UPDATE table2 SET active = false", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec("UPDATE table1 SET name = 'name1'", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	t.Run("update on empty table should complete without issues", func(t *testing.T) {
		_, ctxs, err := engine.Exec("UPDATE table1 SET active = false", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)`, i, i, i%2 == 0), nil, nil)
		require.NoError(t, err)
	}

	t.Run("updating with contradiction should not produce any change", func(t *testing.T) {
		_, ctxs, err := engine.Exec("UPDATE table1 SET active = false WHERE false", nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	t.Run("updating specific row should update only one row", func(t *testing.T) {
		_, ctxs, err := engine.Exec("UPDATE table1 SET active = true WHERE title = @title", map[string]interface{}{"title": "title1"}, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, 1, ctxs[0].UpdatedRows())

		r, err := engine.Query("SELECT COUNT(*) FROM table1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, int64(rowCount), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")].Value())

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query("SELECT COUNT(*) FROM table1 WHERE active", nil, nil)
		require.NoError(t, err)

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, int64(rowCount/2+1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")].Value())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestTransactions(t *testing.T) {
	st, err := store.Open("sqldata_tx", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_tx")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
									id INTEGER,
									title VARCHAR,
									PRIMARY KEY id
								)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		COMMIT;
		`, nil, nil)
	require.ErrorIs(t, err, ErrNoOngoingTx)

	_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			CREATE INDEX ON table2(title);
		COMMIT;
		`, nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT;
		`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			CREATE TABLE table2 (id INTEGER, title VARCHAR[100], age INTEGER, PRIMARY KEY id);
			CREATE INDEX ON table2(title);
		COMMIT;
		`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			CREATE INDEX ON table2(age);
			INSERT INTO table2 (id, title, age) VALUES (1, 'title1', 40);
		COMMIT;
		`, nil, nil)
	require.NoError(t, err)
}

func TestTransactionsEdgeCases(t *testing.T) {
	st, err := store.Open("sqldata_tx_edge_cases", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_tx_edge_cases")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithAutocommit(true))
	require.NoError(t, err)

	t.Run("nested tx are not supported", func(t *testing.T) {
		_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			BEGIN TRANSACTION;
				CREATE TABLE table1 (
					id INTEGER,
					title VARCHAR,
					PRIMARY KEY id
				);
			COMMIT;
		COMMIT;
		`, nil, nil)
		require.ErrorIs(t, err, ErrNestedTxNotSupported)
	})

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		CREATE TABLE table1 (
			id INTEGER,
			title VARCHAR,
			PRIMARY KEY id
		)`, nil, nil)
	require.NoError(t, err)

	t.Run("rollback without explicit transaction should return error", func(t *testing.T) {
		_, _, err = engine.Exec(`
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			ROLLBACK;
		`, nil, nil)
		require.ErrorIs(t, err, ErrNoOngoingTx)
	})

	t.Run("auto-commit should automatically commit ongoing tx", func(t *testing.T) {
		ntx, ctxs, err := engine.Exec(`
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		`, nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 2)
		require.Nil(t, ntx)
	})

	t.Run("explicit tx initialization should automatically commit ongoing tx", func(t *testing.T) {
		engine.autocommit = false

		ntx, ctxs, err := engine.Exec(`
			UPSERT INTO table1 (id, title) VALUES (3, 'title3');
			BEGIN TRANSACTION;
				UPSERT INTO table1 (id, title) VALUES (4, 'title4');
			COMMIT;
		`, nil, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 2)
		require.Nil(t, ntx)
	})
}

func TestUseSnapshot(t *testing.T) {
	st, err := store.Open("sqldata_snap", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_snap")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE SNAPSHOT SINCE TX 1", nil, nil)
	require.Equal(t, ErrNoSupported, err)

	_, _, err = engine.Exec(`
		BEGIN TRANSACTION;
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT;
		`, nil, nil)
	require.NoError(t, err)
}

func TestEncodeRawValue(t *testing.T) {
	b, err := EncodeValue(int64(1), IntegerType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue(true, IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(true, BooleanType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeValue(uint64(1), BooleanType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue("title", VarcharType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeValue(uint64(1), VarcharType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue([]byte{}, BLOBType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue(nil, BLOBType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue(uint64(1), BLOBType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(uint64(1), "invalid type", 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeValue("012345678901234567890123456789012", VarcharType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue("01234567890123456789012345678902", VarcharType, 0)
	require.NoError(t, err)

	_, err = EncodeValue("012345678901234567890123456789012", VarcharType, 0)
	require.NoError(t, err)

	b, err = EncodeValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}, BLOBType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
	}, BLOBType, 0)
	require.NoError(t, err)

	_, err = EncodeValue([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}, BLOBType, 0)
	require.NoError(t, err)
}

func TestEncodeValue(t *testing.T) {
	b, err := EncodeValue((&Number{val: 1}).Value(), IntegerType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue((&Bool{val: true}).Value(), IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Bool{val: true}).Value(), BooleanType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeValue((&Number{val: 1}).Value(), BooleanType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Varchar{val: "title"}).Value(), VarcharType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeValue((&Number{val: 1}).Value(), VarcharType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Blob{val: []byte{}}).Value(), BLOBType, 50)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue((&Blob{val: nil}).Value(), BLOBType, 50)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue((&Number{val: 1}).Value(), BLOBType, 50)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Number{val: 1}).Value(), "invalid type", 50)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeValue((&Varchar{val: "012345678901234567890123456789012"}).Value(), VarcharType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue((&Varchar{val: "01234567890123456789012345678902"}).Value(), VarcharType, 256)
	require.NoError(t, err)

	_, err = EncodeValue((&Varchar{val: "012345678901234567890123456789012"}).Value(), VarcharType, 256)
	require.NoError(t, err)

	b, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}).Value(), BLOBType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
	}}).Value(), BLOBType, 256)
	require.NoError(t, err)

	_, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}).Value(), BLOBType, 256)
	require.NoError(t, err)

	b, err = EncodeValue((&Timestamp{val: time.Unix(0, 1000)}).Value(), TimestampType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue((&Number{val: 1}).Value(), TimestampType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)
}

func TestQuery(t *testing.T) {
	st, err := store.Open("sqldata_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_q")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, err = engine.Query("SELECT id FROM table1", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, err = engine.Query("SELECT id FROM table1", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, err = engine.Query("SELECT * FROM table1", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	_, _, err = engine.Exec("SELECT id FROM table1", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, err = engine.Query("SELECT id FROM table1", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
								id INTEGER,
								ts TIMESTAMP,
								title VARCHAR,
								active BOOLEAN,
								payload BLOB,
								PRIMARY KEY id)`, nil, nil)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["id"] = 0

	r, err := engine.Query("SELECT id FROM table1 WHERE id >= @id", nil, nil)
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

	r, err = engine.Query("SELECT * FROM table1", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	rowCount := 10

	start := time.Now()

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(fmt.Sprintf(`
			UPSERT INTO table1 (id, ts, title, active, payload)
			VALUES (%d, NOW(), 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, nil)
		require.NoError(t, err)
	}

	t.Run("should resolve every row", func(t *testing.T) {
		r, err = engine.Query("SELECT * FROM table1", nil, nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector()
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)

		require.Equal(t, "db1", r.Database())
		require.Equal(t, "table1", r.TableAlias())

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "db1", "table1", "ts")].Value().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "payload")].Value())
		}

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should fail reading due to non-existent column", func(t *testing.T) {
		r, err := engine.Query("SELECT id1 FROM table1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.ErrorIs(t, err, ErrColumnDoesNotExist)
		require.Nil(t, row)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with two-time table aliasing", func(t *testing.T) {
		r, err = engine.Query(fmt.Sprintf(`
			SELECT * FROM table1 AS mytable1 WHERE mytable1.id >= 0 LIMIT %d
		`, rowCount), nil, nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector()
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)

		require.Equal(t, "db1", r.Database())
		require.Equal(t, "mytable1", r.TableAlias())

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "ts")].Value().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "id")].Value())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "title")].Value())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "active")].Value())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "payload")].Value())
		}

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with column and two-time table aliasing", func(t *testing.T) {
		r, err = engine.Query(fmt.Sprintf(`
			SELECT mytable1.id AS D, ts, Title, payload, Active FROM table1 mytable1 WHERE mytable1.id >= 0 LIMIT %d
		`, rowCount), nil, nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector()
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)

		require.Equal(t, "db1", r.Database())
		require.Equal(t, "mytable1", r.TableAlias())

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "ts")].Value().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "d")].Value())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "title")].Value())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "active")].Value())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "db1", "mytable1", "payload")].Value())
		}

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	r, err = engine.Query("SELECT id, title, active, payload FROM table1 ORDER BY title", nil, nil)
	require.Equal(t, ErrLimitedOrderBy, err)
	require.Nil(t, r)

	r, err = engine.Query("SELECT Id, Title, Active, payload FROM Table1 ORDER BY Id DESC", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)

		require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, (rowCount-1-i)%2 == 0, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value())

		encPayload := []byte(fmt.Sprintf("blob%d", rowCount-1-i))
		require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "payload")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id FROM table1 WHERE id", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param1"] = true

	r, err = engine.Query("SELECT id FROM table1 WHERE active = @some_param1", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrMissingParameter)

	r.SetParameters(params)

	row, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, int64(2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param"] = true

	encPayloadPrefix := hex.EncodeToString([]byte("blob"))

	r, err = engine.Query(fmt.Sprintf(`
		SELECT id, title, active
		FROM table1
		WHERE active = @some_param AND title > 'title' AND payload >= x'%s' AND title LIKE 't'`, encPayloadPrefix), params, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i += 2 {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, params["some_param"], row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT * FROM table1 WHERE id = 0", nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 5)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE id / 0", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrDivisionByZero)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE id + 1/1 > 1 * (1 - 0)", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE id = 0 AND NOT active OR active", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("INVALID QUERY", nil, nil)
	require.ErrorIs(t, err, ErrParsingError)
	require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")
	require.Nil(t, r)

	r, err = engine.Query("UPSERT INTO table1 (id) VALUES(1)", nil, nil)
	require.ErrorIs(t, err, ErrExpectingDQLStmt)
	require.Nil(t, r)

	r, err = engine.Query("UPSERT INTO table1 (id) VALUES(1); UPSERT INTO table1 (id) VALUES(1)", nil, nil)
	require.ErrorIs(t, err, ErrExpectingDQLStmt)
	require.Nil(t, r)

	r, err = engine.QueryPreparedStmt(nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, r)

	params = make(map[string]interface{})
	params["null_param"] = nil

	r, err = engine.Query("SELECT id FROM table1 WHERE active = @null_param", params, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestQueryDistinct(t *testing.T) {
	st, err := store.Open("sqldata_qd", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_qd")

	opts := DefaultOptions().WithPrefix(sqlPrefix).WithDistinctLimit(4)
	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
								id INTEGER AUTO_INCREMENT,
								title VARCHAR,
								amount INTEGER,
								active BOOLEAN,
								PRIMARY KEY id)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`INSERT INTO table1 (title, amount, active) VALUES
								('title1', 100, NULL),
								('title2', 200, false),
								('title3', 200, true),
								('title4', 300, NULL)`, nil, nil)
	require.NoError(t, err)

	t.Run("should return all titles", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query("SELECT DISTINCT title FROM table1 WHERE id <= @id", nil, nil)
		require.NoError(t, err)

		r.SetParameters(params)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(db1.table1.title)", cols[0].Selector())

		for i := 1; i <= 3; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(db1.table1.title)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return two titles", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query("SELECT DISTINCT title FROM table1 WHERE id <= @id LIMIT 2", nil, nil)
		require.NoError(t, err)

		r.SetParameters(params)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(db1.table1.title)", cols[0].Selector())

		for i := 1; i <= 2; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(db1.table1.title)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return two distinct amounts", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query("SELECT DISTINCT amount FROM table1 WHERE id <= @id", params, nil)
		require.NoError(t, err)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(db1.table1.amount)", cols[0].Selector())

		for i := 1; i <= 2; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, int64(i*100), row.ValuesBySelector["(db1.table1.amount)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return rows with null, false and true", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query("SELECT DISTINCT active FROM table1 WHERE id <= @id", params, nil)
		require.NoError(t, err)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(db1.table1.active)", cols[0].Selector())

		for i := 0; i <= 2; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)

			if i == 0 {
				require.Nil(t, row.ValuesBySelector["(db1.table1.active)"].Value())
				continue
			}

			require.Equal(t, i == 2, row.ValuesBySelector["(db1.table1.active)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return three rows", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query("SELECT DISTINCT amount, active FROM table1 WHERE id <= @id", params, nil)
		require.NoError(t, err)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 2)
		require.Equal(t, "(db1.table1.amount)", cols[0].Selector())
		require.Equal(t, "(db1.table1.active)", cols[1].Selector())

		for i := 0; i <= 2; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 2)

			if i == 0 {
				require.Equal(t, int64(100), row.ValuesBySelector["(db1.table1.amount)"].Value())
				require.Nil(t, row.ValuesBySelector["(db1.table1.active)"].Value())
				continue
			}

			require.Equal(t, int64(200), row.ValuesBySelector["(db1.table1.amount)"].Value())
			require.Equal(t, i == 2, row.ValuesBySelector["(db1.table1.active)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return too many rows error", func(t *testing.T) {
		r, err := engine.Query("SELECT DISTINCT id FROM table1", nil, nil)
		require.NoError(t, err)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(db1.table1.id)", cols[0].Selector())

		for i := 0; i < engine.distinctLimit; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)

			require.Equal(t, int64(i+1), row.ValuesBySelector["(db1.table1.id)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrTooManyRows)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestIndexing(t *testing.T) {
	st, err := store.Open("sqldata_indexing", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_indexing")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`CREATE TABLE table1 (
								id INTEGER AUTO_INCREMENT,
								ts INTEGER,
								title VARCHAR[20],
								active BOOLEAN,
								amount INTEGER,
								payload BLOB,
								PRIMARY KEY id
							)`, nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1 (ts)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX ON table1 (title, amount)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1 (active, title)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE UNIQUE INDEX ON table1 (title)", nil, nil)
	require.NoError(t, err)

	t.Run("should fail due to unique index", func(t *testing.T) {
		_, _, err = engine.Exec("INSERT INTO table1 (ts, title, amount, active) VALUES (1, 'title1', 10, true), (2, 'title1', 10, false)", nil, nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)
	})

	t.Run("should fail due non-available index", func(t *testing.T) {
		_, err = engine.Query("SELECT * FROM table1 ORDER BY amount DESC", nil, nil)
		require.ErrorIs(t, err, ErrNoAvailableIndex)
	})

	t.Run("should use primary index by default", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "id", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.True(t, scanSpecs.Index.IsPrimary())
		require.Empty(t, scanSpecs.rangesByColID)
		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use primary index in descending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 ORDER BY id DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "id", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.True(t, scanSpecs.Index.IsPrimary())
		require.Empty(t, scanSpecs.rangesByColID)
		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` ascending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 ORDER BY ts", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Empty(t, scanSpecs.rangesByColID)
		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` descending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 ORDER BY ts DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Empty(t, scanSpecs.rangesByColID)
		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE ts = 1629902962 OR ts < 1629902963 ORDER BY ts", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		tsRange := scanSpecs.rangesByColID[2]
		require.Nil(t, tsRange.lRange)
		require.NotNil(t, tsRange.hRange)
		require.False(t, tsRange.hRange.inclusive)
		require.Equal(t, int64(1629902963), tsRange.hRange.val.Value())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 AS t WHERE t.ts = 1629902962 AND t.ts = 1629902963 ORDER BY t.ts", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		tsRange := scanSpecs.rangesByColID[2]
		require.NotNil(t, tsRange.lRange)
		require.True(t, tsRange.lRange.inclusive)
		require.Equal(t, int64(1629902963), tsRange.lRange.val.Value())
		require.NotNil(t, tsRange.hRange)
		require.True(t, tsRange.hRange.inclusive)
		require.Equal(t, int64(1629902962), tsRange.hRange.val.Value())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE ts > 1629902962 AND ts < 1629902963 ORDER BY ts", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		tsRange := scanSpecs.rangesByColID[2]
		require.NotNil(t, tsRange.lRange)
		require.False(t, tsRange.lRange.inclusive)
		require.Equal(t, int64(1629902962), tsRange.lRange.val.Value())
		require.NotNil(t, tsRange.hRange)
		require.False(t, tsRange.hRange.inclusive)
		require.Equal(t, int64(1629902963), tsRange.hRange.val.Value())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title, amount` in asc order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title, amount) ORDER BY title", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 2)
		require.Equal(t, "title", orderBy[0].Column)
		require.Equal(t, "amount", orderBy[1].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 2)
		require.Empty(t, scanSpecs.rangesByColID)
		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` in asc order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title) ORDER BY title", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Empty(t, scanSpecs.rangesByColID)
		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` in default order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (ts)", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Empty(t, scanSpecs.rangesByColID)
		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should fail using index on `ts` when ordering by `title`", func(t *testing.T) {
		_, err := engine.Query("SELECT * FROM table1 USE INDEX ON (ts) ORDER BY title", nil, nil)
		require.ErrorIs(t, err, ErrNoAvailableIndex)
	})

	t.Run("should use index on `title` with max value in desc order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' ORDER BY title DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.Nil(t, titleRange.lRange)
		require.NotNil(t, titleRange.hRange)
		require.False(t, titleRange.hRange.inclusive)
		require.Equal(t, "title10", titleRange.hRange.val.Value())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title,amount` in desc order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE title = 'title1' ORDER BY amount DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 2)
		require.Equal(t, "title", orderBy[0].Column)
		require.Equal(t, "amount", orderBy[1].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 2)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.True(t, titleRange.lRange.inclusive)
		require.Equal(t, "title1", titleRange.lRange.val.Value())
		require.NotNil(t, titleRange.hRange)
		require.True(t, titleRange.hRange.inclusive)
		require.Equal(t, "title1", titleRange.hRange.val.Value())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` ascending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE title > 'title10' ORDER BY ts ASC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.False(t, titleRange.lRange.inclusive)
		require.Equal(t, "title10", titleRange.lRange.val.Value())
		require.Nil(t, titleRange.hRange)

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` descending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY ts DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "ts", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.False(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.True(t, titleRange.lRange.inclusive)
		require.Equal(t, "title1", titleRange.lRange.val.Value())
		require.Nil(t, titleRange.hRange)

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` descending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY title DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 2)
		require.Equal(t, "title", orderBy[0].Column)
		require.Equal(t, "amount", orderBy[1].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 2)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.True(t, titleRange.lRange.inclusive)
		require.Equal(t, "title1", titleRange.lRange.val.Value())
		require.Nil(t, titleRange.hRange)

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order starting with 'title1'", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title) WHERE title > 'title10' or title = 'title1' ORDER BY title", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.True(t, titleRange.lRange.inclusive)
		require.Equal(t, "title1", titleRange.lRange.val.Value())
		require.Nil(t, titleRange.hRange)

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' or title = 'title1' ORDER BY title", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.Nil(t, titleRange.lRange)
		require.NotNil(t, titleRange.hRange)
		require.False(t, titleRange.hRange.inclusive)
		require.Equal(t, "title10", titleRange.hRange.val.Value())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` descending order", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' and title = 'title1' ORDER BY title DESC", nil, nil)
		require.NoError(t, err)

		orderBy := r.OrderBy()
		require.NotNil(t, orderBy)
		require.Len(t, orderBy, 1)
		require.Equal(t, "title", orderBy[0].Column)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.NotNil(t, scanSpecs.Index)
		require.False(t, scanSpecs.Index.IsPrimary())
		require.True(t, scanSpecs.Index.IsUnique())
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Len(t, scanSpecs.rangesByColID, 1)

		titleRange := scanSpecs.rangesByColID[3]
		require.NotNil(t, titleRange.lRange)
		require.True(t, titleRange.lRange.inclusive)
		require.Equal(t, "title1", titleRange.lRange.val.Value())
		require.NotNil(t, titleRange.hRange)
		require.True(t, titleRange.hRange.inclusive)
		require.Equal(t, "title1", titleRange.hRange.val.Value())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestExecCornerCases(t *testing.T) {
	st, err := store.Open("sqldata_q", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_q")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	tx, _, err := engine.Exec("INVALID STATEMENT", nil, nil)
	require.ErrorIs(t, err, ErrParsingError)
	require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")
	require.Nil(t, tx)
}

func TestQueryWithNullables(t *testing.T) {
	st, err := store.Open("sqldata_nullable", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_nullable")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, ts TIMESTAMP, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1 (id, ts, title) VALUES (1, TIME(), 'title1')", nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	rowCount := 10

	start := time.Now()

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table1 (id, ts, title) VALUES (%d, NOW(), 'title%d')", i, i), nil, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query("SELECT id, ts, title, active FROM table1 WHERE NOT(active != NULL)", nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)
		require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "db1", "table1", "ts")].Value().(time.Time)))
		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, &NullValue{t: BooleanType}, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")])
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestOrderBy(t *testing.T) {
	st, err := store.Open("sqldata_orderby", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_orderby")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.Equal(t, ErrNoDatabaseSelected, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR[100], age INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, err = engine.Query("SELECT id, title, age FROM table1 ORDER BY id, title DESC", nil, nil)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.Query("SELECT id, title, age FROM (SELECT id, title, age FROM table1) ORDER BY id", nil, nil)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.Query("SELECT id, title, age FROM (SELECT id, title, age FROM table1 AS t1) ORDER BY age DESC", nil, nil)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, err = engine.Query("SELECT id, title, age FROM table2 ORDER BY title", nil, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.Query("SELECT id, title, age FROM table1 ORDER BY amount", nil, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	_, err = engine.Query("SELECT id, title, age FROM table1 ORDER BY age", nil, nil)
	require.Equal(t, ErrLimitedOrderBy, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(age)", nil, nil)
	require.NoError(t, err)

	params := make(map[string]interface{}, 1)
	params["age"] = nil
	_, _, err = engine.Exec("INSERT INTO table1 (id, title, age) VALUES (1, 'title', @age)", params, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1 (id, title) VALUES (2, 'title')", nil, nil)
	require.NoError(t, err)

	rowCount := 1

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i + 3
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = 40 + i

		_, _, err = engine.Exec("INSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query("SELECT id, title, age FROM table1 ORDER BY title", nil, nil)
	require.NoError(t, err)

	orderBy := r.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "title", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)
	require.Equal(t, "db1", orderBy[0].Database)

	row, err := r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, int64(i+3), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, int64(40+i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, age FROM table1 ORDER BY age", nil, nil)
	require.NoError(t, err)

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, int64(i+3), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, int64(40+i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, age FROM table1 ORDER BY age DESC", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, int64(rowCount-1-i+3), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		require.Equal(t, int64(40-(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())
	}

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	row, err = r.Read()
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 3)

	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
	require.Equal(t, "title", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
	require.Nil(t, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "age")].Value())

	err = r.Close()
	require.NoError(t, err)
}

func TestQueryWithRowFiltering(t *testing.T) {
	st, err := store.Open("sqldata_where", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_where")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query("SELECT id, title, active FROM table1 WHERE false", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE false OR true", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE 1 < 2", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE 1 >= 2", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE 1 = true", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE NOT table1.active", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM table1 WHERE table1.id > 4", nil, nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read()
		require.NoError(t, err)
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table1 (id, title) VALUES (%d, 'title%d')", rowCount, rowCount), nil, nil)
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title FROM table1 WHERE active = null AND payload = null", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title FROM table1 WHERE active = null AND payload = null AND active = payload", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = r.Close()
	require.NoError(t, err)
}

func TestQueryWithInClause(t *testing.T) {
	st, err := store.Open("sqldata_where_in", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_where_in")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR[50], active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)
		`, i, i, i%2 == 0), nil, nil)
		require.NoError(t, err)
	}

	inListExp := &InListExp{}
	require.False(t, inListExp.isConstant())

	t.Run("infer parameters without parameters should return an empty list", func(t *testing.T) {
		params, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE title IN ('title0', 'title1')", nil)
		require.NoError(t, err)
		require.Empty(t, params)
	})

	t.Run("infer inference with wrong types should return an error", func(t *testing.T) {
		_, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE 100 + title IN ('title0', 'title1')", nil)
		require.ErrorIs(t, err, ErrInvalidTypes)
	})

	t.Run("infer inference with valid types should succeed", func(t *testing.T) {
		params, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE active AND title IN ('title0', 'title1')", nil)
		require.NoError(t, err)
		require.Empty(t, params)
	})

	t.Run("infer parameters should return matching type", func(t *testing.T) {
		params, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE title IN (@param0, @param1)", nil)
		require.NoError(t, err)
		require.Len(t, params, 2)
		require.Equal(t, VarcharType, params["param0"])
		require.Equal(t, VarcharType, params["param1"])
	})

	t.Run("infer parameters with type conflicts should return an error", func(t *testing.T) {
		_, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE active = @param1 and title IN (@param0, @param1)", nil)
		require.ErrorIs(t, err, ErrInferredMultipleTypes)
	})

	t.Run("infer parameters with unexistent column should return an error", func(t *testing.T) {
		_, err := engine.InferParameters("SELECT id, title, active FROM table1 WHERE invalidColumn IN ('title1', 'title2')", nil)
		require.ErrorIs(t, err, ErrColumnDoesNotExist)
	})

	t.Run("in clause with invalid column should return an error", func(t *testing.T) {
		r, err := engine.Query("SELECT id, title, active FROM table1 WHERE invalidColumn IN (1, 2)", nil, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.ErrorIs(t, err, ErrColumnDoesNotExist)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause with invalid type should return an error", func(t *testing.T) {
		r, err := engine.Query("SELECT id, title, active FROM table1 WHERE title IN (1, 2)", nil, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNotComparableValues)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading two rows", func(t *testing.T) {
		r, err := engine.Query("SELECT id, title, active FROM table1 WHERE title IN ('title0', 'title1')", nil, nil)
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause with invalid values should return an error", func(t *testing.T) {
		r, err := engine.Query("SELECT id, title, active FROM table1 WHERE title IN ('title0', true + 'title1')", nil, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.ErrorIs(t, err, ErrInvalidValue)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading rows NOT included in 'IN' clause", func(t *testing.T) {
		r, err := engine.Query("SELECT id, title, active FROM table1 WHERE title NOT IN ('title1', 'title0')", nil, nil)
		require.NoError(t, err)

		for i := 2; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading using 'IN' clause in join condition", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM table1 as t1 INNER JOIN table1 as t2 ON t1.title IN (t2.title) ORDER BY title", nil, nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "title")].Value())
		}

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestAggregations(t *testing.T) {
	st, err := store.Open("sqldata_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_agg")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(age)", nil, nil)
	require.NoError(t, err)

	rowCount := 10
	base := 30

	for i := 1; i <= rowCount; i++ {
		params := make(map[string]interface{}, 3)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i

		_, _, err = engine.Exec("INSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query("SELECT COUNT(*) FROM table1 WHERE id < i", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id FROM table1 WHERE false", nil, nil)
	require.NoError(t, err)

	row, err := r.Read()
	require.Equal(t, ErrNoMoreRows, err)
	require.Nil(t, row)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(`
		SELECT COUNT(*), SUM(age), MIN(title), MAX(age), AVG(age), MIN(active), MAX(active), MIN(payload)
		FROM table1 WHERE false`, nil, nil)
	require.NoError(t, err)

	row, err = r.Read()
	require.NoError(t, err)
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col0")].Value())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col1")].Value())
	require.Equal(t, "", row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col2")].Value())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col3")].Value())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col4")].Value())

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT COUNT(*) AS c, SUM(age), MIN(age), MAX(age), AVG(age) FROM table1 AS t1", nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 5)

	require.Equal(t, int64(rowCount), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "c")].Value())

	require.Equal(t, int64((1+2*base+rowCount)*rowCount/2), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "col1")].Value())

	require.Equal(t, int64(1+base), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "col2")].Value())

	require.Equal(t, int64(base+rowCount), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "col3")].Value())

	require.Equal(t, int64(base+rowCount/2), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "col4")].Value())

	_, err = r.Read()
	require.Equal(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)
}

func TestCount(t *testing.T) {
	st, err := store.Open("sqldata_agg", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_agg")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE t1(id INTEGER AUTO_INCREMENT, val1 INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON t1(val1)", nil, nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			_, _, err = engine.Exec("INSERT INTO t1(val1) VALUES($1)", map[string]interface{}{"param1": j}, nil)
			require.NoError(t, err)
		}
	}

	r, err := engine.Query("SELECT COUNT(*) as c FROM t1", nil, nil)
	require.NoError(t, err)

	row, err := r.Read()
	require.NoError(t, err)
	require.EqualValues(t, uint64(30), row.ValuesBySelector["(db1.t1.c)"].Value())

	err = r.Close()
	require.NoError(t, err)

	_, err = engine.Query("SELECT COUNT(*) as c FROM t1 GROUP BY val1", nil, nil)
	require.ErrorIs(t, err, ErrLimitedGroupBy)

	r, err = engine.Query("SELECT COUNT(*) as c FROM t1 GROUP BY val1 ORDER BY val1", nil, nil)
	require.NoError(t, err)

	for j := 0; j < 3; j++ {
		row, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, uint64(10), row.ValuesBySelector["(db1.t1.c)"].Value())
	}

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestGroupByHaving(t *testing.T) {
	st, err := store.Open("sqldata_having", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_having")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(active)", nil, nil)
	require.NoError(t, err)

	rowCount := 10
	base := 40

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 4)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i
		params["active"] = i%2 == 0

		_, _, err = engine.Exec("INSERT INTO table1 (id, title, age, active) VALUES (@id, @title, @age, @active)", params, nil)
		require.NoError(t, err)
	}

	_, err = engine.Query("SELECT active, COUNT(*), SUM(age1) FROM table1 WHERE active != null HAVING AVG(age) >= MIN(age)", nil, nil)
	require.Equal(t, ErrHavingClauseRequiresGroupClause, err)

	r, err := engine.Query(`
		SELECT active, COUNT(*), SUM(age1)
		FROM table1
		WHERE active != null
		GROUP BY active
		HAVING AVG(age) >= MIN(age)
		ORDER BY active`, nil, nil)
	require.NoError(t, err)

	r.SetParameters(nil)

	_, err = r.Read()
	require.Equal(t, ErrColumnDoesNotExist, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(`
		SELECT active, COUNT(*), SUM(age1)
		FROM table1
		WHERE AVG(age) >= MIN(age)
		GROUP BY active
		ORDER BY active`, nil, nil)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT active, COUNT(id) FROM table1 GROUP BY active ORDER BY active", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.Equal(t, ErrLimitedCount, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(`
		SELECT active, COUNT(*)
		FROM table1
		GROUP BY active
		HAVING AVG(age) >= MIN(age1)
		ORDER BY active`, nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(`
		SELECT active, COUNT(*) as c, MIN(age), MAX(age), AVG(age), SUM(age)
		FROM table1
		GROUP BY active
		HAVING COUNT(*) <= SUM(age)   AND
				MIN(age) <= MAX(age) AND
				AVG(age) <= MAX(age) AND
				MAX(age) < SUM(age)  AND
				AVG(age) >= MIN(age) AND
				SUM(age) > 0
		ORDER BY active DESC`, nil, nil)

	require.NoError(t, err)

	_, err = r.Columns()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 6)

		require.Equal(t, i == 0, row.ValuesBySelector[EncodeSelector("", "db1", "table1", "active")].Value())

		require.Equal(t, int64(rowCount/2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "c")].Value())

		if i%2 == 0 {
			require.Equal(t, int64(base), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col2")].Value())
			require.Equal(t, int64(base+rowCount-2), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col3")].Value())
		} else {
			require.Equal(t, int64(base+1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col2")].Value())
			require.Equal(t, int64(base+rowCount-1), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "col3")].Value())
		}
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestJoins(t *testing.T) {
	st, err := store.Open("sqldata_innerjoin", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_innerjoin")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, fkid2 INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table2 (id INTEGER, amount INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)`, i, i, rowCount-1-i, i), nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table2 (id, amount) VALUES (%d, %d)", rowCount-1-i, i*i), nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, nil)
		require.NoError(t, err)
	}

	t.Run("should not find any matching row", func(t *testing.T) {
		r, err := engine.Query(`
		SELECT table1.title, table2.amount, table3.age
		FROM (SELECT * FROM table2 WHERE amount = 1)
		INNER JOIN table1 ON table2.id = table1.fkid1 AND (table2.amount > 0 OR table2.amount > 0+1)
		INNER JOIN table3 ON table1.fkid2 = table3.id AND table3.age < 30`, nil, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should find one matching row", func(t *testing.T) {
		r, err := engine.Query(`
		SELECT t1.title, t2.amount, t3.age
		FROM (SELECT id, amount FROM table2 WHERE amount = 1) AS t2
		INNER JOIN table1 AS t1 ON t2.id = t1.fkid1 AND t2.amount > 0
		INNER JOIN table3 AS t3 ON t1.fkid2 = t3.id AND t3.age > 30`, nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.Len(t, row.ValuesBySelector, 3)

		_, err = r.Read()
		require.Equal(t, ErrNoMoreRows, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every inserted row", func(t *testing.T) {
		r, err := engine.Query(`
			SELECT id, title, table2.amount, table3.age
			FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id
			INNER JOIN table3 ON table1.fkid2 = table3.id
			WHERE table1.id >= 0 AND table3.age >= 30
			ORDER BY id DESC`, nil, nil)
		require.NoError(t, err)

		r.SetParameters(nil)

		cols, err := r.Columns()
		require.NoError(t, err)
		require.Len(t, cols, 4)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 4)

			require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "id")].Value())
			require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "table1", "title")].Value())
			require.Equal(t, int64((rowCount-1-i)*(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "db1", "table2", "amount")].Value())
			require.Equal(t, int64(30+(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "db1", "table3", "age")].Value())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return error when joining nonexistent table", func(t *testing.T) {
		r, err := engine.Query(`
		SELECT title
		FROM table1
		INNER JOIN table22 ON table1.id = table11.fkid1`, nil, nil)
		require.NoError(t, err)

		_, err = r.Read()
		require.ErrorIs(t, err, ErrTableDoesNotExist)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestJoinsWithNullIndexes(t *testing.T) {
	st, err := store.Open("sqldata_join_nullable_index", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_join_nullable_index")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		CREATE TABLE table1 (id INTEGER, fkid2 INTEGER, PRIMARY KEY id);
		CREATE TABLE table2 (id INTEGER, id2 INTEGER, val INTEGER, PRIMARY KEY id);
		CREATE INDEX ON table2(id2);

		INSERT INTO table2(id, id2, val) VALUES (1, 1, 100), (2, null, 200);
		INSERT INTO table1(id, fkid2) VALUES (10, 1), (20, null);
	`, nil, nil)
	require.NoError(t, err)

	r, err := engine.Query(`
			SELECT table2.val
			FROM table1 INNER JOIN table2 ON table1.fkid2 = table2.id2
			ORDER BY table1.id`, nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 1)

	row, err := r.Read()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 1)
	require.EqualValues(t, 100, row.ValuesBySelector[EncodeSelector("", "db1", "table2", "val")].Value())

	row, err = r.Read()
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 1)
	require.EqualValues(t, 200, row.ValuesBySelector[EncodeSelector("", "db1", "table2", "val")].Value())

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestJoinsWithJointTable(t *testing.T) {
	st, err := store.Open("sqldata_innerjoin_joint", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_innerjoin_joint")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table2 (id INTEGER AUTO_INCREMENT, amount INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table12 (id INTEGER AUTO_INCREMENT, fkid1 INTEGER, fkid2 INTEGER, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table1 (name) VALUES ('name1'), ('name2'), ('name3')", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table2 (amount) VALUES (10), (20), (30)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table12 (fkid1, fkid2, active) VALUES (1,1,false),(1,2,true),(1,3,true)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table12 (fkid1, fkid2, active) VALUES (2,1,false),(2,2,false),(2,3,true)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("INSERT INTO table12 (fkid1, fkid2, active) VALUES (3,1,false),(3,2,false),(3,3,false)", nil, nil)
	require.NoError(t, err)

	r, err := engine.Query(`
		SELECT q.name, t2.amount, t12.active
		FROM (SELECT * FROM table1 where name = 'name1') q
		INNER JOIN table12 t12 on t12.fkid1 = q.id
		INNER JOIN table2 t2  on t12.fkid2 = t2.id
		WHERE t12.active = true`, nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 3)

	for i := 0; i < 2; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, "name1", row.ValuesBySelector[EncodeSelector("", "db1", "q", "name")].Value())
		require.Equal(t, int64(20+i*10), row.ValuesBySelector[EncodeSelector("", "db1", "t2", "amount")].Value())
		require.Equal(t, true, row.ValuesBySelector[EncodeSelector("", "db1", "t12", "active")].Value())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestNestedJoins(t *testing.T) {
	st, err := store.Open("sqldata_nestedjoins", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_nestedjoins")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table2 (id INTEGER, amount INTEGER, fkid1 INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1) VALUES (%d, 'title%d', %d)", i, i, rowCount-1-i), nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table2 (id, amount, fkid1) VALUES (%d, %d, %d)", rowCount-1-i, i*i, i), nil, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(`
		SELECT id, title, t2.amount AS total_amount, t3.age
		FROM table1 t1
		INNER JOIN table2 t2 ON (fkid1 = t2.id AND title != NULL)
		INNER JOIN table3 t3 ON t2.fkid1 = t3.id
		ORDER BY id DESC`, nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)

		require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "db1", "t1", "title")].Value())
		require.Equal(t, int64((rowCount-1-i)*(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "db1", "t2", "total_amount")].Value())
		require.Equal(t, int64(30+(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "db1", "t3", "age")].Value())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestReOpening(t *testing.T) {
	st, err := store.Open("sqldata_reopening", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_reopening")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, name VARCHAR[30], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.NoError(t, err)

	engine, err = NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, name VARCHAR[30], PRIMARY KEY id)", nil, nil)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	_, _, err = engine.Exec("CREATE INDEX ON table1(name)", nil, nil)
	require.ErrorIs(t, err, ErrIndexAlreadyExists)
}

func TestSubQuery(t *testing.T) {
	st, err := store.Open("sqldata_subq", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_subq")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil, nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(`
		SELECT id, title t
		FROM (SELECT id, title, active FROM table1) t2
		WHERE active AND t2.id >= 0`, nil, nil)
	require.NoError(t, err)

	cols, err := r.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 2)

	for i := 0; i < rowCount; i += 2 {
		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)

		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "db1", "t2", "id")].Value())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "db1", "t2", "t")].Value())
	}

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.Exec("UPSERT INTO table1 (id, title) VALUES (0, 'title0')", nil, nil)
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE active", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query("SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE title", nil, nil)
	require.NoError(t, err)

	_, err = r.Read()
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = r.Close()
	require.NoError(t, err)
}

func TestJoinsWithSubquery(t *testing.T) {
	st, err := store.Open("sqldata_subq", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_subq")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithAutocommit(true))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		CREATE TABLE IF NOT EXISTS customers (
			id            INTEGER,
			customer_name VARCHAR[60],
			email         VARCHAR[150],
			address       VARCHAR,
			city          VARCHAR,
			ip            VARCHAR[40],
			country       VARCHAR[15],
			age           INTEGER,
			active        BOOLEAN,
			PRIMARY KEY (id)
		);

		CREATE TABLE customer_review(
			customerid INTEGER,
			productid  INTEGER,
			review     VARCHAR,
			PRIMARY KEY (customerid, productid)
		);

		INSERT INTO customers (
			id, customer_name, email, address,
			city, ip, country, age, active
		)
		VALUES (
			1,
			'Isidro Behnen',
			'ibehnen0@mail.ru',
			'ibehnen0@chronoengine.com',
			'Arvika',
			'127.0.0.15',
			'SE',
			24,
			true
		);

		INSERT INTO customer_review (customerid, productid, review)
		VALUES(1, 1, 'Nice Juice!');
	`, nil, nil)
	require.NoError(t, err)

	r, err := engine.Query(`
		SELECT * FROM (
			SELECT id, customer_name, age
			FROM customers
			AS c
		)
		INNER JOIN (
			SELECT MAX(customerid) as customerid, COUNT(*) as review_count
			FROM customer_review
			AS r
		) ON r.customerid = c.id
		WHERE c.age < 30
		`,
		nil, nil)
	require.NoError(t, err)

	row, err := r.Read()
	require.NoError(t, err)

	require.Len(t, row.ValuesBySelector, 5)
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "c", "id")].Value())
	require.Equal(t, "Isidro Behnen", row.ValuesBySelector[EncodeSelector("", "db1", "c", "customer_name")].Value())
	require.Equal(t, int64(24), row.ValuesBySelector[EncodeSelector("", "db1", "c", "age")].Value())
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "r", "customerid")].Value())
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "db1", "r", "review_count")].Value())

	err = r.Close()
	require.NoError(t, err)
}

func TestInferParameters(t *testing.T) {
	st, err := store.Open("catalog_infer_params", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	stmt := "CREATE DATABASE db1"

	params, err := engine.InferParameters(stmt, nil)
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParametersPreparedStmts([]SQLStmt{&CreateDatabaseStmt{}}, nil)
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParameters(stmt, nil)
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParametersPreparedStmts([]SQLStmt{&CreateDatabaseStmt{}}, nil)
	require.NoError(t, err)
	require.Empty(t, params)

	_, _, err = engine.Exec(stmt, nil, nil)
	require.NoError(t, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (@id, @title);", nil)
	require.ErrorIs(t, err, ErrNoDatabaseSelected)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, err = engine.InferParameters("invalid sql stmt", nil)
	require.ErrorIs(t, err, ErrParsingError)
	require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")

	_, err = engine.InferParametersPreparedStmts(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	params, err = engine.InferParameters(stmt, nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("USE DATABASE db1", nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("USE SNAPSHOT BEFORE TX 10", nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	stmt = "CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"

	params, err = engine.InferParameters(stmt, nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	pstmt, err := Parse(strings.NewReader(stmt))
	require.NoError(t, err)
	require.Len(t, pstmt, 1)

	_, err = engine.InferParametersPreparedStmts(pstmt, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(stmt, nil, nil)
	require.NoError(t, err)

	params, err = engine.InferParameters("ALTER TABLE mytableSE ADD COLUMN note VARCHAR", nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("ALTER TABLE mytableSE RENAME COLUMN note TO newNote", nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	stmt = "CREATE INDEX ON mytable(active)"

	params, err = engine.InferParameters(stmt, nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.Exec(stmt, nil, nil)
	require.NoError(t, err)

	params, err = engine.InferParameters("BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@id, @title); COMMIT;", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, VarcharType, params["title"])

	params, err = engine.InferParameters("BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@id, @title); ROLLBACK;", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)

	params, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (1, 'title1')", nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (1, 'title1'), (@id2, @title2)", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id2"])
	require.Equal(t, VarcharType, params["title2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE (id - 1) > (@id + (@id+1))", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters("SELECT * FROM mytable t1 INNER JOIN mytable t2 ON t1.id = t2.id WHERE id > @id", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > @id AND (NOT @active OR active)", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, BooleanType, params["active"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > ? AND (NOT ? OR active)", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE id > $2 AND (NOT $1 OR active)", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, IntegerType, params["param2"])

	params, err = engine.InferParameters("SELECT COUNT(*) FROM mytable GROUP BY active HAVING @param1 = COUNT(*) ORDER BY active", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters("SELECT COUNT(*), MIN(id) FROM mytable GROUP BY active HAVING @param1 < MIN(id) ORDER BY active", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @active AND title LIKE 't+'", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["active"])

	params, err = engine.InferParameters("SELECT * FROM TABLES() WHERE name = @tablename", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, VarcharType, params["tablename"])

	params, err = engine.InferParameters("SELECT * FROM INDEXES('mytable') idxs WHERE idxs.\"unique\" = @unique", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["unique"])

	params, err = engine.InferParameters("SELECT * FROM COLUMNS('mytable') WHERE name = @column", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, VarcharType, params["column"])
}

func TestInferParametersPrepared(t *testing.T) {
	st, err := store.Open("catalog_infer_params_prepared", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_prepared")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	stmts, err := Parse(strings.NewReader("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	params, err := engine.InferParametersPreparedStmts(stmts, nil)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.ExecPreparedStmts(stmts, nil, nil)
	require.NoError(t, err)
}

func TestInferParametersUnbounded(t *testing.T) {
	st, err := store.Open("catalog_infer_params_unbounded", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_unbounded")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	params, err := engine.InferParameters("SELECT * FROM mytable WHERE @param1 = @param2", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, AnyType, params["param1"])
	require.Equal(t, AnyType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 AND @param2", nil)
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, AnyType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NOT NULL", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 AND active)", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters("SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 <= mytable.id)", nil)
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])
}

func TestInferParametersInvalidCases(t *testing.T) {
	st, err := store.Open("catalog_infer_params_invalid", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_infer_params_invalid")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (@param1, @param1)", nil)
	require.Equal(t, ErrInferredMultipleTypes, err)

	_, err = engine.InferParameters("INSERT INTO mytable(id, title) VALUES (@param1)", nil)
	require.Equal(t, ErrInvalidNumberOfValues, err)

	_, err = engine.InferParameters("INSERT INTO mytable1(id, title) VALUES (@param1, @param2)", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.InferParameters("INSERT INTO mytable(id, note) VALUES (@param1, @param2)", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, err = engine.InferParameters("SELECT * FROM mytable WHERE id > @param1 AND (@param1 OR active)", nil)
	require.Equal(t, ErrInferredMultipleTypes, err)

	_, err = engine.InferParameters("BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@param1, @param1); COMMIT;", nil)
	require.Equal(t, ErrInferredMultipleTypes, err)
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
			"Too large timestamp", []byte{0, 0, 0, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9}, TimestampType,
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
			[]byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0},
			IntegerType,
			&Number{val: 0},
			12,
		},
		{
			"large integer",
			[]byte{0, 0, 0, 8, 0, 0, 0, 0, 127, 255, 255, 255},
			IntegerType,
			&Number{val: math.MaxInt32},
			12,
		},
		{
			"large integer padded",
			[]byte{0, 0, 0, 8, 0, 0, 0, 0, 127, 255, 255, 255, 1, 1, 1},
			IntegerType,
			&Number{val: math.MaxInt32},
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
		{"no engine prefix", "no-e-prefix)"},
		{"no mapping prefix", "e-prefix-no-mapping-prefix"},
		{"short mapping prefix", "e-prefix-mapping"},
	} {
		t.Run(d.n, func(t *testing.T) {
			prefix, err := trimPrefix(e.prefix, []byte(d.k), []byte("-mapping-prefix"))
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
			prefix, err := trimPrefix(e.prefix, []byte(d.k), []byte("-mapping-prefix"))
			require.NoError(t, err)
			require.NotNil(t, prefix)
			require.EqualValues(t, []byte(d.p), prefix)
		})
	}
}

func TestUnmapDatabaseId(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	id, err := unmapDatabaseID(e.prefix, nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = unmapDatabaseID(e.prefix, []byte{})
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = unmapDatabaseID(e.prefix, []byte("pref"))
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = unmapDatabaseID(e.prefix, []byte("e-prefix.a"))
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, id)

	id, err = unmapDatabaseID(e.prefix, []byte(
		"e-prefix.CTL.DATABASE.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, id)

	id, err = unmapDatabaseID(e.prefix, append(
		[]byte("e-prefix.CTL.DATABASE."),
		1, 2, 3, 4,
	))
	require.NoError(t, err)
	require.EqualValues(t, 0x01020304, id)
}

func TestUnmapTableId(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, err := unmapTableID(e.prefix, nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)

	dbID, tableID, err = unmapTableID(e.prefix, []byte(
		"e-prefix.CTL.TABLE.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)

	dbID, tableID, err = unmapTableID(e.prefix, append(
		[]byte("e-prefix.CTL.TABLE."),
		0x01, 0x02, 0x03, 0x04,
		0x11, 0x12, 0x13, 0x14,
	))
	require.NoError(t, err)
	require.EqualValues(t, 0x01020304, dbID)
	require.EqualValues(t, 0x11121314, tableID)
}

func TestUnmapColSpec(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, colID, colType, err := unmapColSpec(e.prefix, nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = unmapColSpec(e.prefix, []byte(
		"e-prefix.CTL.COLUMN.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = unmapColSpec(e.prefix, append(
		[]byte("e-prefix.CTL.COLUMN."),
		0x01, 0x02, 0x03, 0x04,
		0x11, 0x12, 0x13, 0x14,
		0x21, 0x22, 0x23, 0x24,
		0x00,
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)
	require.Zero(t, colType)

	dbID, tableID, colID, colType, err = unmapColSpec(e.prefix, append(
		[]byte("e-prefix.CTL.COLUMN."),
		0x01, 0x02, 0x03, 0x04,
		0x11, 0x12, 0x13, 0x14,
		0x21, 0x22, 0x23, 0x24,
		'I', 'N', 'T', 'E', 'G', 'E', 'R',
	))

	require.NoError(t, err)
	require.EqualValues(t, 0x01020304, dbID)
	require.EqualValues(t, 0x11121314, tableID)
	require.EqualValues(t, 0x21222324, colID)
	require.Equal(t, "INTEGER", colType)
}

func TestUnmapIndex(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	dbID, tableID, colID, err := unmapIndex(e.prefix, nil)
	require.ErrorIs(t, err, ErrIllegalMappedKey)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)

	dbID, tableID, colID, err = unmapIndex(e.prefix, []byte(
		"e-prefix.CTL.INDEX.a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Zero(t, dbID)
	require.Zero(t, tableID)
	require.Zero(t, colID)

	dbID, tableID, colID, err = unmapIndex(e.prefix, append(
		[]byte("e-prefix.CTL.INDEX."),
		0x01, 0x02, 0x03, 0x04,
		0x11, 0x12, 0x13, 0x14,
		0x21, 0x22, 0x23, 0x24,
	))

	require.NoError(t, err)
	require.EqualValues(t, 0x01020304, dbID)
	require.EqualValues(t, 0x11121314, tableID)
	require.EqualValues(t, 0x21222324, colID)
}

func TestUnmapIndexEntry(t *testing.T) {
	e := Engine{prefix: []byte("e-prefix.")}

	encPKVals, err := unmapIndexEntry(&Index{id: PKIndexID, unique: true}, e.prefix, nil)
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Nil(t, encPKVals)

	encPKVals, err = unmapIndexEntry(&Index{id: PKIndexID, unique: true}, e.prefix, []byte(
		"e-prefix.R.\x80a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Nil(t, encPKVals)

	fullValue := append(
		[]byte("e-prefix.E."),
		0x01, 0x02, 0x03, 0x04,
		0x11, 0x12, 0x13, 0x14,
		0x00, 0x00, 0x00, 0x02,
		0x80,
		'a', 'b', 'c', 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 3,
		0x80,
		'w', 'x', 'y', 'z',
		0, 0, 0, 4,
	)

	sIndex := &Index{
		table: &Table{
			db: &Database{
				id: 0x01020304,
			},
			id: 0x11121314,
		},
		id:     2,
		unique: false,
		cols: []*Column{
			{id: 3, colType: VarcharType, maxLen: 10},
		},
	}

	encPKLen := 8

	for i := 13; i < len(fullValue)-encPKLen; i++ {
		encPKVals, err = unmapIndexEntry(sIndex, e.prefix, fullValue[:i])
		require.ErrorIs(t, err, ErrCorruptedData)
		require.Nil(t, encPKVals)
	}

	encPKVals, err = unmapIndexEntry(sIndex, e.prefix, fullValue)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0x80, 'w', 'x', 'y', 'z', 0, 0, 0, 4}, encPKVals)
}

func TestEncodeAsKeyEdgeCases(t *testing.T) {
	_, err := EncodeAsKey(nil, IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, err = EncodeAsKey("a", VarcharType, maxKeyLen+1)
	require.ErrorIs(t, err, ErrMaxKeyLengthExceeded)

	_, err = EncodeAsKey("a", "NOTATYPE", maxKeyLen)
	require.ErrorIs(t, err, ErrInvalidValue)

	t.Run("varchar cases", func(t *testing.T) {
		_, err = EncodeAsKey(true, VarcharType, 10)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, err = EncodeAsKey("abc", VarcharType, 1)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)
	})

	t.Run("integer cases", func(t *testing.T) {
		_, err = EncodeAsKey(true, IntegerType, 8)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, err = EncodeAsKey(int64(10), IntegerType, 4)
		require.ErrorIs(t, err, ErrCorruptedData)
	})

	t.Run("boolean cases", func(t *testing.T) {
		_, err = EncodeAsKey("abc", BooleanType, 1)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, err = EncodeAsKey(true, BooleanType, 2)
		require.ErrorIs(t, err, ErrCorruptedData)
	})

	t.Run("blob cases", func(t *testing.T) {
		_, err = EncodeAsKey("abc", BLOBType, 3)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, err = EncodeAsKey([]byte{1, 2, 3}, BLOBType, 2)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)
	})

	t.Run("timestamp cases", func(t *testing.T) {
		_, err = EncodeAsKey(true, TimestampType, 8)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, err = EncodeAsKey(int64(10), TimestampType, 4)
		require.ErrorIs(t, err, ErrCorruptedData)
	})
}

func TestIndexingNullableColumns(t *testing.T) {
	st, err := store.Open("catalog_indexing_nullable", store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()
	defer os.RemoveAll("catalog_indexing_nullable")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	exec := func(t *testing.T, stmt string) *SQLTx {
		ret, _, err := engine.Exec(stmt, nil, nil)
		require.NoError(t, err)
		return ret
	}
	query := func(t *testing.T, stmt string, expectedRows ...*Row) {
		reader, err := engine.Query(stmt, nil, nil)
		require.NoError(t, err)

		for _, expectedRow := range expectedRows {
			row, err := reader.Read()
			require.NoError(t, err)

			require.EqualValues(t, expectedRow, row)
		}

		_, err = reader.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = reader.Close()
		require.NoError(t, err)
	}

	colVal := func(t *testing.T, v interface{}, tp SQLValueType) TypedValue {
		switch v := v.(type) {
		case nil:
			return &NullValue{t: tp}
		case int:
			return &Number{val: int64(v)}
		case string:
			return &Varchar{val: v}
		case []byte:
			return &Blob{val: v}
		case bool:
			return &Bool{val: v}
		}
		require.Fail(t, "Unknown type of value")
		return nil
	}

	t1Row := func(id int64, v1, v2 interface{}) *Row {
		idVal := &Number{val: id}
		v1Val := colVal(t, v1, IntegerType)
		v2Val := colVal(t, v2, VarcharType)

		return &Row{
			ValuesByPosition: []TypedValue{
				idVal,
				v1Val,
				v2Val,
			},
			ValuesBySelector: map[string]TypedValue{
				EncodeSelector("", "db1", "table1", "id"): idVal,
				EncodeSelector("", "db1", "table1", "v1"): v1Val,
				EncodeSelector("", "db1", "table1", "v2"): v2Val,
			},
		}
	}

	t2Row := func(id int64, v1, v2, v3, v4 interface{}) *Row {
		idVal := &Number{val: id}
		v1Val := colVal(t, v1, IntegerType)
		v2Val := colVal(t, v2, VarcharType)
		v3Val := colVal(t, v3, BooleanType)
		v4Val := colVal(t, v4, BLOBType)

		return &Row{
			ValuesByPosition: []TypedValue{
				idVal,
				v1Val,
				v2Val,
				v3Val,
				v4Val,
			},
			ValuesBySelector: map[string]TypedValue{
				EncodeSelector("", "db1", "table2", "id"): idVal,
				EncodeSelector("", "db1", "table2", "v1"): v1Val,
				EncodeSelector("", "db1", "table2", "v2"): v2Val,
				EncodeSelector("", "db1", "table2", "v3"): v3Val,
				EncodeSelector("", "db1", "table2", "v4"): v4Val,
			},
		}
	}

	exec(t, `
		CREATE TABLE table1 (
			id INTEGER AUTO_INCREMENT,
			v1 INTEGER,
			v2 VARCHAR[16],
			PRIMARY KEY(id)
		)
	`)
	exec(t, "CREATE INDEX ON table1 (v1, v2)")
	query(t, "SELECT * FROM table1 USE INDEX ON(v1,v2)")

	t.Run("succeed adding non-null columns", func(t *testing.T) {
		exec(t, "INSERT INTO table1(v1,v2) VALUES(1, '2')")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2)",
			t1Row(1, 1, "2"),
		)

		exec(t, "INSERT INTO table1(v1,v2) VALUES(1, '3')")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)
	})

	t.Run("succeed adding null columns as the second indexed column", func(t *testing.T) {

		exec(t, "INSERT INTO table1(v1,v2) VALUES(1, null)")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(3, 1, nil),
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)

		exec(t, "INSERT INTO table1(v1,v2) VALUES(1, null)")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(3, 1, nil),
			t1Row(4, 1, nil),
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)

		exec(t, "INSERT INTO table1(v1,v2) VALUES(2, null)")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(3, 1, nil),
			t1Row(4, 1, nil),
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)
	})

	t.Run("succeed adding null columns as the first indexed column", func(t *testing.T) {
		exec(t, "INSERT INTO table1(v1,v2) VALUES(null, '4')")
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(3, 1, nil),
			t1Row(4, 1, nil),
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)

		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=1 ORDER BY v2",
			t1Row(3, 1, nil),
			t1Row(4, 1, nil),
			t1Row(1, 1, "2"),
			t1Row(2, 1, "3"),
		)
	})

	t.Run("succeed querying null columns using index", func(t *testing.T) {
		query(t,
			"SELECT * FROM table1 USE INDEX ON(v1,v2) WHERE v1=null",
			t1Row(6, nil, "4"),
		)
	})

	t.Run("suceed creating table with two indexes", func(t *testing.T) {

		exec(t, `
			CREATE TABLE table2 (
				id INTEGER AUTO_INCREMENT,
				v1 INTEGER,
				v2 VARCHAR[16],
				v3 BOOLEAN,
				v4 BLOB[15],
				PRIMARY KEY(id)
			)
		`)

		exec(t, "CREATE INDEX ON table2(v1, v2)")
		exec(t, "CREATE UNIQUE INDEX ON table2(v3, v4)")

		query(t, "SELECT * FROM table2 USE INDEX ON(v3,v4)")

	})

	t.Run("succeed inserting data on table with two indexes", func(t *testing.T) {
		exec(t, "INSERT INTO table2(v1, v2, v3, v4) VALUES(null, null, null, null)")
		query(t, "SELECT * FROM table2 USE INDEX ON(v1, v2)", t2Row(1, nil, nil, nil, nil))
		query(t, "SELECT * FROM table2 USE INDEX ON(v3, v4)", t2Row(1, nil, nil, nil, nil))
	})

	t.Run("fail adding entries with duplicate with nulls", func(t *testing.T) {
		_, _, err := engine.Exec("INSERT INTO table2(v1, v2, v3, v4) VALUES(1, '2', null, null)", nil, nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)
	})

	t.Run("succeed scanning multiple rows on table with two indexes", func(t *testing.T) {
		exec(t, `
			INSERT INTO table2(v1,v2,v3,v4) VALUES
			(1,'2',true, null),
			(3,'4',null, x'1234'),
			(5,'6',false, x'5678')
		`)

		// Order for boolean must be null -> false -> true
		query(t, "SELECT * FROM table2 USE INDEX ON(v3, v4)",
			t2Row(1, nil, nil, nil, nil),
			t2Row(3, 3, "4", nil, []byte{0x12, 0x34}),
			t2Row(4, 5, "6", false, []byte{0x56, 0x78}),
			t2Row(2, 1, "2", true, nil),
		)
	})
}

func TestTemporalQueries(t *testing.T) {
	st, err := store.Open("temporal_queries", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("temporal_queries")
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	rowCount := 10
	for i := 0; i < rowCount; i++ {
		_, txs, err := engine.Exec("INSERT INTO table1(title) VALUES (@title)", map[string]interface{}{"title": fmt.Sprintf("title%d", i)}, nil)
		require.NoError(t, err)
		require.Len(t, txs, 1)

		hdr := txs[0].TxHeader()

		t.Run("querying data with future date should not return any row", func(t *testing.T) {
			r, err := engine.Query("SELECT id, title FROM table1 AFTER CAST(@ts AS TIMESTAMP)", map[string]interface{}{"ts": hdr.Ts}, nil)
			require.NoError(t, err)

			_, err = r.Read()
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with a greater tx should not return any row", func(t *testing.T) {
			r, err := engine.Query("SELECT id, title FROM table1 AFTER TX @tx", map[string]interface{}{"tx": hdr.ID}, nil)
			require.NoError(t, err)

			_, err = r.Read()
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data since tx date should return the last row", func(t *testing.T) {
			q := "SELECT id, title FROM table1 SINCE CAST(@ts AS TIMESTAMP) UNTIL now()"

			params, err := engine.InferParameters(q, nil)
			require.NoError(t, err)
			require.NotNil(t, params)
			require.Len(t, params, 1)
			require.Equal(t, AnyType, params["ts"])

			r, err := engine.Query(q, map[string]interface{}{"ts": hdr.Ts}, nil)
			require.NoError(t, err)

			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i+1), row.ValuesBySelector["(db1.table1.id)"].Value())

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with since tx id should return the last row", func(t *testing.T) {
			r, err := engine.Query("SELECT id, title FROM table1 SINCE TX @tx", map[string]interface{}{"tx": hdr.ID}, nil)
			require.NoError(t, err)

			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i+1), row.ValuesBySelector["(db1.table1.id)"].Value())

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with until current tx ordering desc by name should return always the first row", func(t *testing.T) {
			r, err := engine.Query("SELECT id FROM table1 UNTIL TX @tx ORDER BY title ASC", map[string]interface{}{"tx": hdr.ID}, nil)
			require.NoError(t, err)

			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(1), row.ValuesBySelector["(db1.table1.id)"].Value())

			err = r.Close()
			require.NoError(t, err)
		})

		time.Sleep(1 * time.Second)
	}

	t.Run("querying data with until current time should return all rows", func(t *testing.T) {
		r, err := engine.Query("SELECT COUNT(*) as c FROM table1 SINCE TX 1 UNTIL now()", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("querying data since an older date should return all rows", func(t *testing.T) {
		r, err := engine.Query("SELECT COUNT(*) as c FROM table1 SINCE '2021-12-03'", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("querying data since an older date should return all rows", func(t *testing.T) {
		r, err := engine.Query("SELECT COUNT(*) as c FROM table1 SINCE CAST('2021-12-03' AS TIMESTAMP)", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestUnionOperator(t *testing.T) {
	st, err := store.Open("union_queries", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("union_queries")
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE INDEX ON table1(title)", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table2(id INTEGER AUTO_INCREMENT, name VARCHAR[30], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	_, err = engine.Query("SELECT COUNT(*) as c FROM table1 UNION SELECT id, title FROM table1", nil, nil)
	require.ErrorIs(t, err, ErrColumnMismatchInUnionStmt)

	_, err = engine.Query("SELECT COUNT(*) as c FROM table1 UNION SELECT title FROM table1", nil, nil)
	require.ErrorIs(t, err, ErrColumnMismatchInUnionStmt)

	_, err = engine.InferParameters("SELECT title FROM table1 UNION SELECT name FROM table2", nil)
	require.NoError(t, err)

	_, err = engine.InferParameters("SELECT title FROM table1 UNION invalid stmt", nil)
	require.ErrorIs(t, err, ErrParsingError)

	rowCount := 10
	for i := 0; i < rowCount; i++ {
		_, _, err := engine.Exec("INSERT INTO table1(title) VALUES (@title)", map[string]interface{}{"title": fmt.Sprintf("title%d", i)}, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec("INSERT INTO table2(name) VALUES (@name)", map[string]interface{}{"name": fmt.Sprintf("name%d", i)}, nil)
		require.NoError(t, err)
	}

	t.Run("default union should filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query(`
			SELECT COUNT(*) as c FROM table1
			UNION 
			SELECT COUNT(*) FROM table1
			UNION 
			SELECT COUNT(*) c FROM table1 t1
		`, nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union all should not filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query("SELECT COUNT(*) as c FROM table1 UNION ALL SELECT COUNT(*) FROM table1", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		row, err = r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(db1.table1.c)"].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union should filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query("SELECT title FROM table1 order by title desc UNION SELECT title FROM table1", nil, nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", rowCount-i-1), row.ValuesBySelector["(db1.table1.title)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union with subqueries over different tables", func(t *testing.T) {
		r, err := engine.Query("SELECT title FROM table1 UNION SELECT name FROM table2", nil, nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(db1.table1.title)"].Value())
		}

		for i := 0; i < rowCount; i++ {
			row, err := r.Read()
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("name%d", i), row.ValuesBySelector["(db1.table1.title)"].Value())
		}

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestTemporalQueriesEdgeCases(t *testing.T) {
	st, err := store.Open("temporal_queries_edge_cases", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("temporal_queries_edge_cases")
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
	require.NoError(t, err)

	err = engine.SetCurrentDatabase("db1")
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil, nil)
	require.NoError(t, err)

	edgeCases := []struct {
		title  string
		query  string
		params map[string]interface{}
		err    error
	}{
		{
			title:  "querying data with future date should not return any row",
			query:  "SELECT ts FROM table1 AFTER now() ORDER BY id DESC LIMIT 1",
			params: nil,
			err:    ErrNoMoreRows,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 SINCE TX @tx",
			params: map[string]interface{}{"tx": 0},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 SINCE TX @tx",
			params: map[string]interface{}{"tx": -1},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with col selector as tx id should return error",
			query:  "SELECT id, title FROM table1 SINCE TX id",
			params: nil,
			err:    ErrInvalidValue,
		},
		{
			title:  "querying data with aggregations as tx id should return error",
			query:  "SELECT id, title FROM table1 SINCE TX COUNT(*)",
			params: nil,
			err:    ErrInvalidValue,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 AFTER TX @tx",
			params: map[string]interface{}{"tx": 0},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 AFTER TX @tx",
			params: map[string]interface{}{"tx": -1},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 BEFORE TX @tx",
			params: map[string]interface{}{"tx": 0},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 BEFORE TX @tx",
			params: map[string]interface{}{"tx": -1},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 BEFORE TX @tx",
			params: map[string]interface{}{"tx": 1},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with invalid tx id should return error",
			query:  "SELECT id, title FROM table1 SINCE TX @tx",
			params: map[string]interface{}{"tx": uint64(math.MaxUint64)},
			err:    ErrIllegalArguments,
		},
		{
			title:  "querying data with valid tx id but greater than existent id should return no more rows error",
			query:  "SELECT id, title FROM table1 SINCE TX @tx",
			params: map[string]interface{}{"tx": math.MaxInt64},
			err:    ErrNoMoreRows,
		},
		{
			title:  "querying data with valid tx id but greater than existent id should return no more rows error",
			query:  "SELECT id, title FROM table1 AFTER TX @tx",
			params: map[string]interface{}{"tx": math.MaxInt64},
			err:    ErrNoMoreRows,
		},
		{
			title:  "querying data with valid tx id but greater than existent id should return no more rows error",
			query:  "SELECT id, title FROM table1 BEFORE TX @tx",
			params: map[string]interface{}{"tx": math.MaxInt64},
			err:    ErrNoMoreRows,
		},
	}

	for _, c := range edgeCases {
		t.Run(c.title, func(t *testing.T) {
			r, err := engine.Query(c.query, c.params, nil)
			require.NoError(t, err)

			_, err = r.Read()
			require.ErrorIs(t, err, c.err)

			err = r.Close()
			require.NoError(t, err)
		})
	}
}

func TestMultiDBCatalogQueries(t *testing.T) {
	st, err := store.Open("multidb_catalog_queries", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("multidb_catalog_queries")
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	t.Run("without a handler, multi database stmts are locally resolved", func(t *testing.T) {
		_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
		require.NoError(t, err)

		err = engine.SetCurrentDatabase("db1")
		require.NoError(t, err)

		r, err := engine.Query("SELECT * FROM DATABASES()", nil, nil)
		require.NoError(t, err)

		row, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, "db1", row.ValuesBySelector["(*.databases.name)"].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("with a handler, multi database stmts are delegated to the handler", func(t *testing.T) {
		dbs := []string{"db1", "db2"}

		handler := &multidbHandlerMock{
			dbs:    dbs,
			engine: engine,
		}
		engine.SetMultiDBHandler(handler)

		_, _, err = engine.Exec(`
			BEGIN TRANSACTION;
				CREATE DATABASE db1;
			COMMIT;
		`, nil, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec("CREATE DATABASE db1", nil, nil)
		require.ErrorIs(t, err, ErrNoSupported)

		_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
		require.NoError(t, err)

		ntx, ctxs, err := engine.Exec("USE DATABASE db1; USE DATABASE db2", nil, nil)
		require.NoError(t, err)
		require.Nil(t, ntx)
		require.Len(t, ctxs, 2)
		require.Zero(t, ctxs[0].UpdatedRows())
		require.Zero(t, ctxs[1].UpdatedRows())

		_, _, err = engine.Exec("BEGIN TRANSACTION; USE DATABASE db1; COMMIT;", nil, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		t.Run("unconditional database query", func(t *testing.T) {
			r, err := engine.Query("SELECT * FROM DATABASES() WHERE name LIKE 'db*'", nil, nil)
			require.NoError(t, err)

			for _, db := range dbs {
				row, err := r.Read()
				require.NoError(t, err)
				require.NotNil(t, row)
				require.NotNil(t, row)
				require.Equal(t, db, row.ValuesBySelector["(*.databases.name)"].Value())
			}

			_, err = r.Read()
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("query databases using conditions with table and column aliasing", func(t *testing.T) {
			r, err := engine.Query("SELECT dbs.name as dbname FROM DATABASES() as dbs WHERE name LIKE 'db*'", nil, nil)
			require.NoError(t, err)

			for _, db := range dbs {
				row, err := r.Read()
				require.NoError(t, err)
				require.NotNil(t, row)
				require.NotNil(t, row)
				require.Equal(t, db, row.ValuesBySelector["(*.dbs.dbname)"].Value())
			}

			_, err = r.Read()
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})
	})

	t.Run("with a handler, statements must only involve current selected database", func(t *testing.T) {
		dbs := []string{"db1", "db2"}

		handler := &multidbHandlerMock{
			dbs: dbs,
		}
		engine.SetMultiDBHandler(handler)

		_, _, err = engine.Exec("USE DATABASE db1", nil, nil)
		require.NoError(t, err)

		tx, _, err := engine.Exec("BEGIN TRANSACTION;", nil, nil)
		require.NoError(t, err)
		require.NotNil(t, tx)

		// doing stmt initialization because cross database references are disabled by grammar
		tableRef := &tableRef{
			db:    "db2",
			table: "table1",
		}

		_, err = tableRef.referencedTable(tx)
		require.ErrorIs(t, err, ErrNoSupported)

		tx.currentDB = nil
		_, err = tableRef.referencedTable(tx)
		require.ErrorIs(t, err, ErrNoDatabaseSelected)
	})
}

type multidbHandlerMock struct {
	dbs    []string
	engine *Engine
}

func (h *multidbHandlerMock) ListDatabases(ctx context.Context) ([]string, error) {
	return h.dbs, nil
}

func (h *multidbHandlerMock) CreateDatabase(ctx context.Context, db string, ifNotExists bool) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) UseDatabase(ctx context.Context, db string) error {
	return nil
}

func (h *multidbHandlerMock) ExecPreparedStmts(ctx context.Context, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	return h.engine.ExecPreparedStmts(stmts, params, nil)
}

func TestSingleDBCatalogQueries(t *testing.T) {
	st, err := store.Open("singledb_catalog_queries", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("singledb_catalog_queries")
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec("CREATE DATABASE db1;", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec("USE DATABASE db1;", nil, nil)
	require.NoError(t, err)

	tx, _, err := engine.Exec("BEGIN TRANSACTION;", nil, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(`
		CREATE TABLE mytable1(id INTEGER NOT NULL AUTO_INCREMENT, title VARCHAR[256], PRIMARY KEY id);
		CREATE INDEX ON mytable1(title);
	
		CREATE TABLE mytable2(id INTEGER NOT NULL, name VARCHAR[100], active BOOLEAN, PRIMARY KEY id);
		CREATE INDEX ON mytable2(name);
		CREATE UNIQUE INDEX ON mytable2(name, active);
	`, nil, tx)
	require.NoError(t, err)

	defer tx.Cancel()

	t.Run("querying tables without any condition should return all tables", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM TABLES()", nil, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(db1.tables.name)"].Value())

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.tables.name)"].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("querying tables with name equality comparison should return only one table", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM TABLES() WHERE name = 'mytable2'", nil, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.tables.name)"].Value())

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional index query should return all the indexes of mytable1", func(t *testing.T) {
		params := map[string]interface{}{
			"tableName": "mytable1",
		}
		r, err := engine.Query("SELECT * FROM INDEXES(@tableName)", params, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(db1.indexes.table)"].Value())
		require.Equal(t, "mytable1[id]", row.ValuesBySelector["(db1.indexes.name)"].Value())
		require.True(t, row.ValuesBySelector["(db1.indexes.unique)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.indexes.primary)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(db1.indexes.table)"].Value())
		require.Equal(t, "mytable1[title]", row.ValuesBySelector["(db1.indexes.name)"].Value())
		require.False(t, row.ValuesBySelector["(db1.indexes.unique)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.indexes.primary)"].Value().(bool))

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional index query should return all the indexes of mytable2", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM INDEXES('mytable2')", nil, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.indexes.table)"].Value())
		require.Equal(t, "mytable2[id]", row.ValuesBySelector["(db1.indexes.name)"].Value())
		require.True(t, row.ValuesBySelector["(db1.indexes.unique)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.indexes.primary)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.indexes.table)"].Value())
		require.Equal(t, "mytable2[name]", row.ValuesBySelector["(db1.indexes.name)"].Value())
		require.False(t, row.ValuesBySelector["(db1.indexes.unique)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.indexes.primary)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.indexes.table)"].Value())
		require.Equal(t, "mytable2[name,active]", row.ValuesBySelector["(db1.indexes.name)"].Value())
		require.True(t, row.ValuesBySelector["(db1.indexes.unique)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.indexes.primary)"].Value().(bool))

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional column query should return all the columns of mytable1", func(t *testing.T) {
		params := map[string]interface{}{
			"tableName": "mytable1",
		}

		r, err := engine.Query("SELECT * FROM COLUMNS(@tableName)", params, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(db1.columns.table)"].Value())
		require.Equal(t, "id", row.ValuesBySelector["(db1.columns.name)"].Value())
		require.Equal(t, IntegerType, row.ValuesBySelector["(db1.columns.type)"].Value())
		require.Equal(t, int64(8), row.ValuesBySelector["(db1.columns.max_length)"].Value())
		require.False(t, row.ValuesBySelector["(db1.columns.nullable)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.auto_increment)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.indexed)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.primary)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.unique)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(db1.columns.table)"].Value())
		require.Equal(t, "title", row.ValuesBySelector["(db1.columns.name)"].Value())
		require.Equal(t, VarcharType, row.ValuesBySelector["(db1.columns.type)"].Value())
		require.Equal(t, int64(256), row.ValuesBySelector["(db1.columns.max_length)"].Value())
		require.True(t, row.ValuesBySelector["(db1.columns.nullable)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.auto_increment)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.indexed)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.primary)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.unique)"].Value().(bool))

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional column query should return all the columns of mytable2", func(t *testing.T) {
		r, err := engine.Query("SELECT * FROM COLUMNS('mytable2')", nil, tx)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.columns.table)"].Value())
		require.Equal(t, "id", row.ValuesBySelector["(db1.columns.name)"].Value())
		require.Equal(t, IntegerType, row.ValuesBySelector["(db1.columns.type)"].Value())
		require.Equal(t, int64(8), row.ValuesBySelector["(db1.columns.max_length)"].Value())
		require.False(t, row.ValuesBySelector["(db1.columns.nullable)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.auto_increment)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.indexed)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.primary)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.unique)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.columns.table)"].Value())
		require.Equal(t, "name", row.ValuesBySelector["(db1.columns.name)"].Value())
		require.Equal(t, VarcharType, row.ValuesBySelector["(db1.columns.type)"].Value())
		require.Equal(t, int64(100), row.ValuesBySelector["(db1.columns.max_length)"].Value())
		require.True(t, row.ValuesBySelector["(db1.columns.nullable)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.auto_increment)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.indexed)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.primary)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.unique)"].Value().(bool))

		row, err = r.Read()
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(db1.columns.table)"].Value())
		require.Equal(t, "active", row.ValuesBySelector["(db1.columns.name)"].Value())
		require.Equal(t, BooleanType, row.ValuesBySelector["(db1.columns.type)"].Value())
		require.Equal(t, int64(1), row.ValuesBySelector["(db1.columns.max_length)"].Value())
		require.True(t, row.ValuesBySelector["(db1.columns.nullable)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.auto_increment)"].Value().(bool))
		require.True(t, row.ValuesBySelector["(db1.columns.indexed)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.primary)"].Value().(bool))
		require.False(t, row.ValuesBySelector["(db1.columns.unique)"].Value().(bool))

		_, err = r.Read()
		require.ErrorIs(t, err, ErrNoMoreRows)
	})
}
