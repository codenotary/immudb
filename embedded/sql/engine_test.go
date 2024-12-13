/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package sql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var sqlPrefix = []byte{2}

func closeStore(t *testing.T, st *store.ImmuStore) {
	err := st.Close()
	if !t.Failed() {
		// Do not pollute error output if test has already failed
		require.NoError(t, err)
	}
}

func setupCommonTest(t *testing.T) *Engine {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	return engine
}

func TestCreateDatabaseWithoutMultiIndexingEnabled(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(false))
	require.NoError(t, err)
	defer closeStore(t, st)

	_, err = NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.ErrorIs(t, err, ErrMultiIndexingNotEnabled)
}

func TestCreateDatabaseWithoutMultiDBHandler(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE DATABASE db1", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE DATABASE IF NOT EXISTS db1", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE USER user1 WITH PASSWORD 'user1Password!' READ", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)

	_, _, err = engine.Exec(context.Background(), nil, "ALTER USER user1 WITH PASSWORD 'user1Password!' ADMIN", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)

	_, _, err = engine.Exec(context.Background(), nil, "DROP USER user1", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)
}

func TestUseDatabaseWithoutMultiDBHandler(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "USE DATABASE db1", nil)
	require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)

	t.Run("without a handler, multi database stmts are not resolved", func(t *testing.T) {
		_, err := engine.Query(context.Background(), nil, "SELECT * FROM DATABASES()", nil)
		require.ErrorIs(t, err, ErrUnspecifiedMultiDBHandler)
	})

	r, err := engine.Query(context.Background(), nil, "SELECT ts FROM pg_type WHERE ts < 1 + NOW()", nil)
	require.NoError(t, err)
	defer r.Close()

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

func TestCreateTable(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (name VARCHAR, PRIMARY KEY name)", nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (name VARCHAR[30], PRIMARY KEY name)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("CREATE TABLE table2 (name VARCHAR[%d], PRIMARY KEY name)", MaxKeyLen+1), nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table3 (name VARCHAR[32], PRIMARY KEY name)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table4 (id INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)", nil)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS table1 (id INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS blob_table (id BLOB[2], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS balances (id INTEGER, balance FLOAT, CHECK (balance + id) >= 0, PRIMARY KEY id)", nil)
	require.NoError(t, err)
}

func TestTimestampType(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS timestamp_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "timestamp_table", "ts")

	t.Run("must accept NOW() as a timestamp", func(t *testing.T) {
		tsBefore := time.Now().UTC()

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO timestamp_table(ts) VALUES(NOW())", nil)
		require.NoError(t, err)

		tsAfter := time.Now().UTC()

		_, err := engine.InferParameters(context.Background(), nil, "SELECT ts FROM timestamp_table WHERE ts < 1 + NOW()")
		require.ErrorIs(t, err, ErrInvalidTypes)

		params := map[string]interface{}{
			"limit":  1,
			"offset": 0,
		}

		r, err := engine.Query(context.Background(), nil, "SELECT ts FROM timestamp_table WHERE ts < NOW() ORDER BY id DESC LIMIT @limit+0 OFFSET @offset", params)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.False(t, tsBefore.After(row.ValuesBySelector[sel].RawValue().(time.Time)))
		require.False(t, tsAfter.Before(row.ValuesBySelector[sel].RawValue().(time.Time)))

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
	})

	t.Run("must accept time.Time as timestamp parameter", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil,
			"INSERT INTO timestamp_table(ts) VALUES(@ts)", map[string]interface{}{
				"ts": time.Date(2021, 12, 1, 18, 06, 14, 0, time.UTC),
			},
		)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Date(2021, 12, 1, 18, 06, 14, 0, time.UTC), row.ValuesBySelector[sel].RawValue())
	})

	t.Run("must correctly validate timestamp equality", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil,
			"INSERT INTO timestamp_table(ts) VALUES(@ts)", map[string]interface{}{
				"ts": time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC),
			},
		)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			"SELECT ts FROM timestamp_table WHERE ts = @ts ORDER BY id", map[string]interface{}{
				"ts": time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC),
			})
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Date(2021, 12, 6, 10, 14, 0, 0, time.UTC), row.ValuesBySelector[sel].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(context.Background(), nil,
			"SELECT ts FROM timestamp_table WHERE ts = @ts ORDER BY id", map[string]interface{}{
				"ts": "2021-12-06 10:14",
			})
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNotComparableValues)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestTimestampIndex(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS timestamp_index (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON timestamp_index(ts)", nil)
	require.NoError(t, err)

	for i := 100; i > 0; i-- {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO timestamp_index(ts) VALUES(@ts)", map[string]interface{}{"ts": time.Unix(int64(i), 0)})
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, "SELECT * FROM timestamp_index ORDER BY ts", nil)
	require.NoError(t, err)
	defer r.Close()

	for i := 100; i > 0; i-- {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.EqualValues(t, i, row.ValuesBySelector[EncodeSelector("", "timestamp_index", "id")].RawValue())
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

func TestTimestampCasts(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS timestamp_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "timestamp_table", "ts")

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
				context.Background(), nil,
				fmt.Sprintf("INSERT INTO timestamp_table(ts) VALUES(CAST('%s' AS TIMESTAMP))", d.str), nil)
			require.NoError(t, err)

			r, err := engine.Query(context.Background(), nil, "SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil)
			require.NoError(t, err)
			defer r.Close()

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
			require.Equal(t, d.t, row.ValuesBySelector[sel].RawValue())
		})
	}

	t.Run("insert a timestamp value using a cast from INTEGER", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(), nil,
			"INSERT INTO timestamp_table(ts) VALUES(CAST(123456 AS TIMESTAMP))", nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT ts FROM timestamp_table ORDER BY id DESC LIMIT 1", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, TimestampType, row.ValuesBySelector[sel].Type())
		require.Equal(t, time.Unix(123456, 0).UTC(), row.ValuesBySelector[sel].RawValue())
	})

	t.Run("test casting from null values", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(), nil,
			`
			CREATE TABLE IF NOT EXISTS values_table (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, str VARCHAR, i INTEGER, PRIMARY KEY id);
			INSERT INTO values_table(ts, str,i) VALUES(NOW(), NULL, NULL);
		`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(), nil,
			`
			UPDATE values_table SET ts = CAST(str AS TIMESTAMP);
		`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(), nil,
			`
			UPDATE values_table SET ts = i::TIMESTAMP;
		`, nil)
		require.NoError(t, err)
	})

	t.Run("test casting invalid string", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO timestamp_table(ts) VALUES(CAST('not a datetime' AS TIMESTAMP))", nil)
		require.ErrorIs(t, err, ErrUnsupportedCast)

		_, _, err = engine.Exec(
			context.Background(), nil,
			"INSERT INTO timestamp_table(ts) VALUES(CAST(@ts AS TIMESTAMP))", map[string]interface{}{
				"ts": strings.Repeat("long string ", 1000),
			})
		require.ErrorIs(t, err, ErrUnsupportedCast)
	})

	t.Run("test casting unsupported type", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO timestamp_table(ts) VALUES(CAST(true AS TIMESTAMP))", nil)
		require.ErrorIs(t, err, ErrUnsupportedCast)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO timestamp_table(ts) VALUES(CAST(true AS INTEGER))", nil)
		require.ErrorIs(t, err, ErrUnsupportedCast)
	})

	t.Run("test type inference with casting", func(t *testing.T) {
		_, err = engine.Query(context.Background(), nil, "SELECT * FROM timestamp_table WHERE id < CAST(true AS TIMESTAMP)", nil)
		require.ErrorIs(t, err, ErrUnsupportedCast)

		rowReader, err := engine.Query(context.Background(), nil, "SELECT * FROM timestamp_table WHERE ts > CAST(id::INTEGER AS TIMESTAMP)", nil)
		require.NoError(t, err)

		_, err = rowReader.Read(context.Background())
		require.NoError(t, err)

		require.NoError(t, rowReader.Close())
	})
}

func TestUUIDAsPK(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS uuid_table(id UUID, test INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "uuid_table", "id")

	t.Run("UUID as PK", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id) VALUES(RANDOM_UUID())", nil)
		require.NoError(t, err)

		_, err := engine.InferParameters(context.Background(), nil, "SELECT id FROM uuid_table WHERE id = NOW()")
		require.ErrorIs(t, err, ErrInvalidTypes)

		r, err := engine.Query(context.Background(), nil, "SELECT id FROM uuid_table", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
	})

	t.Run("must accept RANDOM_UUID() as an UUID", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id) VALUES(RANDOM_UUID())", nil)
		require.NoError(t, err)

		_, err := engine.InferParameters(context.Background(), nil, "SELECT id FROM uuid_table WHERE id = NOW()")
		require.ErrorIs(t, err, ErrInvalidTypes)

		r, err := engine.Query(context.Background(), nil, "SELECT id FROM uuid_table", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
	})

	t.Run("must accept uuid string as an UUID", func(t *testing.T) {
		id := uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id, test) VALUES(@uuid, 3)", map[string]interface{}{
			"uuid": id.String(),
		})
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT id FROM uuid_table WHERE test = 3", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
		require.Equal(t, id, row.ValuesByPosition[0].RawValue())
	})

	t.Run("must accept byte slice as an UUID", func(t *testing.T) {
		id := uuid.UUID([16]byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id, test) VALUES(@uuid, 4)", map[string]interface{}{
			"uuid": id[:],
		})
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT id FROM uuid_table WHERE test = 4", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
		require.Equal(t, id, row.ValuesByPosition[0].RawValue())
	})

}

func TestUUIDNonPK(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE uuid_table(id INTEGER, u UUID, t VARCHAR, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	sel := EncodeSelector("", "uuid_table", "u")

	t.Run("UUID as non PK", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id, u, t) VALUES(1, RANDOM_UUID(), 't')", nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT u FROM uuid_table", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
	})

	t.Run("UUID as non PK must accept uuid string", func(t *testing.T) {
		id := uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id, u, t) VALUES(2, @id, 't')", map[string]interface{}{
			"id": id.String(),
		})
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT u FROM uuid_table WHERE id = 2 LIMIT 1", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
		require.Equal(t, id, row.ValuesByPosition[0].RawValue())
	})

	t.Run("UUID as non PK must accept byte slice", func(t *testing.T) {
		id := uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO uuid_table(id, u, t) VALUES(3, @id, 't')", map[string]interface{}{
			"id": id[:],
		})
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT u FROM uuid_table WHERE id = 3 LIMIT 1", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, UUIDType, row.ValuesBySelector[sel].Type())

		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[sel])
		require.Equal(t, id, row.ValuesByPosition[0].RawValue())
	})

}

func TestFloatType(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		"CREATE TABLE IF NOT EXISTS float_table (id INTEGER AUTO_INCREMENT, ft FLOAT, PRIMARY KEY id)",
		nil,
	)
	require.NoError(t, err)

	t.Run("must insert float type", func(t *testing.T) {
		for _, d := range []struct {
			valStr   string
			valFloat float64
		}{
			{"0", 0},
			{"-0", 0},
			{"1", 1},
			{"-1", -1.0},
			{"100.100", 100.100},
			{".7", .7},
			{".543210", .543210},
			{"105.7", 105.7},
			{"00105.98988897", 00105.98988897},
		} {
			t.Run("Valid float: "+d.valStr, func(t *testing.T) {
				_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO float_table(ft) VALUES("+d.valStr+")", nil)
				require.NoError(t, err)

				r, err := engine.Query(context.Background(), nil, "SELECT ft FROM float_table ORDER BY id DESC LIMIT 1", nil)
				require.NoError(t, err)
				defer r.Close()

				row, err := r.Read(context.Background())
				require.NoError(t, err)
				require.Equal(t, Float64Type, row.ValuesByPosition[0].Type())
				require.Equal(t, d.valFloat, row.ValuesByPosition[0].RawValue())
			})
		}

		for _, d := range []string{
			"105.9898.8897",
			"0..0",
		} {
			t.Run("Invalid float: "+d, func(t *testing.T) {
				_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO float_table(ft) VALUES("+d+")", nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("must accept float as parameter", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_table(ft) VALUES(@ft)",
			map[string]interface{}{
				"ft": -0.4,
			},
		)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, "SELECT ft FROM float_table ORDER BY id DESC LIMIT 1", nil)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, Float64Type, row.ValuesByPosition[0].Type())
		require.Equal(t, -0.4, row.ValuesByPosition[0].RawValue())
	})

	t.Run("must correctly validate float equality", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_table(ft) VALUES(@ft)",
			map[string]interface{}{
				"ft": 0.78,
			},
		)
		require.NoError(t, err)

		r, err := engine.Query(
			context.Background(),
			nil,
			"SELECT ft FROM float_table WHERE ft = @ft ORDER BY id",
			map[string]interface{}{
				"ft": 0.78,
			})
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, Float64Type, row.ValuesByPosition[0].Type())
		require.Equal(t, 0.78, row.ValuesByPosition[0].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(
			context.Background(), nil,
			"SELECT ts FROM float_table WHERE ft = @ft ORDER BY id",
			map[string]interface{}{
				"ft": "2021-12-06 10:14",
			})
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrUnsupportedCast)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("must correctly handle floating points in aggregate functions", func(t *testing.T) {
		_, _, err := engine.Exec(
			context.Background(),
			nil,
			`
			CREATE TABLE aggregate_test(
				id INTEGER AUTO_INCREMENT,
				f FLOAT,
				PRIMARY KEY(id)
			)
			`,
			nil,
		)
		require.NoError(t, err)

		aggregateFunctions := []struct {
			fn     string
			result float64
		}{
			{"MAX", 4.0},
			{"MIN", -1.0},
			{"SUM", 10.0},
			{"AVG", 10.0 / 6.0},
		}

		// Empty table - this is a corner case that has to be checked too
		for _, d := range aggregateFunctions {
			t.Run(d.fn, func(t *testing.T) {
				res, err := engine.Query(
					context.Background(),
					nil,
					"SELECT "+d.fn+"(f) FROM aggregate_test",
					nil)
				require.NoError(t, err)
				defer res.Close()

				row, err := res.Read(context.Background())
				require.NoError(t, err)

				require.Len(t, row.ValuesByPosition, 1)
				require.EqualValues(t, 0.0, row.ValuesByPosition[0].RawValue())
			})
		}

		// Add some values
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			`
			INSERT INTO aggregate_test(f)
			VALUES (2.0), (1.0), (4.0), (3.0), (-1.0), (1.0)
			`,
			nil)
		require.NoError(t, err)

		for _, d := range aggregateFunctions {
			t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
				res, err := engine.Query(
					context.Background(),
					nil,
					"SELECT "+d.fn+"(f) FROM aggregate_test",
					nil)
				require.NoError(t, err)
				defer res.Close()

				row, err := res.Read(context.Background())
				require.NoError(t, err)

				require.Len(t, row.ValuesByPosition, 1)
				require.EqualValues(t, d.result, row.ValuesByPosition[0].RawValue())
			})
		}
	})

	t.Run("correctly infer fliating-point parameter", func(t *testing.T) {
		params, err := engine.InferParameters(
			context.Background(),
			nil,
			"SELECT * FROM float_table WHERE ft = @fparam",
		)
		require.NoError(t, err)
		require.Equal(t, map[string]SQLValueType{"fparam": Float64Type}, params)
	})
}

func TestFloatIndex(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		"CREATE TABLE IF NOT EXISTS float_index (id INTEGER AUTO_INCREMENT, ft FLOAT, PRIMARY KEY id)",
		nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		"CREATE INDEX ON float_index(ft)",
		nil,
	)
	require.NoError(t, err)

	for i := 100; i > 0; i-- {
		val, _ := strconv.ParseFloat(fmt.Sprint(i, ".", i), 64)
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_index(ft) VALUES(@ft)",
			map[string]interface{}{"ft": val})
		require.NoError(t, err)
	}

	r, err := engine.Query(
		context.Background(),
		nil,
		"SELECT * FROM float_index ORDER BY ft",
		nil)
	require.NoError(t, err)
	defer r.Close()

	prevf := float64(-1.0)
	for i := 100; i > 0; i-- {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.EqualValues(t, i, row.ValuesBySelector[EncodeSelector("", "float_index", "id")].RawValue())

		currf := row.ValuesBySelector[EncodeSelector("", "float_index", "ft")].RawValue().(float64)
		require.Less(t, prevf, currf)
		prevf = currf
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

func TestFloatIndexOnNegatives(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		"CREATE TABLE IF NOT EXISTS float_index (id INTEGER AUTO_INCREMENT, ft FLOAT, PRIMARY KEY id)",
		nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		"CREATE INDEX ON float_index(ft)",
		nil)
	require.NoError(t, err)

	var z float64
	floatSerie := []float64{
		z,      /*0*/
		-z,     /*-0*/
		1 / z,  /*+Inf*/
		-1 / z, /*-Inf*/
		+z / z, /*NaN*/
		-z / z, /*NaN*/
		-1.0,
		3.345,
		-0.5,
		0.0,
		-100.8,
		0.5,
		1.0,
		math.MaxFloat64,
		-math.MaxFloat64,
		math.SmallestNonzeroFloat64,
	}

	for _, ft := range floatSerie {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_index(ft) VALUES(@ft)",
			map[string]interface{}{"ft": ft})
		require.NoError(t, err)
	}

	r, err := engine.Query(
		context.Background(),
		nil,
		"SELECT * FROM float_index ORDER BY ft",
		nil)
	require.NoError(t, err)
	defer r.Close()

	sort.Float64s(floatSerie)

	for i := 0; i < len(floatSerie); i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)

		val := row.ValuesBySelector[EncodeSelector("", "float_index", "ft")].RawValue()
		if i == 0 {
			require.True(t, math.IsNaN(val.(float64)))
			continue
		}
		if i == 1 {
			require.True(t, math.IsNaN(val.(float64)))
			continue
		}
		if i == 7 { // negative zero
			require.True(t, math.Signbit(val.(float64)))
		}
		if i == 8 { // positive zero
			require.False(t, math.Signbit(val.(float64)))
		}
		if i == 9 { // positive zero
			require.False(t, math.Signbit(val.(float64)))
		}
		if i == 10 {
			require.Equal(t, math.SmallestNonzeroFloat64, val)
		}
		require.Equal(t, floatSerie[i], val)
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

func TestFloatCasts(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		"CREATE TABLE IF NOT EXISTS float_table (id INTEGER AUTO_INCREMENT, ft FLOAT, PRIMARY KEY id)",
		nil)
	require.NoError(t, err)

	for _, d := range []struct {
		str string
		f   float64
	}{
		{"0.5", 0.5},
		{".1", 0.1},
	} {
		t.Run(fmt.Sprintf("insert a float value using a cast from '%s'", d.str), func(t *testing.T) {
			_, _, err = engine.Exec(
				context.Background(),
				nil,
				fmt.Sprintf("INSERT INTO float_table(ft) VALUES(CAST('%s' AS FLOAT))", d.str),
				nil,
			)
			require.NoError(t, err)

			r, err := engine.Query(
				context.Background(),
				nil,
				"SELECT ft FROM float_table ORDER BY id DESC LIMIT 1",
				nil)
			require.NoError(t, err)
			defer r.Close()

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, Float64Type, row.ValuesByPosition[0].Type())
			require.Equal(t, d.f, row.ValuesByPosition[0].RawValue())
		})
	}

	t.Run("insert a float value using a cast from INTEGER", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_table(ft) VALUES(CAST(123456 AS FLOAT))",
			nil)
		require.NoError(t, err)

		r, err := engine.Query(
			context.Background(),
			nil,
			"SELECT ft FROM float_table ORDER BY id DESC LIMIT 1",
			nil,
		)
		require.NoError(t, err)
		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, Float64Type, row.ValuesByPosition[0].Type())
		require.Equal(t, float64(123456), row.ValuesByPosition[0].RawValue())
	})

	t.Run("test casting from null values", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			`
			CREATE TABLE IF NOT EXISTS values_table (id INTEGER AUTO_INCREMENT, ft FLOAT, str VARCHAR, i INTEGER, PRIMARY KEY id);
			INSERT INTO values_table(ft, str,i) VALUES(NULL, NULL, NULL);
			`,
			nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			`
			UPDATE values_table SET ft = CAST(str AS FLOAT);
			`,
			nil,
		)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			`
			UPDATE values_table SET ft = CAST(i AS FLOAT);
			`,
			nil)
		require.NoError(t, err)
	})

	t.Run("test casting invalid string", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_table(ft) VALUES(CAST('not a float' AS FLOAT))",
			nil)
		require.ErrorIs(t, err, ErrUnsupportedCast)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO float_table(ft) VALUES(CAST(@ft AS FLOAT))",
			map[string]interface{}{
				"ft": strings.Repeat("long string ", 1000),
			})
		require.ErrorIs(t, err, ErrUnsupportedCast)
	})

}

func TestNumericCasts(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		`
			CREATE TABLE IF NOT EXISTS numeric_table (id INTEGER AUTO_INCREMENT, quantity INTEGER, price FLOAT, PRIMARY KEY id);
			CREATE INDEX ON numeric_table(quantity);
			CREATE INDEX ON numeric_table(price);
			CREATE INDEX ON numeric_table(quantity, price);
		`,
		nil)
	require.NoError(t, err)

	for _, d := range []struct {
		q interface{}
		p interface{}
	}{
		{10, 0.5},
		{1.5, 7},
		{nil, nil},
	} {
		params := make(map[string]interface{})
		params["q"] = d.q
		params["p"] = d.p

		t.Run("insert row with numeric casting", func(t *testing.T) {
			_, _, err = engine.Exec(
				context.Background(),
				nil,
				"INSERT INTO numeric_table(quantity, price) VALUES(CAST(@q AS INTEGER), CAST(@p AS FLOAT))",
				params,
			)
			require.NoError(t, err)

			r, err := engine.Query(
				context.Background(),
				nil,
				"SELECT quantity, price FROM numeric_table ORDER BY id DESC LIMIT 1",
				nil)
			require.NoError(t, err)
			defer r.Close()

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, IntegerType, row.ValuesByPosition[0].Type())
			require.Equal(t, Float64Type, row.ValuesByPosition[1].Type())
		})

		t.Run("insert row with implicit numeric casting", func(t *testing.T) {
			_, _, err = engine.Exec(
				context.Background(),
				nil,
				"INSERT INTO numeric_table(quantity, price) VALUES(@q, @p)",
				params,
			)
			require.NoError(t, err)

			r, err := engine.Query(
				context.Background(),
				nil,
				"SELECT quantity, price FROM numeric_table ORDER BY id DESC LIMIT 1",
				nil)
			require.NoError(t, err)
			defer r.Close()

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, IntegerType, row.ValuesByPosition[0].Type())
			require.Equal(t, Float64Type, row.ValuesByPosition[1].Type())
		})
	}
}

func TestNowFunctionEvalsToTxTimestamp(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(), nil, "CREATE TABLE tx_timestamp (id INTEGER AUTO_INCREMENT, ts TIMESTAMP, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	currentTs := time.Now()

	for it := 0; it < 3; it++ {
		time.Sleep(1 * time.Microsecond)

		_, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION; ROLLBACK;", nil)
		require.NoError(t, err)

		tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		require.True(t, tx.Timestamp().After(currentTs))

		rowCount := 10

		for i := 0; i < rowCount; i++ {
			_, _, err = engine.Exec(context.Background(), tx, "INSERT INTO tx_timestamp(ts) VALUES (NOW()), (NOW())", nil)
			require.NoError(t, err)
		}

		r, err := engine.Query(context.Background(), tx, "SELECT * FROM tx_timestamp WHERE ts = @ts", map[string]interface{}{"ts": tx.Timestamp()})
		require.NoError(t, err)
		defer r.Close()

		for i := 0; i < rowCount*2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, tx.Timestamp(), row.ValuesBySelector[EncodeSelector("", "tx_timestamp", "ts")].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx, "COMMIT;", nil)
		require.NoError(t, err)

		currentTs = tx.Timestamp()
	}
}

func TestAddColumn(t *testing.T) {
	dir := t.TempDir()

	t.Run("create-store", func(t *testing.T) {
		st, err := store.Open(dir, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer closeStore(t, st)

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (name VARCHAR, PRIMARY KEY id)", nil)
		require.ErrorIs(t, err, ErrColumnDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(name, surname) VALUES('John', 'Smith')", nil)
		require.ErrorIs(t, err, ErrColumnDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN int INTEGER AUTO_INCREMENT", nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN surname VARCHAR NOT NULL", nil)
		require.ErrorIs(t, err, ErrNewColumnMustBeNullable)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table2 ADD COLUMN surname VARCHAR", nil)
		require.ErrorIs(t, err, ErrTableDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN value INTEGER[100]", nil)
		require.ErrorIs(t, err, ErrLimitedMaxLen)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN surname VARCHAR", nil)
		require.ErrorIs(t, err, ErrColumnAlreadyExists)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(name, surname) VALUES('John', 'Smith')", nil)
		require.NoError(t, err)

		res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname FROM table1", nil)
		require.NoError(t, err)

		row, err := res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 1, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "John", row.ValuesByPosition[1].RawValue())
		require.EqualValues(t, "Smith", row.ValuesByPosition[2].RawValue())

		_, err = res.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = res.Close()
		require.NoError(t, err)
	})

	t.Run("reopen-store", func(t *testing.T) {
		st, err := store.Open(dir, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer closeStore(t, st)

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname FROM table1", nil)
		require.NoError(t, err)

		row, err := res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 1, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "John", row.ValuesByPosition[1].RawValue())
		require.EqualValues(t, "Smith", row.ValuesByPosition[2].RawValue())

		_, err = res.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = res.Close()
		require.NoError(t, err)
	})
}

func TestRenaming(t *testing.T) {
	dir := t.TempDir()

	t.Run("create-store", func(t *testing.T) {
		st, err := store.Open(dir, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer closeStore(t, st)

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME TO table11", nil)
		require.ErrorIs(t, err, ErrTableDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table11 (id INTEGER AUTO_INCREMENT, name VARCHAR[50], PRIMARY KEY id)", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table11(name)", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table11 RENAME TO table1", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(name) VALUES('John'), ('Sylvia'), ('Robocop') ", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME COLUMN name TO name", nil)
		require.ErrorIs(t, err, ErrSameOldAndNewNames)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME COLUMN name TO id", nil)
		require.ErrorIs(t, err, ErrColumnAlreadyExists)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table2 RENAME COLUMN name TO surname", nil)
		require.ErrorIs(t, err, ErrTableDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME COLUMN surname TO name", nil)
		require.ErrorIs(t, err, ErrColumnDoesNotExist)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME COLUMN name TO surname", nil)
		require.NoError(t, err)

		res, err := engine.Query(context.Background(), nil, "SELECT id, surname FROM table1 ORDER BY surname", nil)
		require.NoError(t, err)

		row, err := res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 1, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "John", row.ValuesByPosition[1].RawValue())

		row, err = res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 3, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "Robocop", row.ValuesByPosition[1].RawValue())

		row, err = res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 2, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "Sylvia", row.ValuesByPosition[1].RawValue())

		_, err = res.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = res.Close()
		require.NoError(t, err)
	})

	t.Run("reopen-store", func(t *testing.T) {
		st, err := store.Open(dir, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer closeStore(t, st)

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME TO table1", nil)
		require.ErrorIs(t, err, ErrSameOldAndNewNames)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME TO table11", nil)
		require.NoError(t, err)

		res, err := engine.Query(context.Background(), nil, "SELECT id, surname FROM table11 ORDER BY surname", nil)
		require.NoError(t, err)

		row, err := res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 1, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "John", row.ValuesByPosition[1].RawValue())

		row, err = res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 3, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "Robocop", row.ValuesByPosition[1].RawValue())

		row, err = res.Read(context.Background())
		require.NoError(t, err)

		require.EqualValues(t, 2, row.ValuesByPosition[0].RawValue())
		require.EqualValues(t, "Sylvia", row.ValuesByPosition[1].RawValue())

		_, err = res.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = res.Close()
		require.NoError(t, err)
	})
}

func TestAlterTableDropColumn(t *testing.T) {
	path := t.TempDir()

	defer os.RemoveAll(path)

	t.Run("create-store", func(t *testing.T) {
		st, err := store.Open(path, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer func() { require.NoError(t, st.Close()) }()

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		t.Run("create initial table", func(t *testing.T) {
			_, _, err = engine.Exec(
				context.Background(),
				nil,
				`
				CREATE TABLE table1 (
					id INTEGER AUTO_INCREMENT,
					active BOOLEAN,
					name VARCHAR[50],
					surname VARCHAR[50],
					age INTEGER,
					PRIMARY KEY (id)
				)`, nil)
			require.NoError(t, err)

			_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
			require.NoError(t, err)

			_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1(name, surname)", nil)
			require.NoError(t, err)

			_, _, err = engine.Exec(
				context.Background(), nil,
				`
				INSERT INTO table1(name, surname, active, age)
				VALUES
					('John', 'Smith', true, 42),
					('Sylvia', 'Smith', true, 27),
					('Robo', 'Cop', false, 101)
				`, nil)
			require.NoError(t, err)
		})

		_, _, err = engine.Exec(context.Background(), nil, "DROP INDEX ON table1(id)", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		t.Run("fail to drop indexed from table that does not exist", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table2 DROP COLUMN active", nil)
			require.ErrorIs(t, err, ErrTableDoesNotExist)
		})

		t.Run("fail to drop indexed columns", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN id", nil)
			require.ErrorIs(t, err, ErrCannotDropColumn)

			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN name", nil)
			require.ErrorIs(t, err, ErrCannotDropColumn)

			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN surname", nil)
			require.ErrorIs(t, err, ErrCannotDropColumn)
		})

		t.Run("fail to drop columns that does not exist", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN nonexistent", nil)
			require.ErrorIs(t, err, ErrColumnDoesNotExist)
		})

		t.Run("drop column in the middle", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN active", nil)
			require.NoError(t, err)

			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN deprecated BOOLEAN", nil)
			require.NoError(t, err)

			tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
			require.NoError(t, err)

			catTable, err := tx.catalog.GetTableByName("table1")
			require.NoError(t, err)

			require.Len(t, catTable.cols, 5)
			require.Len(t, catTable.colsByID, 5)
			require.Len(t, catTable.colsByName, 5)
			require.EqualValues(t, catTable.maxColID, 6)

			err = tx.Cancel()
			require.NoError(t, err)

			res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname, active, age FROM table1", nil)
			require.NoError(t, err)

			_, err = res.Read(context.Background())
			require.ErrorIs(t, err, ErrColumnDoesNotExist)

			err = res.Close()
			require.NoError(t, err)

			res, err = engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
			require.NoError(t, err)

			for i := 0; i < 3; i++ {
				row, err := res.Read(context.Background())
				require.NoError(t, err)
				require.Len(t, row.ValuesByPosition, 5)
				require.Len(t, row.ValuesBySelector, 5)
			}

			_, err = res.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = res.Close()
			require.NoError(t, err)
		})

		t.Run("drop the last column", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "DROP TABLE table11", nil)
			require.ErrorIs(t, err, ErrTableDoesNotExist)

			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table11 DROP COLUMN age", nil)
			require.ErrorIs(t, err, ErrTableDoesNotExist)

			_, _, err = engine.Exec(context.Background(), nil, "DROP INDEX ON table11(age)", nil)
			require.ErrorIs(t, err, ErrTableDoesNotExist)

			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 DROP COLUMN age", nil)
			require.NoError(t, err)

			tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
			require.NoError(t, err)

			catTable, err := tx.catalog.GetTableByName("table1")
			require.NoError(t, err)

			require.Len(t, catTable.cols, 4)
			require.Len(t, catTable.colsByID, 4)
			require.Len(t, catTable.colsByName, 4)
			require.EqualValues(t, catTable.maxColID, 6)

			err = tx.Cancel()
			require.NoError(t, err)

			res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname, age FROM table1", nil)
			require.NoError(t, err)

			_, err = res.Read(context.Background())
			require.ErrorIs(t, err, ErrColumnDoesNotExist)

			res.Close()

			res, err = engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
			require.NoError(t, err)

			for i := 0; i < 3; i++ {
				row, err := res.Read(context.Background())
				require.NoError(t, err)
				require.Len(t, row.ValuesByPosition, 4)
				require.Len(t, row.ValuesBySelector, 4)
			}

			_, err = res.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = res.Close()
			require.NoError(t, err)
		})

		t.Run("adding new column must not reuse old column IDs", func(t *testing.T) {
			_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 ADD COLUMN active BOOLEAN", nil)
			require.NoError(t, err)

			tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
			require.NoError(t, err)

			catTable, err := tx.catalog.GetTableByName("table1")
			require.NoError(t, err)

			require.Len(t, catTable.cols, 5)
			require.Len(t, catTable.colsByID, 5)
			require.Len(t, catTable.colsByName, 5)
			require.EqualValues(t, 7, catTable.colsByName["active"].id)
			require.EqualValues(t, 7, catTable.maxColID)

			err = tx.Cancel()
			require.NoError(t, err)

			res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname, active FROM table1", nil)
			require.NoError(t, err)

			for i := 0; i < 3; i++ {
				row, err := res.Read(context.Background())
				require.NoError(t, err)
				require.Len(t, row.ValuesByPosition, 4)
				require.Len(t, row.ValuesBySelector, 4)
				require.Nil(t, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())
			}

			_, err = res.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = res.Close()
			require.NoError(t, err)
		})
	})

	t.Run("reopen-store", func(t *testing.T) {
		st, err := store.Open(path, store.DefaultOptions().WithMultiIndexing(true))
		require.NoError(t, err)
		defer func() { require.NoError(t, st.Close()) }()

		engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
		require.NoError(t, err)

		res, err := engine.Query(context.Background(), nil, "SELECT id, name, surname, active FROM table1", nil)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			row, err := res.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesByPosition, 4)
			require.Len(t, row.ValuesBySelector, 4)
			require.Nil(t, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())
		}

		_, err = res.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = res.Close()
		require.NoError(t, err)
	})
}

func TestCreateIndex(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, name VARCHAR[256], age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX IF NOT EXISTS ON table1(name)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
	require.ErrorIs(t, err, ErrIndexAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(id)", nil)
	require.ErrorIs(t, err, ErrIndexAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX IF NOT EXISTS ON table1(id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(age)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
	require.ErrorIs(t, err, ErrIndexAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table2(name)", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(title)", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, name, age) VALUES (1, 'name1', 50)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(name, age) VALUES ('name2', 10)", nil)
	require.ErrorIs(t, err, ErrPKCanNotBeNull)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1(active)", nil)
	require.ErrorIs(t, err, ErrLimitedIndexCreation)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)
}

func TestUpsertInto(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, `CREATE TABLE table1 (
								id INTEGER,
								title VARCHAR,
								amount INTEGER,
								active BOOLEAN NOT NULL,
								PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1(amount, active)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title) VALUES (1, 'title1')", nil)
	require.ErrorIs(t, err, ErrNotNullableColumnCannotBeNull)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, age) VALUES (1, 50)", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", nil)
	require.ErrorIs(t, err, ErrMissingParameter)

	params := make(map[string]interface{}, 1)
	params["id"] = [4]byte{1, 2, 3, 4}
	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", params)
	require.ErrorIs(t, err, ErrUnsupportedParameter)

	params = make(map[string]interface{}, 1)
	params["id"] = []byte{1, 2, 3}
	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (@id, 'title1', true)", params)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Contains(t, err.Error(), "is not an integer")

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (1, @title, false)", nil)
	require.ErrorIs(t, err, ErrMissingParameter)

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (1, @title, true)", params)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Contains(t, err.Error(), "is not a string")

	params = make(map[string]interface{}, 1)
	params["title"] = uint64(1)
	params["Title"] = uint64(2)
	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (1, @title, true)", params)
	require.ErrorIs(t, err, ErrDuplicatedParameters)

	_, ctxs, err := engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, amount, active) VALUES (1, 10, true)", nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.Equal(t, ctxs[0].UpdatedRows(), 1)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, amount, active) VALUES (2, 10, true)", nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	t.Run("row with pk 1 should have active in false", func(t *testing.T) {
		_, ctxs, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, amount, active) VALUES (1, 20, false)", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, ctxs[0].UpdatedRows(), 1)

		r, err := engine.Query(context.Background(), nil, "SELECT amount, active FROM table1 WHERE id = 1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)
		require.Equal(t, int64(20), row.ValuesBySelector[EncodeSelector("", "table1", "amount")].RawValue())
		require.False(t, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue().(bool))
		require.Len(t, row.ValuesByPosition, 2)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[EncodeSelector("", "table1", "amount")])
		require.Equal(t, row.ValuesByPosition[1], row.ValuesBySelector[EncodeSelector("", "table1", "active")])

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("row with pk 1 should have active in true", func(t *testing.T) {
		_, ctxs, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, amount, active) VALUES (1, 10, true)", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, ctxs[0].UpdatedRows(), 1)

		r, err := engine.Query(context.Background(), nil, "SELECT amount, active FROM table1 WHERE id = 1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)
		require.Equal(t, int64(10), row.ValuesBySelector[EncodeSelector("", "table1", "amount")].RawValue())
		require.True(t, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue().(bool))

		err = r.Close()
		require.NoError(t, err)
	})

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (Id, Title, Active) VALUES (1, 'some title', false)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (Id, Title, Amount, Active) VALUES (1, 'some title', 100, false)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, amount, active) VALUES (2, 'another title', 200, true)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id) VALUES (1, 'yat')", nil)
	require.ErrorIs(t, err, ErrInvalidNumberOfValues)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, id) VALUES (1, 2)", nil)
	require.ErrorIs(t, err, ErrDuplicatedColumn)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, active) VALUES ('1a', true)", nil)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.ErrorIs(t, err, ErrUnsupportedCast)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, active) VALUES (NULL, false)", nil)
	require.ErrorIs(t, err, ErrPKCanNotBeNull)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title, active) VALUES (2, NULL, true)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (title, active) VALUES ('interesting title', true)", nil)
	require.ErrorIs(t, err, ErrPKCanNotBeNull)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS blob_table (id BLOB[2], PRIMARY KEY id)", nil)
	require.NoError(t, err)
}

func TestUpsertIntoSelect(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil, `CREATE TABLE table1 (
				id INTEGER AUTO_INCREMENT,
				meta JSON,

				PRIMARY KEY id
			)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil, `CREATE TABLE table2 (
				id INTEGER AUTO_INCREMENT,
				name VARCHAR,
				age INTEGER,
				active BOOLEAN,
				created_at TIMESTAMP,

				PRIMARY KEY id
			)`, nil)
	require.NoError(t, err)

	n := 100
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("name%d", i)
		age := 10 + rand.Intn(50)
		active := rand.Intn(2) == 1

		upsert := fmt.Sprintf(
			`INSERT INTO table1 (meta) VALUES ('{"name": "%s", "age": %d, "active": %t, "createdAt": "%s"}')`,
			name,
			age,
			active,
			time.Now().Format("2006-01-02 15:04:05.999999"),
		)
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			upsert,
			nil,
		)
		require.NoError(t, err)
	}

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		`INSERT INTO table2(name, age, active, created_at)
			SELECT meta->'name', meta->'age', meta->'active', meta->'createdAt'::TIMESTAMP
			FROM table1
		`,
		nil,
	)
	require.NoError(t, err)

	rows, err := engine.queryAll(
		context.Background(),
		nil,
		`SELECT t1.meta->'name' = t2.name, t1.meta->'age' = t2.age, t1.meta->'active' = t2.active, t1.meta->'createdAt'::TIMESTAMP = t2.created_at
		FROM table1 AS t1 JOIN table2 AS t2 on t1.id = t2.id`,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 100)

	for _, row := range rows {
		require.Len(t, row.ValuesByPosition, 4)

		for _, v := range row.ValuesByPosition {
			require.True(t, v.RawValue().(bool))
		}
	}
}

func TestInsertIntoEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, payload BLOB[2], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1 (title)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (active)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (payload)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1')", nil)
	require.NoError(t, err)

	t.Run("on conflict cases", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1')", nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

		ntx, ctxs, err := engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1') ON CONFLICT DO NOTHING", nil)
		require.NoError(t, err)
		require.Nil(t, ntx)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
		require.Nil(t, ctxs[0].TxHeader())
	})

	t.Run("on conflict case with multiple rows", func(t *testing.T) {
		ntx, ctxs, err := engine.Exec(context.Background(), nil, `
			INSERT INTO table1 (id, title, active, payload)
			VALUES
				(1, 'title1', true, x'00A1'),
				(11, 'title11', true, x'00B1')
			ON CONFLICT DO NOTHING`, nil)
		require.NoError(t, err)
		require.Nil(t, ntx)
		require.Len(t, ctxs, 1)
		require.Equal(t, 1, ctxs[0].UpdatedRows())
		require.NotNil(t, ctxs[0].TxHeader())

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title11', true, x'00B1')", nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)
	})

	t.Run("varchar key cases", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title123456789', true, x'00A1')", nil)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 10, true, '00A1')", nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("boolean key cases", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', 'true', x'00A1')", nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("blob key cases", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', true, x'00A100A2')", nil)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title1', true, '00A100A2')", nil)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("insertion in table with varchar pk", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE languages (code VARCHAR[128],name VARCHAR[255],PRIMARY KEY code)", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO languages (code,name) VALUES ('code1', 'name1')", nil)
		require.NoError(t, err)
	})
}

func TestAutoIncrementPK(t *testing.T) {
	engine := setupCommonTest(t)

	t.Run("invalid use of auto-increment", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR AUTO_INCREMENT, PRIMARY KEY id)", nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER AUTO_INCREMENT, PRIMARY KEY id)", nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id VARCHAR AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id)", nil)
		require.ErrorIs(t, err, ErrLimitedAutoIncrement)
	})

	_, _, err := engine.Exec(
		context.Background(), nil,
		`
			CREATE TABLE table1 (
				id INTEGER NOT NULL AUTO_INCREMENT,
				title VARCHAR,
				active BOOLEAN,
				PRIMARY KEY id
			)
	`, nil)
	require.NoError(t, err)

	_, ctxs, err := engine.Exec(context.Background(), nil, "INSERT INTO table1(title) VALUES ('name1')", nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].Closed())
	require.Equal(t, int64(1), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, int64(1), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, 1, ctxs[0].UpdatedRows())

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES (1, 'name2')", nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES (1, 'name2') ON CONFLICT DO NOTHING", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1(id, title) VALUES (1, 'name11')", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES (3, 'name3')", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1(id, title) VALUES (5, 'name5')", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES (2, 'name2')", nil)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1(id, title) VALUES (2, 'name2')", nil)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1(id, title) VALUES (3, 'name33')", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES (5, 'name55')", nil)
	require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

	_, ctxs, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(title) VALUES ('name6')", nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].Closed())
	require.Equal(t, int64(6), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, int64(6), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, 1, ctxs[0].UpdatedRows())

	_, ctxs, err = engine.Exec(
		context.Background(), nil,
		`
		BEGIN TRANSACTION;
			INSERT INTO table1(title) VALUES ('name7');
			INSERT INTO table1(title) VALUES ('name8');
		COMMIT;
	`, nil)
	require.NoError(t, err)
	require.Len(t, ctxs, 1)
	require.True(t, ctxs[0].Closed())
	require.Equal(t, int64(7), ctxs[0].FirstInsertedPKs()["table1"])
	require.Equal(t, int64(8), ctxs[0].LastInsertedPKs()["table1"])
	require.Equal(t, 2, ctxs[0].UpdatedRows())
}

func TestDelete(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil,
		`CREATE TABLE table1 (
		id INTEGER,
		title VARCHAR[50],
		active BOOLEAN,
		PRIMARY KEY id
	)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1(title)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)

	params, err := engine.InferParameters(context.Background(), nil, "DELETE FROM table1 WHERE active = @active")
	require.NoError(t, err)
	require.NotNil(t, params)
	require.Len(t, params, 1)
	require.Equal(t, params["active"], BooleanType)

	_, _, err = engine.Exec(context.Background(), nil, "DELETE FROM table2", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "DELETE FROM table1 WHERE name = 'name1'", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	t.Run("delete on empty table should complete without issues", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "DELETE FROM table1", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(
			context.Background(), nil,
			fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)`, i, i, i%2 == 0), nil)
		require.NoError(t, err)
	}

	t.Run("deleting with contradiction should not produce any change", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "DELETE FROM table1 WHERE false", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	t.Run("deleting active rows should remove half of the rows", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "DELETE FROM table1 WHERE active = @active", map[string]interface{}{"active": true})
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, rowCount/2, ctxs[0].UpdatedRows())

		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Len(t, row.ValuesBySelector, 1)
		require.Equal(t, int64(rowCount/2), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())
		require.Len(t, row.ValuesByPosition, 1)
		require.Equal(t, row.ValuesByPosition[0], row.ValuesBySelector[EncodeSelector("", "table1", "col0")])

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1 WHERE active", nil)
		require.NoError(t, err)

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestErrorDuringDelete(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(), nil,
		`
		create table mytable(name varchar[30], primary key name);
		insert into mytable(name) values('name1');
	`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "delete FROM mytable where name=name1", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "delete FROM mytable where name='name1'", nil)
	require.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil,
		`CREATE TABLE table1 (
		id INTEGER,
		title VARCHAR[50],
		active BOOLEAN,
		PRIMARY KEY id
	)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1(title)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)

	params, err := engine.InferParameters(context.Background(), nil, "UPDATE table1 SET active = @active")
	require.NoError(t, err)
	require.NotNil(t, params)
	require.Len(t, params, 1)
	require.Equal(t, params["active"], BooleanType)

	_, _, err = engine.Exec(context.Background(), nil, "UPDATE table2 SET active = false", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "UPDATE table1 SET name = 'name1'", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	t.Run("update on empty table should complete without issues", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "UPDATE table1 SET active = false", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(
			context.Background(), nil,
			fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)`, i, i, i%2 == 0), nil)
		require.NoError(t, err)
	}

	t.Run("updating with contradiction should not produce any change", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "UPDATE table1 SET active = false WHERE false", nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Zero(t, ctxs[0].UpdatedRows())
	})

	t.Run("updating specific row should update only one row", func(t *testing.T) {
		_, ctxs, err := engine.Exec(context.Background(), nil, "UPDATE table1 SET active = true WHERE title = @title", map[string]interface{}{"title": "title1"})
		require.NoError(t, err)
		require.Len(t, ctxs, 1)
		require.Equal(t, 1, ctxs[0].UpdatedRows())

		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(rowCount), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1 WHERE active", nil)
		require.NoError(t, err)

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(rowCount/2+1), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestErrorDuringUpdate(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(), nil,
		`
		create table mytable(id varchar[128], value integer, primary key id);
		insert into mytable(id, value) values('aa',12), ('ab',13);
	`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "update mytable set value=@val where id=@id", nil)
	require.ErrorIs(t, err, ErrMissingParameter)

	params := make(map[string]interface{})
	params["id"] = "ab"
	params["val"] = 15
	_, _, err = engine.Exec(context.Background(), nil, "update mytable set value=@val where id=@id", params)
	require.NoError(t, err)
}

func TestTransactions(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(), nil, `CREATE TABLE table1 (
									id INTEGER,
									title VARCHAR,
									PRIMARY KEY id
								)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `
		COMMIT;
		`, nil)
	require.ErrorIs(t, err, ErrNoOngoingTx)

	_, _, err = engine.Exec(context.Background(), nil, `
		BEGIN TRANSACTION;
			CREATE INDEX ON table2(title);
		COMMIT;
		`, nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, `
		BEGIN TRANSACTION;
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT;
		`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `
		BEGIN TRANSACTION;
			CREATE TABLE table2 (id INTEGER, title VARCHAR[100], age INTEGER, PRIMARY KEY id);
			CREATE INDEX ON table2(title);
		COMMIT;
		`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `
		BEGIN;
			CREATE INDEX ON table2(age);
			INSERT INTO table2 (id, title, age) VALUES (1, 'title1', 40);
		COMMIT;
		`, nil)
	require.NoError(t, err)
}

func TestTransactionsEdgeCases(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithAutocommit(true))
	require.NoError(t, err)

	t.Run("nested tx are not supported", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(), nil, `
		BEGIN TRANSACTION;
			BEGIN TRANSACTION;
				CREATE TABLE table1 (
					id INTEGER,
					title VARCHAR,
					PRIMARY KEY id
				);
			COMMIT;
		COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNestedTxNotSupported)
	})

	_, _, err = engine.Exec(context.Background(), nil, `
		CREATE TABLE table1 (
			id INTEGER,
			title VARCHAR,
			PRIMARY KEY id
		)`, nil)
	require.NoError(t, err)

	t.Run("rollback without explicit transaction should return error", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, `
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			ROLLBACK;
		`, nil)
		require.ErrorIs(t, err, ErrNoOngoingTx)
	})

	t.Run("auto-commit should automatically commit ongoing tx", func(t *testing.T) {
		ntx, ctxs, err := engine.Exec(context.Background(), nil, `
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		`, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 2)
		require.Nil(t, ntx)
	})

	t.Run("explicit tx initialization should automatically commit ongoing tx", func(t *testing.T) {
		engine.autocommit = false

		ntx, ctxs, err := engine.Exec(context.Background(), nil, `
			UPSERT INTO table1 (id, title) VALUES (3, 'title3');
			BEGIN TRANSACTION;
				UPSERT INTO table1 (id, title) VALUES (4, 'title4');
			COMMIT;
		`, nil)
		require.NoError(t, err)
		require.Len(t, ctxs, 2)
		require.Nil(t, ntx)
	})
}

func TestUseSnapshot(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "USE SNAPSHOT SINCE TX 1", nil)
	require.ErrorIs(t, err, ErrNoSupported)

	_, _, err = engine.Exec(context.Background(), nil, `
		BEGIN TRANSACTION;
			UPSERT INTO table1 (id, title) VALUES (1, 'title1');
			UPSERT INTO table1 (id, title) VALUES (2, 'title2');
		COMMIT;
		`, nil)
	require.NoError(t, err)
}

func TestEncodeValue(t *testing.T) {
	b, err := EncodeValue(&Integer{val: int64(1)}, IntegerType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue(&Integer{val: int64(1)}, BooleanType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Integer{val: int64(1)}, VarcharType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Integer{val: int64(1)}, BLOBType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Integer{val: int64(1)}, "invalid type", 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Bool{val: true}, BooleanType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeValue(&Bool{val: true}, IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue(&Varchar{val: "title"}, VarcharType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeValue(&Blob{val: []byte{}}, BLOBType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue(&Blob{val: nil}, BLOBType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeValue(&Varchar{val: "012345678901234567890123456789012"}, VarcharType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue(&Varchar{val: "01234567890123456789012345678902"}, VarcharType, 0)
	require.NoError(t, err)

	_, err = EncodeValue(&Varchar{val: "012345678901234567890123456789012"}, VarcharType, 0)
	require.NoError(t, err)

	b, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2},
	}, BLOBType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1},
	}, BLOBType, 0)
	require.NoError(t, err)

	_, err = EncodeValue(&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2},
	}, BLOBType, 0)
	require.NoError(t, err)

	b, err = EncodeValue((&Integer{val: 1}), IntegerType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue((&Bool{val: true}), IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Bool{val: true}), BooleanType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 1, 1}, b)

	b, err = EncodeValue((&Integer{val: 1}), BooleanType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Varchar{val: "title"}), VarcharType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 5, 't', 'i', 't', 'l', 'e'}, b)

	b, err = EncodeValue((&Integer{val: 1}), VarcharType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Blob{val: []byte{}}), BLOBType, 50)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue((&Blob{val: nil}), BLOBType, 50)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 0}, b)

	b, err = EncodeValue((&Integer{val: 1}), BLOBType, 50)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	b, err = EncodeValue((&Integer{val: 1}), "invalid type", 50)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)

	// Max allowed key size is 32 bytes
	b, err = EncodeValue((&Varchar{val: "012345678901234567890123456789012"}), VarcharType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue((&Varchar{val: "01234567890123456789012345678902"}), VarcharType, 256)
	require.NoError(t, err)

	_, err = EncodeValue((&Varchar{val: "012345678901234567890123456789012"}), VarcharType, 256)
	require.NoError(t, err)

	b, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}), BLOBType, 32)
	require.ErrorIs(t, err, ErrMaxLengthExceeded)
	require.Nil(t, b)

	_, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1,
	}}), BLOBType, 256)
	require.NoError(t, err)

	_, err = EncodeValue((&Blob{val: []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
	}}), BLOBType, 256)
	require.NoError(t, err)

	b, err = EncodeValue((&Timestamp{val: time.Unix(0, 1000)}), TimestampType, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1}, b)

	b, err = EncodeValue((&Integer{val: 1}), TimestampType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)
	require.Nil(t, b)
}

func TestQuery(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, err = engine.Query(context.Background(), nil, "SELECT id FROM table1", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, `CREATE TABLE table1 (
								id INTEGER,
								ts TIMESTAMP,
								title VARCHAR,
								active BOOLEAN,
								payload BLOB,
								PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["id"] = 0

	r, err := engine.Query(context.Background(), nil, "SELECT id FROM table1 WHERE id >= @id", nil)
	require.NoError(t, err)

	orderBy := r.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	rowCount := 10

	start := time.Now()

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(
			context.Background(), nil,
			fmt.Sprintf(`
			UPSERT INTO table1 (id, ts, title, active, payload)
			VALUES (%d, NOW(), 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil)
		require.NoError(t, err)
	}

	t.Run("should resolve every row", func(t *testing.T) {
		r, err = engine.Query(context.Background(), nil, "SELECT * FROM table1 ORDER BY title", nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector(context.Background())
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)
		require.Equal(t, "table1", r.TableAlias())

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "table1", "ts")].RawValue().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "table1", "payload")].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should fail reading due to non-existent column", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id1 FROM table1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.ErrorIs(t, err, ErrColumnDoesNotExist)
		require.Nil(t, row)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with two-time table aliasing", func(t *testing.T) {
		r, err = engine.Query(
			context.Background(), nil,
			fmt.Sprintf(`
			SELECT * FROM table1 AS mytable1 WHERE mytable1.id >= 0 LIMIT %d
		`, rowCount), nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector(context.Background())
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)
		require.Equal(t, "mytable1", r.TableAlias())

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "mytable1", "ts")].RawValue().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "mytable1", "id")].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "mytable1", "title")].RawValue())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "mytable1", "active")].RawValue())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "mytable1", "payload")].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every row with column and two-time table aliasing", func(t *testing.T) {
		r, err = engine.Query(
			context.Background(), nil,
			fmt.Sprintf(`
			SELECT mytable1.id AS D, ts, Title, payload, Active FROM table1 mytable1 WHERE mytable1.id >= 0 LIMIT %d
		`, rowCount), nil)
		require.NoError(t, err)

		colsBySel, err := r.colsBySelector(context.Background())
		require.NoError(t, err)
		require.Len(t, colsBySel, 5)
		require.Equal(t, "mytable1", r.TableAlias())

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 5)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 5)
			require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "mytable1", "ts")].RawValue().(time.Time)))
			require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "mytable1", "d")].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "mytable1", "title")].RawValue())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "mytable1", "active")].RawValue())

			encPayload := []byte(fmt.Sprintf("blob%d", i))
			require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "mytable1", "payload")].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active, payload FROM table1 ORDER BY title", nil)
	require.NoError(t, err)
	require.NotNil(t, r)

	allRows := make([]*Row, rowCount)
	for i := 0; i < rowCount; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)

		allRows[i] = row
	}
	_, err = r.Read(context.Background())
	require.ErrorIs(t, ErrNoMoreRows, err)

	err = r.Close()
	require.NoError(t, err)

	isSorted := sort.SliceIsSorted(allRows, func(i, j int) bool {
		r1 := allRows[i].ValuesByPosition[1]
		r2 := allRows[j].ValuesByPosition[1]

		return r1.RawValue().(string) < r2.RawValue().(string)
	})
	require.True(t, isSorted)

	r, err = engine.Query(context.Background(), nil, "SELECT Id, Title, Active, payload FROM Table1 ORDER BY Id DESC", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)

		require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
		require.Equal(t, (rowCount-1-i)%2 == 0, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())

		encPayload := []byte(fmt.Sprintf("blob%d", rowCount-1-i))
		require.Equal(t, []byte(encPayload), row.ValuesBySelector[EncodeSelector("", "table1", "payload")].RawValue())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id FROM table1 WHERE id", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param1"] = true

	r, err = engine.Query(context.Background(), nil, "SELECT id FROM table1 WHERE active = @some_param1", params)
	require.NoError(t, err)

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())

	err = r.Close()
	require.NoError(t, err)

	params = make(map[string]interface{})
	params["some_param"] = true

	encPayloadPrefix := hex.EncodeToString([]byte("blob"))

	r, err = engine.Query(
		context.Background(), nil,
		fmt.Sprintf(`
		SELECT id, title, active
		FROM table1
		WHERE active = @some_param AND title > 'title' AND payload >= x'%s' AND title LIKE 't'`, encPayloadPrefix), params)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i += 2 {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
		require.Equal(t, params["some_param"], row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE id = 0", nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Len(t, row.ValuesBySelector, 5)

	err = r.Close()
	require.NoError(t, err)

	t.Run("Query with integer division by zero", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id / 0", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrDivisionByZero)

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id % 0", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrDivisionByZero)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("Query with floating-point division by zero", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id / (1.0-1.0)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrDivisionByZero)

		err = r.Close()
		require.NoError(t, err)

		r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id % (1.0-1.0)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrDivisionByZero)

		err = r.Close()
		require.NoError(t, err)
	})

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id = 0 AND NOT active OR active", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	t.Run("Query with integer arithmetics", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id + 1/1 > 1 * (1 - 0)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("Query with floating-point arithmetic", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id + 1.0/1.0 > 1.0 * (1.0 - 0.0)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("Query with boolean expressions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE id = 0 AND NOT active OR active", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("query expressions", func(t *testing.T) {
		reader, err := engine.Query(context.Background(), nil, "SELECT 1, (id + 1) * 2.0, id % 2 = 0 FROM table1", nil)
		require.NoError(t, err)

		cols, err := reader.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 3)

		require.Equal(t, ColDescriptor{Table: "table1", Column: "col0", Type: IntegerType}, cols[0])
		require.Equal(t, ColDescriptor{Table: "table1", Column: "col1", Type: Float64Type}, cols[1])
		require.Equal(t, ColDescriptor{Table: "table1", Column: "col2", Type: BooleanType}, cols[2])

		rows, err := ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, 10)
		require.NoError(t, reader.Close())

		for i, row := range rows {
			require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())
			require.Equal(t, float64((i+1)*2), row.ValuesBySelector[EncodeSelector("", "table1", "col1")].RawValue())
			require.Equal(t, i%2 == 0, row.ValuesBySelector[EncodeSelector("", "table1", "col2")].RawValue())
		}
	})

	t.Run("query with case when then", func(t *testing.T) {
		_, _, err := engine.Exec(
			context.Background(),
			nil,
			`CREATE TABLE employees (
				employee_id INTEGER AUTO_INCREMENT,
				first_name VARCHAR[50],
				last_name VARCHAR[50],
				department VARCHAR[50],
				salary INTEGER,
				hire_date TIMESTAMP,
				job_title VARCHAR[50],

				PRIMARY KEY employee_id
			);`,
			nil,
		)
		require.NoError(t, err)

		n := 100
		for i := 0; i < n; i++ {
			_, _, err := engine.Exec(
				context.Background(),
				nil,
				`INSERT INTO employees(first_name, last_name, department, salary, job_title)
				VALUES (@first_name, @last_name, @department, @salary, @job_title)
				`,
				map[string]interface{}{
					"first_name": fmt.Sprintf("name%d", i),
					"last_name":  fmt.Sprintf("surname%d", i),
					"department": []string{"sales", "manager", "engineering"}[rand.Intn(3)],
					"salary":     []int64{20, 40, 50, 80, 100}[rand.Intn(5)] * 1000,
					"job_title":  []string{"manager", "senior engineer", "executive"}[rand.Intn(3)],
				},
			)
			require.NoError(t, err)
		}

		_, err = engine.queryAll(
			context.Background(),
			nil,
			"SELECT CASE WHEN salary THEN 0 END FROM employees",
			nil,
		)
		require.ErrorIs(t, err, ErrInvalidTypes)

		rows, err := engine.queryAll(
			context.Background(),
			nil,
			`SELECT
				employee_id,
				first_name,
				last_name,
				salary,
				CASE
					WHEN salary < 50000 THEN @low
					WHEN salary >= 50000 AND salary <= 100000 THEN @medium
					ELSE @high
				END AS salary_category
			FROM employees;`,
			map[string]interface{}{
				"low":    "Low",
				"medium": "Medium",
				"high":   "High",
			},
		)
		require.NoError(t, err)
		require.Len(t, rows, n)

		for _, row := range rows {
			salary := row.ValuesByPosition[3].RawValue().(int64)
			category, _ := row.ValuesByPosition[4].RawValue().(string)

			expectedCategory := "High"
			if salary < 50000 {
				expectedCategory = "Low"
			} else if salary >= 50000 && salary <= 100000 {
				expectedCategory = "Medium"
			}
			require.Equal(t, expectedCategory, category)
		}

		rows, err = engine.queryAll(
			context.Background(),
			nil,
			`SELECT
				department,
				job_title,
				CASE department
					WHEN 'sales' THEN
						CASE
							WHEN job_title = 'manager' THEN '20% Bonus'
							ELSE '10% Bonus'
						END
					WHEN 'engineering' THEN
						CASE
							WHEN job_title = 'senior engineer' THEN '15% Bonus'
							ELSE '5% Bonus'
						END
					ELSE
						CASE
							WHEN job_title = 'executive' THEN '12% Bonus'
							ELSE 'No Bonus'
						END
				END AS bonus
			FROM employees;`,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, n)

		for _, row := range rows {
			department := row.ValuesByPosition[0].RawValue().(string)
			job, _ := row.ValuesByPosition[1].RawValue().(string)
			bonus, _ := row.ValuesByPosition[2].RawValue().(string)

			var expectedBonus string
			switch department {
			case "sales":
				if job == "manager" {
					expectedBonus = "20% Bonus"
				} else {
					expectedBonus = "10% Bonus"
				}
			case "engineering":
				if job == "senior engineer" {
					expectedBonus = "15% Bonus"
				} else {
					expectedBonus = "5% Bonus"
				}
			default:
				if job == "executive" {
					expectedBonus = "12% Bonus"
				} else {
					expectedBonus = "No Bonus"
				}
			}
			require.Equal(t, expectedBonus, bonus)
		}

		rows, err = engine.queryAll(
			context.Background(),
			nil,
			`SELECT
				CASE
					WHEN department = 'sales' THEN 'Sales Team'
				END AS department
			FROM employees
			WHERE department != 'sales'
			LIMIT 1
			;`,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Nil(t, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("invalid queries", func(t *testing.T) {
		r, err = engine.Query(context.Background(), nil, "INVALID QUERY", nil)
		require.ErrorIs(t, err, ErrParsingError)
		require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")
		require.Nil(t, r)

		r, err = engine.Query(context.Background(), nil, "UPSERT INTO table1 (id) VALUES(1)", nil)
		require.ErrorIs(t, err, ErrExpectingDQLStmt)
		require.Nil(t, r)

		r, err = engine.Query(context.Background(), nil, "UPSERT INTO table1 (id) VALUES(1); UPSERT INTO table1 (id) VALUES(1)", nil)
		require.ErrorIs(t, err, ErrExpectingDQLStmt)
		require.Nil(t, r)

		r, err = engine.QueryPreparedStmt(context.Background(), nil, nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
		require.Nil(t, r)

		params = make(map[string]interface{})
		params["null_param"] = nil

		r, err = engine.Query(context.Background(), nil, "SELECT id FROM table1 WHERE active = @null_param", params)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("query from values", func(t *testing.T) {
		_, err := engine.queryAll(
			context.Background(),
			nil,
			`
			SELECT * FROM (
				VALUES
					(1, 'foo'),
					(2, true)
			)
			`,
			nil,
		)
		require.ErrorContains(t, err, "cannot match types VARCHAR and BOOLEAN")

		_, err = engine.queryAll(
			context.Background(),
			nil,
			`
			SELECT * FROM (
				VALUES
					(@a),
					(@b)
			)
			`,
			map[string]interface{}{"a": 1, "b": "test"},
		)
		require.ErrorContains(t, err, "cannot match types INTEGER and VARCHAR")

		rows, err := engine.queryAll(
			context.Background(),
			nil,
			`
			SELECT * FROM (
				VALUES
					(1, 'foo', true, 1.22, '2024-11-29'::TIMESTAMP),
					(2, 'bar', false, 1.25, '1996-09-11'::TIMESTAMP),
					(3, 'baz', true, 2.50, '2000-01-01'::TIMESTAMP),
					(4, 'qux', false, 3.75, '2010-05-15'::TIMESTAMP),
					(5, 'quux', true, 0.99, '2022-12-31'::TIMESTAMP)
			)
			`,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 5)

		expectedRows := []*Row{
			{
				ValuesByPosition: []TypedValue{
					&Integer{1}, &Varchar{"foo"}, &Bool{true}, &Float64{1.22}, &Timestamp{time.Date(2024, 11, 29, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				ValuesByPosition: []TypedValue{
					&Integer{2}, &Varchar{"bar"}, &Bool{false}, &Float64{1.25}, &Timestamp{time.Date(1996, 9, 11, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				ValuesByPosition: []TypedValue{
					&Integer{3}, &Varchar{"baz"}, &Bool{true}, &Float64{2.50}, &Timestamp{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				ValuesByPosition: []TypedValue{
					&Integer{4}, &Varchar{"qux"}, &Bool{false}, &Float64{3.75}, &Timestamp{time.Date(2010, 5, 15, 0, 0, 0, 0, time.UTC)},
				},
			},
			{
				ValuesByPosition: []TypedValue{
					&Integer{5}, &Varchar{"quux"}, &Bool{true}, &Float64{0.99}, &Timestamp{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC)},
				},
			},
		}

		for i, row := range rows {
			require.Equal(t, expectedRows[i].ValuesByPosition, row.ValuesByPosition)
		}
	})

	t.Run("constant selection query", func(t *testing.T) {
		_, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT *",
			nil,
		)
		require.ErrorContains(t, err, "SELECT * with no tables specified is not valid")

		assertQueryShouldProduceResults(
			t,
			engine,
			"SELECT 1, true, 'test'",
			"SELECT * FROM (VALUES (1, true, 'test'))",
		)
	})
}

func TestJSON(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))

	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil,
		`
		CREATE TABLE tbl_with_json (
			id INTEGER AUTO_INCREMENT,
			json_data JSON NOT NULL,

			PRIMARY KEY(id)
		)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		`INSERT INTO tbl_with_json(json_data) VALUES ('invalid json value')`,
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		`INSERT INTO tbl_with_json(json_data) VALUES (10)`,
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidValue)

	n := 100
	for i := 0; i < n; i++ {
		data := fmt.Sprintf(
			`{"usr": {"name": "%s", "active": %t, "details": {"age": %d, "city": "%s"}, "perms": ["r", "w"]}}`,
			fmt.Sprintf("name%d", i+1),
			i%2 == 0,
			i+1,
			fmt.Sprintf("city%d", i+1),
		)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			fmt.Sprintf(`INSERT INTO tbl_with_json(json_data) VALUES ('%s')`, data),
			nil,
		)
		require.NoError(t, err)
	}

	t.Run("apply -> operator on non JSON column", func(t *testing.T) {
		_, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT id->'name' FROM tbl_with_json",
			nil,
		)
		require.ErrorContains(t, err, "-> operator cannot be applied on column of type INTEGER")
	})

	t.Run("filter json fields", func(t *testing.T) {
		t.Run("filter boolean value", func(t *testing.T) {
			rows, err := engine.queryAll(
				context.Background(),
				nil,
				`
					SELECT json_data->'usr'
					FROM tbl_with_json
					WHERE json_data->'usr'->'active' = TRUE
				`,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, rows, n/2)

			for i, row := range rows {
				usr, _ := row.ValuesBySelector[EncodeSelector("", "tbl_with_json", "json_data->'usr'")].RawValue().(map[string]interface{})

				require.Equal(t, map[string]interface{}{
					"name":   fmt.Sprintf("name%d", (2*i + 1)),
					"active": true,
					"details": map[string]interface{}{
						"age":  float64((2*i + 1)),
						"city": fmt.Sprintf("city%d", (2*i + 1)),
					},
					"perms": []interface{}{
						"r", "w",
					},
				}, usr)
			}
		})

		t.Run("filter numeric value", func(t *testing.T) {
			rows, err := engine.queryAll(
				context.Background(),
				nil,
				`
					SELECT json_data->'usr'->'name'
					FROM tbl_with_json
					WHERE json_data->'usr'->'details'->'age' + 1 >= 52
				`,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, rows, n/2)

			for i, row := range rows {
				name := row.ValuesByPosition[0].RawValue()
				require.Equal(t, name, fmt.Sprintf("name%d", 51+i))
			}
		})

		t.Run("filter varchar value", func(t *testing.T) {
			rows, err := engine.queryAll(
				context.Background(),
				nil,
				`
					SELECT json_data->'usr'->'name'
					FROM tbl_with_json
					WHERE json_data->'usr'->'name' LIKE '^name.*' AND json_data->'usr'->'perms'->'0' = 'r'
				`,
				nil,
			)
			require.NoError(t, err)
			require.Len(t, rows, n)

			for i, row := range rows {
				name := row.ValuesByPosition[0].RawValue()
				require.Equal(t, name, fmt.Sprintf("name%d", i+1))
			}
		})
	})

	t.Run("order by json field", func(t *testing.T) {
		_, err := engine.queryAll(
			context.Background(),
			nil,
			`
				SELECT json_data
				FROM tbl_with_json
				ORDER BY json_data
			`,
			nil,
		)
		require.ErrorIs(t, err, ErrNotComparableValues)

		rows, err := engine.queryAll(
			context.Background(),
			nil,
			`
				SELECT json_data->'usr', json_data->'usr'->'details'->'age' as age, json_data->'usr'->'details'->'city' as city, json_data->'usr'->'name' as name
				FROM tbl_with_json
				ORDER BY json_data->'usr'->'details'->'age' DESC
			`,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, n)

		for i, row := range rows {
			usr, _ := row.ValuesBySelector[EncodeSelector("", "tbl_with_json", "json_data->'usr'")].RawValue().(map[string]interface{})
			name, _ := row.ValuesBySelector[EncodeSelector("", "tbl_with_json", "name")].RawValue().(string)
			age, _ := row.ValuesBySelector[EncodeSelector("", "tbl_with_json", "age")].RawValue().(float64)
			city, _ := row.ValuesBySelector[EncodeSelector("", "tbl_with_json", "city")].RawValue().(string)

			require.Equal(t, map[string]interface{}{
				"name":   name,
				"active": (n-1-i)%2 == 0,
				"details": map[string]interface{}{
					"age":  age,
					"city": city,
				},
				"perms": []interface{}{"r", "w"},
			}, usr)

			require.Equal(t, fmt.Sprintf("name%d", n-i), name)
			require.Equal(t, float64(n-i), age)
			require.Equal(t, fmt.Sprintf("city%d", n-i), city)
		}
	})

	t.Run("test join on json field", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER AUTO_INCREMENT, value VARCHAR, PRIMARY KEY(id))", nil)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1(value) VALUES (@name)", map[string]interface{}{"name": fmt.Sprintf("name%d", i+1)})
			require.NoError(t, err)
		}

		rows, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT table1.value, json_data->'usr'->'name' FROM tbl_with_json JOIN table1 ON table1.value = tbl_with_json.json_data->'usr'->'name' ORDER BY table1.id",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 10)

		for i, row := range rows {
			require.Len(t, row.ValuesByPosition, 2)
			require.Equal(t, fmt.Sprintf("name%d", i+1), row.ValuesByPosition[0].RawValue())
			require.Equal(t, row.ValuesByPosition[0].RawValue(), row.ValuesByPosition[1].RawValue())
		}
	})

	_, _, err = engine.Exec(context.Background(), nil, "DELETE FROM tbl_with_json", nil)
	require.NoError(t, err)

	randJson := func(src *rand.Rand) interface{} {
		switch src.Intn(6) {
		case 0:
			return src.Float64()
		case 1:
			return fmt.Sprintf("string%d", src.Int63())
		case 2:
			return src.Int()%2 == 0
		case 3:
			return map[string]interface{}{
				"test": "value",
			}
		case 4:
			return []interface{}{"test", true, 10.5}
		}
		return nil
	}

	seed := time.Now().UnixNano()
	src := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		data := randJson(src)

		jsonData, err := json.Marshal(data)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"INSERT INTO tbl_with_json(json_data) VALUES (@data)",
			map[string]interface{}{"data": string(jsonData)},
		)
		require.NoError(t, err)
	}

	t.Run("lookup field", func(t *testing.T) {
		rows, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT json_data, json_data->'test' FROM tbl_with_json",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, n)

		for _, row := range rows {
			data := row.ValuesByPosition[0].RawValue()
			value := row.ValuesByPosition[1].RawValue()
			if _, isObject := data.(map[string]interface{}); isObject {
				require.Equal(t, "value", row.ValuesByPosition[1].RawValue())
			} else {
				require.Nil(t, value)
			}
		}
	})

	t.Run("query json with mixed types", func(t *testing.T) {
		rows, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT json_data FROM tbl_with_json",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, n)

		stringValues := 0

		src := rand.New(rand.NewSource(seed))
		for _, row := range rows {
			s := row.ValuesByPosition[0].RawValue()
			require.Equal(t, randJson(src), s)

			if _, ok := s.(string); ok {
				stringValues++
			}
		}

		rows, err = engine.queryAll(
			context.Background(),
			nil,
			"SELECT COUNT(*) FROM tbl_with_json WHERE json_typeof(json_data) = 'STRING'",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(stringValues))
	})

	t.Run("update json data", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			fmt.Sprintf(`UPDATE tbl_with_json SET json_data = '%d' WHERE json_typeof(json_data) = 'STRING'`, rand.Int63()),
			nil,
		)
		require.NoError(t, err)

		rows, err := engine.queryAll(
			context.Background(),
			nil,
			"SELECT COUNT(*) FROM tbl_with_json WHERE json_typeof(json_data) = 'STRING'",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Zero(t, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("cannot index json column", func(t *testing.T) {
		_, _, err = engine.Exec(
			context.Background(),
			nil,
			"CREATE INDEX ON tbl_with_json(json_data);", nil)
		require.ErrorIs(t, err, ErrCannotIndexJson)

		_, _, err = engine.Exec(
			context.Background(), nil,
			`
			CREATE TABLE test (
				json_data JSON NOT NULL,

				PRIMARY KEY(json_data)
			)`, nil)
		require.ErrorIs(t, err, ErrCannotIndexJson)
	})
}

func TestQueryCornerCases(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))

	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil,
		`
		CREATE TABLE table1 (
			id INTEGER AUTO_INCREMENT,
			PRIMARY KEY(id)
		)`, nil)
	require.NoError(t, err)

	res, err := engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
	require.NoError(t, err)

	err = res.Close()
	require.NoError(t, err)

	t.Run("run out of snapshots", func(t *testing.T) {
		// Get one tx that takes the snapshot
		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		res, err = engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
		require.ErrorIs(t, err, tbtree.ErrorToManyActiveSnapshots)
		require.Nil(t, res)

		res, err = engine.Query(context.Background(), tx, "SELECT * FROM table1", nil)
		require.NoError(t, err)

		err = res.Close()
		require.NoError(t, err)

		err = tx.Cancel()
		require.NoError(t, err)
	})

	t.Run("invalid query parameters", func(t *testing.T) {
		_, err := engine.Query(context.Background(), nil, "SELECT * FROM table1", map[string]interface{}{
			"param": "value",
			"Param": "value",
		})
		require.ErrorIs(t, err, ErrDuplicatedParameters)
	})
}

func TestQueryDistinct(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	opts := DefaultOptions().WithPrefix(sqlPrefix).WithDistinctLimit(4)
	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil, `CREATE TABLE table1 (
								id INTEGER AUTO_INCREMENT,
								title VARCHAR,
								amount INTEGER,
								active BOOLEAN,
								PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table1 (title, amount, active) VALUES
								('title1', 100, NULL),
								('title2', 200, false),
								('title3', 200, true),
								('title4', 300, NULL)`, nil)
	require.NoError(t, err)

	t.Run("should return all titles", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT title FROM table1 WHERE id <= @id", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.title)", cols[0].Selector())

		for i := 1; i <= 3; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return two titles", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT title FROM table1 WHERE id <= @id LIMIT 2", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.title)", cols[0].Selector())

		for i := 1; i <= 2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return two titles starting from the second one", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT title FROM table1 WHERE id <= @id LIMIT 2 OFFSET 1", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.title)", cols[0].Selector())

		for i := 2; i <= 3; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return two distinct amounts", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT amount FROM table1 WHERE id <= @id", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.amount)", cols[0].Selector())

		for i := 1; i <= 2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)
			require.Equal(t, int64(i*100), row.ValuesBySelector["(table1.amount)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return rows with null, false and true", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT active FROM table1 WHERE id <= @id", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.active)", cols[0].Selector())

		for i := 0; i <= 2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)

			if i == 0 {
				require.Nil(t, row.ValuesBySelector["(table1.active)"].RawValue())
				continue
			}

			require.Equal(t, i == 2, row.ValuesBySelector["(table1.active)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return three rows", func(t *testing.T) {
		params := make(map[string]interface{})
		params["id"] = 3

		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT amount, active FROM table1 WHERE id <= @id", params)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 2)
		require.Equal(t, "(table1.amount)", cols[0].Selector())
		require.Equal(t, "(table1.active)", cols[1].Selector())

		for i := 0; i <= 2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 2)

			if i == 0 {
				require.Equal(t, int64(100), row.ValuesBySelector["(table1.amount)"].RawValue())
				require.Nil(t, row.ValuesBySelector["(table1.active)"].RawValue())
				continue
			}

			require.Equal(t, int64(200), row.ValuesBySelector["(table1.amount)"].RawValue())
			require.Equal(t, i == 2, row.ValuesBySelector["(table1.active)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return too many rows error", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT DISTINCT id FROM table1", nil)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 1)
		require.Equal(t, "(table1.id)", cols[0].Selector())

		for i := 0; i < engine.distinctLimit; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Len(t, row.ValuesBySelector, 1)

			require.Equal(t, int64(i+1), row.ValuesBySelector["(table1.id)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrTooManyRows)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestIndexSelection(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `CREATE TABLE table1 (
		v0 INTEGER,
		v1 INTEGER,
		v2 INTEGER,
		v3 INTEGER,
		v4 INTEGER,
		v5 INTEGER,

		PRIMARY KEY (v0, v1)
	)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX on table1(v1, v2, v3, v4)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX on table1(v3, v4)", nil)
	require.NoError(t, err)

	type test struct {
		query                   string
		expectedIndex           []string
		expectedGroupBySortCols []string
		expectedOrderBySortCols []string
		desc                    bool
	}

	testCases := []test{
		{
			query:         "SELECT * FROM table1",
			expectedIndex: []string{"v0", "v1"},
		},
		{
			query:         "SELECT * FROM table1 ORDER BY v1",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
		},
		{
			query:         "SELECT * FROM table1 WHERE v0 = 0 ORDER BY v1",
			expectedIndex: []string{"v0", "v1"},
		},
		{
			query:                   "SELECT * FROM table1 ORDER BY v5 DESC",
			expectedIndex:           []string{"v0", "v1"},
			expectedOrderBySortCols: []string{EncodeSelector("", "table1", "v5")},
		},
		{
			query:         "SELECT * FROM table1 ORDER BY v1, v2, v3",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
		},
		{
			query:         "SELECT * FROM table1 WHERE v1 = 0 AND v2 = 1 ORDER BY v3 DESC",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
			desc:          true,
		},
		{
			query:         "SELECT * FROM table1 ORDER BY v3 DESC, v4 DESC",
			expectedIndex: []string{"v3", "v4"},
			desc:          true,
		},
		{
			query:                   "SELECT * FROM table1 ORDER BY v3 DESC, v4 ASC",
			expectedIndex:           []string{"v0", "v1"},
			expectedOrderBySortCols: []string{EncodeSelector("", "table1", "v3"), EncodeSelector("", "table1", "v4")},
		},
		{
			query:                   "SELECT * FROM table1 USE INDEX ON (v1, v2, v3, v4) ORDER BY v3 DESC, v4 DESC",
			expectedIndex:           []string{"v1", "v2", "v3", "v4"},
			expectedOrderBySortCols: []string{EncodeSelector("", "table1", "v3"), EncodeSelector("", "table1", "v4")},
		},
		{
			query:         "SELECT COUNT(*) FROM table1 GROUP BY v1, v2",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
		},
		{
			query:                   "SELECT COUNT(*) FROM table1 GROUP BY v1, v3",
			expectedIndex:           []string{"v0", "v1"},
			expectedGroupBySortCols: []string{EncodeSelector("", "table1", "v1"), EncodeSelector("", "table1", "v3")},
		},
		{
			query:                   "SELECT COUNT(*) FROM table1 GROUP BY v1, v2, v3 ORDER BY v1 DESC, v3 DESC",
			expectedIndex:           []string{"v1", "v2", "v3", "v4"},
			expectedOrderBySortCols: []string{EncodeSelector("", "table1", "v1"), EncodeSelector("", "table1", "v3")},
		},
		{
			query:         "SELECT COUNT(*) FROM table1 WHERE v1 = 0 AND v2 = 1 GROUP BY v2, v3 ORDER BY v3 DESC",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
			desc:          true,
		},
		{
			query:         "SELECT COUNT(*) FROM table1 GROUP BY v1, v2, v3 ORDER BY v1 DESC, v2 DESC",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
			desc:          true,
		},
		{
			query:         "SELECT COUNT(*) FROM table1 GROUP BY v1, v2, v3 ORDER BY v1, v2, v3, COUNT(*)",
			expectedIndex: []string{"v1", "v2", "v3", "v4"},
			expectedOrderBySortCols: []string{
				EncodeSelector("", "table1", "v1"),
				EncodeSelector("", "table1", "v2"),
				EncodeSelector("", "table1", "v3"),
				EncodeSelector("COUNT", "table1", "*"),
			},
		},
		{
			query:                   "SELECT COUNT(*) FROM table1 GROUP BY v4, v3 ORDER BY v3 DESC, v4 DESC",
			expectedIndex:           []string{"v0", "v1"},
			expectedGroupBySortCols: []string{EncodeSelector("", "table1", "v4"), EncodeSelector("", "table1", "v3")},
			expectedOrderBySortCols: []string{EncodeSelector("", "table1", "v3"), EncodeSelector("", "table1", "v4")},
		},
		{
			query:                   "SELECT COUNT(*) FROM table1 USE INDEX ON(v3, v4) GROUP BY v1, v2 ORDER BY v1 DESC, v2 DESC",
			expectedIndex:           []string{"v3", "v4"},
			expectedGroupBySortCols: []string{EncodeSelector("", "table1", "v1"), EncodeSelector("", "table1", "v2")},
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("test index selection %d", i), func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, testCase.query, nil)
			require.NoError(t, err)
			defer reader.Close()

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, len(testCase.expectedIndex))
			require.Equal(t, specs.DescOrder, testCase.desc)
			require.Len(t, specs.groupBySortExps, len(testCase.expectedGroupBySortCols))
			require.Len(t, specs.orderBySortExps, len(testCase.expectedOrderBySortCols))

			for i, col := range testCase.expectedIndex {
				require.Equal(t, col, specs.Index.cols[i].Name())
			}

			for i, col := range testCase.expectedGroupBySortCols {
				require.Equal(t, col, EncodeSelector(specs.groupBySortExps[i].AsSelector().resolve("table1")))
			}

			for i, col := range testCase.expectedOrderBySortCols {
				require.Equal(t, col, EncodeSelector(specs.orderBySortExps[i].AsSelector().resolve("table1")))
			}
		})
	}
}

func TestIndexing(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `CREATE TABLE table1 (
								id INTEGER AUTO_INCREMENT,
								ts INTEGER,
								title VARCHAR[20],
								active BOOLEAN,
								amount INTEGER,
								payload BLOB,
								PRIMARY KEY id
							)`, nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (ts)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1 (title, amount)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (active, title)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE UNIQUE INDEX ON table1 (title)", nil)
	require.NoError(t, err)

	t.Run("should fail due to unique index", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (ts, title, amount, active) VALUES (1, 'title1', 10, true), (2, 'title1', 10, false)", nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)
	})

	t.Run("should use primary index by default", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 ORDER BY id DESC", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 ORDER BY ts", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 ORDER BY ts DESC", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE ts = 1629902962 OR ts < 1629902963 ORDER BY ts", nil)
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
		require.Equal(t, int64(1629902963), tsRange.hRange.val.RawValue())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 AS t WHERE t.ts = 1629902962 AND t.ts = 1629902963 ORDER BY t.ts", nil)
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
		require.Equal(t, int64(1629902963), tsRange.lRange.val.RawValue())
		require.NotNil(t, tsRange.hRange)
		require.True(t, tsRange.hRange.inclusive)
		require.Equal(t, int64(1629902962), tsRange.hRange.val.RawValue())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` with specific value", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE ts > 1629902962 AND ts < 1629902963 ORDER BY ts", nil)
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
		require.Equal(t, int64(1629902962), tsRange.lRange.val.RawValue())
		require.NotNil(t, tsRange.hRange)
		require.False(t, tsRange.hRange.inclusive)
		require.Equal(t, int64(1629902963), tsRange.hRange.val.RawValue())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title, amount` in asc order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title, amount) ORDER BY title", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title) ORDER BY title", nil)
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
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (ts)", nil)
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

	t.Run("should use specified index on `ts` when ordering by `title`", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (ts) ORDER BY title", nil)
		require.NoError(t, err)

		scanSpecs := r.ScanSpecs()
		require.NotNil(t, scanSpecs)
		require.Len(t, scanSpecs.Index.cols, 1)
		require.Equal(t, scanSpecs.Index.cols[0].colName, "ts")

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` with max value in desc order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' ORDER BY title DESC", nil)
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
		require.Equal(t, "title10", titleRange.hRange.val.RawValue())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title,amount` in desc order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE title = 'title1' ORDER BY amount DESC", nil)
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
		require.Equal(t, "title1", titleRange.lRange.val.RawValue())
		require.NotNil(t, titleRange.hRange)
		require.True(t, titleRange.hRange.inclusive)
		require.Equal(t, "title1", titleRange.hRange.val.RawValue())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` ascending order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE title > 'title10' ORDER BY ts ASC", nil)
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
		require.Equal(t, "title10", titleRange.lRange.val.RawValue())
		require.Nil(t, titleRange.hRange)

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `ts` descending order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY ts DESC", nil)
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
		require.Equal(t, "title1", titleRange.lRange.val.RawValue())
		require.Nil(t, titleRange.hRange)

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` descending order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 WHERE title > 'title10' or title = 'title1' ORDER BY title DESC", nil)
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
		require.Equal(t, "title1", titleRange.lRange.val.RawValue())
		require.Nil(t, titleRange.hRange)

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order starting with 'title1'", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title) WHERE title > 'title10' or title = 'title1' ORDER BY title", nil)
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
		require.Equal(t, "title1", titleRange.lRange.val.RawValue())
		require.Nil(t, titleRange.hRange)

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` ascending order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' or title = 'title1' ORDER BY title", nil)
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
		require.Equal(t, "title10", titleRange.hRange.val.RawValue())

		require.False(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should use index on `title` descending order", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON (title) WHERE title < 'title10' and title = 'title1' ORDER BY title DESC", nil)
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
		require.Equal(t, "title1", titleRange.lRange.val.RawValue())
		require.NotNil(t, titleRange.hRange)
		require.True(t, titleRange.hRange.inclusive)
		require.Equal(t, "title1", titleRange.hRange.val.RawValue())

		require.True(t, scanSpecs.DescOrder)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestExecCornerCases(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	tx, _, err := engine.Exec(context.Background(), nil, "INVALID STATEMENT", nil)
	require.ErrorIs(t, err, ErrParsingError)
	require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")
	require.Nil(t, tx)
}

func TestQueryWithNullables(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, ts TIMESTAMP, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, ts, title) VALUES (1, TIME(), 'title1')", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	rowCount := 10

	start := time.Now()

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table1 (id, ts, title) VALUES (%d, NOW(), 'title%d')", i, i), nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, "SELECT id, ts, title, active FROM table1 WHERE NOT(active != NULL)", nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)
		require.False(t, start.After(row.ValuesBySelector[EncodeSelector("", "table1", "ts")].RawValue().(time.Time)))
		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
		require.Equal(t, &NullValue{t: BooleanType}, row.ValuesBySelector[EncodeSelector("", "table1", "active")])
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestOrderBy(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithSortBufferSize(1024))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		`CREATE TABLE table1 (
			id INTEGER,
			title VARCHAR[100],
			age INTEGER,
			height FLOAT,
			weight FLOAT,
			created_at TIMESTAMP,

			PRIMARY KEY id
		)`,
		nil,
	)
	require.NoError(t, err)

	_, err = engine.Query(context.Background(), nil, "SELECT id, title, age FROM table2 ORDER BY title", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.Query(context.Background(), nil, "SELECT id, title, age FROM table1 ORDER BY amount", nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	rowCount := 100 + rand.Intn(engine.sortBufferSize-100) // [100, sortBufferSize]

	for id := 1; id <= rowCount; id++ {
		rand.Seed(int64(id))

		params := map[string]interface{}{
			"id":      id,
			"title":   fmt.Sprintf("title%d", rand.Intn(100)),
			"age":     rand.Intn(100),
			"weight":  50 + rand.Float64()*50,
			"height":  rand.Float64() * 200,
			"created": time.Unix(rand.Int63n(100000), 0).UTC(),
		}
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, age, height, weight, created_at) VALUES (@id, @title, @age, @height, @weight, @created)", params)
		require.NoError(t, err)
	}

	checkDataIntegrity := func(t *testing.T, rows []*Row, table string) {
		require.Len(t, rows, rowCount)

		ids := make(map[int64]struct{})
		for _, row := range rows {
			id := row.ValuesBySelector[EncodeSelector("", table, "id")].RawValue().(int64)
			rand.Seed(int64(id))

			title := row.ValuesBySelector[EncodeSelector("", table, "title")].RawValue().(string)
			age := row.ValuesBySelector[EncodeSelector("", table, "age")].RawValue().(int64)
			height := row.ValuesBySelector[EncodeSelector("", table, "height")].RawValue().(float64)
			weight := row.ValuesBySelector[EncodeSelector("", table, "weight")].RawValue().(float64)
			created := row.ValuesBySelector[EncodeSelector("", table, "created_at")].RawValue().(time.Time).UTC()

			require.Equal(t, fmt.Sprintf("title%d", rand.Intn(100)), title)
			require.Equal(t, int64(rand.Intn(100)), age)
			require.Equal(t, 50+rand.Float64()*50, weight)
			require.Equal(t, rand.Float64()*200, height)
			require.Equal(t, time.Unix(rand.Int63n(100000), 0).UTC(), created)

			_, exists := ids[id]
			require.False(t, exists)

			ids[id] = struct{}{}
		}

		for id := 1; id <= rowCount; id++ {
			_, exists := ids[int64(id)]
			require.True(t, exists)
		}
	}

	type test struct {
		exps           []string
		directions     []int
		expectedIndex  []string
		positionalRefs []int
	}

	testCases := []test{
		{
			exps:           []string{"age"},
			directions:     []int{1},
			positionalRefs: []int{3},
		},
		{
			exps:           []string{"created_at"},
			directions:     []int{-1},
			positionalRefs: []int{6},
		},
		{
			exps:           []string{"title", "age"},
			directions:     []int{-1, 1},
			positionalRefs: []int{2, 3},
		},
		{
			exps:           []string{"age", "title", "height"},
			directions:     []int{1, -1, 1},
			positionalRefs: []int{3, 2, 4},
		},
		{
			exps:       []string{"weight/(height*height)"},
			directions: []int{1},
		},
		{
			exps:           []string{"height", "weight"},
			directions:     []int{1, -1},
			positionalRefs: []int{4, 5},
		},
		{
			exps:       []string{"weight/(height*height)"},
			directions: []int{1},
		},
	}

	runTest := func(t *testing.T, test *test, expectedTempFiles int) []*Row {
		orderByCols := make([]string, len(test.exps))
		for i, col := range test.exps {
			orderByCols[i] = col + " " + directionToSql(test.directions[i])
		}

		reader, err := engine.Query(context.Background(), nil, fmt.Sprintf("SELECT * FROM table1 ORDER BY %s", strings.Join(orderByCols, ",")), nil)
		require.NoError(t, err)
		defer reader.Close()

		rows, err := ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, rowCount)

		specs := reader.ScanSpecs()

		if test.expectedIndex != nil {
			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, len(test.expectedIndex))

			for i, col := range test.expectedIndex {
				require.Equal(t, col, specs.Index.cols[i].Name())
			}
		} else {
			require.Len(t, specs.orderBySortExps, len(orderByCols))
			for i, col := range specs.orderBySortExps {
				e, err := ParseExpFromString(test.exps[i])
				require.NoError(t, err)
				require.Equal(t, e, col.exp)
			}
		}

		checkRowsAreSorted(t, rows, test.exps, test.directions, "table1")
		checkDataIntegrity(t, rows, "table1")

		if test.positionalRefs != nil {
			orderByColPositions := make([]string, len(test.exps))
			for i, ref := range test.positionalRefs {
				orderByColPositions[i] = strconv.Itoa(ref) + " " + directionToSql(test.directions[i])
			}

			rows1, err := engine.queryAll(
				context.Background(),
				nil,
				fmt.Sprintf("SELECT * FROM table1 ORDER BY %s", strings.Join(orderByColPositions, ",")),
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, rows, rows1)
		}

		tx := reader.Tx()
		require.Len(t, tx.tempFiles, expectedTempFiles)

		return rows
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("order by on %s should be executed using in memory sort", strings.Join(test.exps, ",")), func(t *testing.T) {
			runTest(t, &test, 0)
		})
	}

	engine.sortBufferSize = 4 + rand.Intn(13) // [4, 16]

	for _, test := range testCases {
		t.Run(fmt.Sprintf("order by on %s should be executed using file sort", strings.Join(test.exps, ",")), func(t *testing.T) {
			runTest(t, &test, 2)
		})
	}

	t.Run("order by on top of subquery", func(t *testing.T) {
		reader, err := engine.Query(context.Background(), nil, "SELECT age FROM (SELECT id, title, age FROM table1 AS t1) ORDER BY age DESC", nil)
		require.NoError(t, err)
		defer reader.Close()

		rows, err := ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, rowCount)

		checkRowsAreSorted(t, rows, []string{EncodeSelector("", "t1", "age")}, []int{-1}, "t1")
	})

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(age)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(title, age)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(age, title, height)", nil)
	require.NoError(t, err)

	testCases = []test{
		{
			exps:          []string{"age"},
			directions:    []int{1},
			expectedIndex: []string{"age"},
		},
		{
			exps:          []string{"title"},
			directions:    []int{-1},
			expectedIndex: []string{"title", "age"},
		},
		{
			exps:          []string{"title", "age"},
			directions:    []int{1, 1},
			expectedIndex: []string{"title", "age"},
		},
		{
			exps:          []string{"age", "title"},
			directions:    []int{-1, -1, -1},
			expectedIndex: []string{"age", "title", "height"},
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("order by on %s should be executed using index", strings.Join(test.exps, ",")), func(t *testing.T) {
			runTest(t, &test, 0)
		})
	}

	t.Run("order by with preferred index", func(t *testing.T) {
		t.Run("sorting required", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON(title, age) ORDER BY title ASC, age DESC", nil)
			require.NoError(t, err)
			defer reader.Close()

			specs := reader.ScanSpecs()

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount)

			require.Len(t, specs.orderBySortExps, 2)
			require.Equal(t, EncodeSelector(specs.orderBySortExps[0].AsSelector().resolve("table1")), EncodeSelector("", "table1", "title"))
			require.Equal(t, EncodeSelector(specs.orderBySortExps[1].AsSelector().resolve("table1")), EncodeSelector("", "table1", "age"))

			checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "title"), EncodeSelector("", "table1", "age")}, []int{1, -1}, "table1")
			checkDataIntegrity(t, rows, "table1")
		})

		t.Run("sorting not required", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 USE INDEX ON(title, age) ORDER BY title DESC, age DESC", nil)
			require.NoError(t, err)
			defer reader.Close()

			specs := reader.ScanSpecs()

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount)
			require.Len(t, specs.orderBySortExps, 0)

			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, 2)

			require.Equal(t, "title", specs.Index.cols[0].Name())
			require.Equal(t, "age", specs.Index.cols[1].Name())

			checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "title"), EncodeSelector("", "table1", "age")}, []int{-1, -1}, "table1")
			checkDataIntegrity(t, rows, "table1")
		})
	})

	nullValues := 1 + rand.Intn(10)
	for i := 1; i <= nullValues; i++ {
		params := map[string]interface{}{
			"id":      rowCount + i,
			"title":   nil,
			"age":     nil,
			"height":  nil,
			"created": nil,
		}
		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, age, height, created_at) VALUES (@id, @title, @age, @height, @created)", params)
		require.NoError(t, err)
	}

	t.Run("order by with null values", func(t *testing.T) {
		reader, err := engine.Query(context.Background(), nil, "SELECT id, title, age, height, weight, created_at FROM table1 ORDER BY title, id", nil)
		require.NoError(t, err)
		defer reader.Close()

		rows, err := ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, rowCount+nullValues)

		for i := 1; i <= nullValues; i++ {
			row := rows[i-1]
			require.Equal(t, row.ValuesByPosition[0].RawValue(), int64(i+rowCount))

			for _, v := range row.ValuesByPosition[1:] {
				require.Nil(t, v.RawValue())
			}
		}

		tx := reader.Tx()
		require.Len(t, tx.tempFiles, 2)

		checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "title"), EncodeSelector("", "table1", "id")}, []int{1, 1}, "table1")
		checkDataIntegrity(t, rows[nullValues:], "table1")
	})
}

func directionToSql(direction int) string {
	if direction == 1 {
		return "ASC"
	}
	return "DESC"
}

func checkRowsAreSorted(t *testing.T, rows []*Row, expStrings []string, directions []int, table string) {
	exps := make([]ValueExp, len(expStrings))
	for i, s := range expStrings {
		e, err := ParseExpFromString(s)
		require.NoError(t, err)
		exps[i] = e
	}

	k1 := make(Tuple, len(exps))
	k2 := make(Tuple, len(exps))

	isSorted := sort.SliceIsSorted(rows, func(i, j int) bool {
		for idx, e := range exps {
			v1, err := e.reduce(nil, rows[i], table)
			require.NoError(t, err)

			v2, err := e.reduce(nil, rows[j], table)
			require.NoError(t, err)

			k1[idx] = v1
			k2[idx] = v2
		}

		res, idx, err := Tuple(k1).Compare(k2)
		require.NoError(t, err)

		if idx >= 0 {
			return res*directions[idx] < 0
		}
		return false
	})
	require.True(t, isSorted)
}

func TestQueryWithRowFiltering(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(
			context.Background(), nil,
			fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE false", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE false OR true", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read(context.Background())
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE 1 < 2", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount; i++ {
		_, err := r.Read(context.Background())
		require.NoError(t, err)
	}

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE 1 >= 2", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE 1 = true", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE NOT table1.active", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read(context.Background())
		require.NoError(t, err)
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE table1.id > 4", nil)
	require.NoError(t, err)

	for i := 0; i < rowCount/2; i++ {
		_, err := r.Read(context.Background())
		require.NoError(t, err)
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table1 (id, title) VALUES (%d, 'title%d')", rowCount, rowCount), nil)
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title FROM table1 WHERE active = null AND payload = null", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title FROM table1 WHERE active = null AND payload = null AND active = payload", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = r.Close()
	require.NoError(t, err)
}

func TestQueryWithInClause(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR[50], active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf(`
			INSERT INTO table1 (id, title, active) VALUES (%d, 'title%d', %v)
		`, i, i, i%2 == 0), nil)
		require.NoError(t, err)
	}

	inListExp := &InListExp{}
	require.False(t, inListExp.isConstant())

	t.Run("infer parameters without parameters should return an empty list", func(t *testing.T) {
		params, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title IN ('title0', 'title1') ORDER BY title")
		require.NoError(t, err)
		require.Empty(t, params)
	})

	t.Run("infer inference with wrong types should return an error", func(t *testing.T) {
		_, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE 100 + title IN ('title0', 'title1')")
		require.ErrorIs(t, err, ErrInvalidTypes)
	})

	t.Run("infer inference with valid types should succeed", func(t *testing.T) {
		params, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE active AND title IN ('title0', 'title1')")
		require.NoError(t, err)
		require.Empty(t, params)
	})

	t.Run("infer parameters should return matching type", func(t *testing.T) {
		params, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title IN (@param0, @param1)")
		require.NoError(t, err)
		require.Len(t, params, 2)
		require.Equal(t, VarcharType, params["param0"])
		require.Equal(t, VarcharType, params["param1"])
	})

	t.Run("infer parameters with type conflicts should return an error", func(t *testing.T) {
		_, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE active = @param1 and title IN (@param0, @param1)")
		require.ErrorIs(t, err, ErrInferredMultipleTypes)
	})

	t.Run("infer parameters with unexistent column should return an error", func(t *testing.T) {
		_, err := engine.InferParameters(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE invalidColumn IN ('title1', 'title2')")
		require.ErrorIs(t, err, ErrColumnDoesNotExist)
	})

	t.Run("in clause with invalid column should return an error", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE invalidColumn IN (1, 2)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrColumnDoesNotExist)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause with invalid type should return an error", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title IN (1, 2)", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNotComparableValues)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading two rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title IN ('title0', 'title1')", nil)
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause with invalid values should return an error", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title IN ('title0', true + 'title1')", nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrInvalidValue)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading rows NOT included in 'IN' clause", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT id, title, active FROM table1 WHERE title NOT IN ('title1', 'title0')", nil)
		require.NoError(t, err)

		for i := 2; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("in clause should succeed reading using 'IN' clause in join condition", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1 as t1 INNER JOIN table1 as t2 ON t1.title IN (t2.title) ORDER BY title", nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "t1", "title")].RawValue())
		}

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestAggregations(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(
		context.Background(),
		nil,
		`
		CREATE TABLE table1 (
			id INTEGER,
			title VARCHAR,
			age INTEGER,
			active BOOLEAN,
			payload BLOB,
			PRIMARY KEY(id)
		)
		`,
		nil,
	)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(age)", nil)
	require.NoError(t, err)

	rowCount := 10
	base := 30

	nullRows := map[int]bool{
		3: true,
		5: true,
		6: true,
	}

	ageSum := 0

	for i := 1; i <= rowCount; i++ {
		params := make(map[string]interface{}, 3)

		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		if _, setToNull := nullRows[i]; setToNull {
			params["age"] = nil
		} else {
			params["age"] = base + i
			ageSum += base + i
		}

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, age) VALUES (@id, @title, @age)", params)
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1 WHERE id < i", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id FROM table1 WHERE false", nil)
	require.NoError(t, err)

	row, err := r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
	require.Nil(t, row)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, `
		SELECT COUNT(*), SUM(age), MIN(title), MAX(age), AVG(age), MIN(active), MAX(active), MIN(payload)
		FROM table1 WHERE false`, nil)
	require.NoError(t, err)

	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "col0")].RawValue())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "col1")].RawValue())
	require.Equal(t, "", row.ValuesBySelector[EncodeSelector("", "table1", "col2")].RawValue())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "col3")].RawValue())
	require.Equal(t, int64(0), row.ValuesBySelector[EncodeSelector("", "table1", "col4")].RawValue())

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) AS c, SUM(age), MIN(age), MAX(age), AVG(age) FROM table1 AS t1", nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 5)

	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 5)

	require.Equal(t, int64(rowCount), row.ValuesBySelector[EncodeSelector("", "t1", "c")].RawValue())

	require.Equal(t, int64(ageSum), row.ValuesBySelector[EncodeSelector("", "t1", "col1")].RawValue())

	require.Equal(t, int64(1+base), row.ValuesBySelector[EncodeSelector("", "t1", "col2")].RawValue())

	require.Equal(t, int64(base+rowCount), row.ValuesBySelector[EncodeSelector("", "t1", "col3")].RawValue())

	require.Equal(t, int64(ageSum/(rowCount-len(nullRows))), row.ValuesBySelector[EncodeSelector("", "t1", "col4")].RawValue())

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestCount(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE t1(id INTEGER AUTO_INCREMENT, val1 INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON t1(val1)", nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO t1(val1) VALUES($1)", map[string]interface{}{"param1": j})
			require.NoError(t, err)
		}
	}

	r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM t1", nil)
	require.NoError(t, err)

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, uint64(30), row.ValuesBySelector["(t1.c)"].RawValue())

	err = r.Close()
	require.NoError(t, err)

	//_, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM t1 GROUP BY val1", nil)
	//require.ErrorIs(t, err, ErrLimitedGroupBy)

	r, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM t1 GROUP BY val1 ORDER BY val1", nil)
	require.NoError(t, err)

	for j := 0; j < 3; j++ {
		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.EqualValues(t, uint64(10), row.ValuesBySelector["(t1.c)"].RawValue())
	}

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestGroupBy(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, title VARCHAR[128], age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)

	t.Run("selecting aggregations only with empty table should return zero values", func(t *testing.T) {
		rows, err := engine.queryAll(context.Background(), nil, "SELECT COUNT(*), SUM(age), AVG(age), MIN(title), MAX(title) FROM table1", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(0))
		require.Equal(t, rows[0].ValuesByPosition[1].RawValue(), int64(0))
		require.Equal(t, rows[0].ValuesByPosition[2].RawValue(), int64(0))
		require.Equal(t, rows[0].ValuesByPosition[3].RawValue(), "")
		require.Equal(t, rows[0].ValuesByPosition[4].RawValue(), "")
	})

	t.Run("query with empty table and group by should return no rows", func(t *testing.T) {
		rows, err := engine.queryAll(context.Background(), nil, "SELECT COUNT(*), SUM(age) FROM table1 GROUP BY title", nil)
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("columns should appear in group by or aggregations", func(t *testing.T) {
		_, err = engine.queryAll(context.Background(), nil, "SELECT COUNT(*), age FROM table1", nil)
		require.ErrorIs(t, err, ErrColumnMustAppearInGroupByOrAggregation)

		_, err = engine.queryAll(context.Background(), nil, "SELECT COUNT(*), SUM(age), title FROM table1 GROUP BY active", nil)
		require.ErrorIs(t, err, ErrColumnMustAppearInGroupByOrAggregation)

		_, err = engine.queryAll(context.Background(), nil, "SELECT COUNT(*), SUM(age) FROM table1 GROUP BY active ORDER BY title", nil)
		require.ErrorIs(t, err, ErrColumnMustAppearInGroupByOrAggregation)

		_, err = engine.queryAll(context.Background(), nil, "SELECT COUNT(*), MIN(age) FROM table1 GROUP BY age ORDER BY MAX(age) DESC", nil)
		require.ErrorIs(t, err, ErrColumnMustAppearInGroupByOrAggregation)
	})

	rowCount := 10

	params := make(map[string]interface{}, 3)
	for n := 0; n < rowCount; n++ {
		m := n + 1
		for i := 1; i <= m; i++ {
			active := m%2 == 0
			params["title"] = fmt.Sprintf("title%d", m)
			params["age"] = i
			params["active"] = active

			_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (title, age, active) VALUES (@title, @age, @active)", params)
			require.NoError(t, err)
		}
	}

	t.Run("simple group by", func(t *testing.T) {
		rows, err := engine.queryAll(context.Background(), nil, "SELECT COUNT(*) FROM table1", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(rowCount*(rowCount+1)/2))

		reader, err := engine.Query(context.Background(), nil, "SELECT title, COUNT(*), SUM(age), MIN(age), MAX(age), AVG(age) FROM table1 GROUP BY title", nil)
		require.NoError(t, err)

		specs := reader.ScanSpecs()
		require.NotNil(t, specs.Index)
		require.True(t, specs.Index.IsPrimary())
		require.False(t, specs.DescOrder)

		rows, err = ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, rowCount)
		require.NoError(t, reader.Close())

		isSorted := sort.SliceIsSorted(rows, func(i, j int) bool {
			res, err := rows[i].ValuesByPosition[0].Compare(rows[j].ValuesByPosition[0])
			require.NoError(t, err)
			return res < 0
		})
		require.True(t, isSorted)

		for _, row := range rows {
			m, err := strconv.ParseInt(strings.TrimPrefix(row.ValuesByPosition[0].RawValue().(string), "title"), 10, 64)
			require.NoError(t, err)
			require.Equal(t, int64(m), row.ValuesByPosition[1].RawValue().(int64))
			require.Equal(t, int64(m*(m+1)/2), row.ValuesByPosition[2].RawValue().(int64))
			require.Equal(t, int64(1), row.ValuesByPosition[3].RawValue().(int64))
			require.Equal(t, int64(m), row.ValuesByPosition[4].RawValue().(int64))
			require.Equal(t, int64((m+1)/2), row.ValuesByPosition[5].RawValue().(int64))
		}
	})

	t.Run("aggregated functions with no group by", func(t *testing.T) {
		rows, err := engine.queryAll(context.Background(), nil, "SELECT COUNT(*) FROM table1", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(rowCount*(rowCount+1)/2))
	})

	t.Run("group by with no aggregations should select distinct values", func(t *testing.T) {
		rows, err := engine.queryAll(context.Background(), nil, "SELECT age FROM table1 GROUP BY age", nil)
		require.NoError(t, err)
		require.Len(t, rows, rowCount)

		for i, row := range rows {
			require.Equal(t, row.ValuesByPosition[0].RawValue(), int64(i+1))
		}
	})

	t.Run("group by with order by", func(t *testing.T) {
		t.Run("group by fields are covered by order by fields", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), title FROM table1 GROUP BY title ORDER BY title", nil)
			require.NoError(t, err)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.True(t, specs.Index.IsPrimary())
			require.False(t, specs.DescOrder)

			orderBy := reader.OrderBy()
			require.Equal(t, orderBy[0].Selector(), EncodeSelector("", "table1", "title"))
			require.NoError(t, reader.Close())
		})

		t.Run("order by fields are covered by group by fields", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), age, title FROM table1 GROUP BY title, age ORDER BY title DESC", nil)
			require.NoError(t, err)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.True(t, specs.Index.IsPrimary())
			require.False(t, specs.DescOrder)

			orderBy := reader.OrderBy()
			require.Equal(t, orderBy[0].Selector(), EncodeSelector("", "table1", "title"))
			require.Equal(t, orderBy[1].Selector(), EncodeSelector("", "table1", "age"))

			require.NoError(t, reader.Close())
		})

		t.Run("order by fields dont't cover group by fields", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), age, title FROM table1 GROUP BY title, age ORDER BY age DESC, title DESC", nil)
			require.NoError(t, err)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.True(t, specs.Index.IsPrimary())
			require.False(t, specs.DescOrder)

			orderBy := reader.OrderBy()
			require.Equal(t, orderBy[0].Selector(), EncodeSelector("", "table1", "age"))
			require.Equal(t, orderBy[1].Selector(), EncodeSelector("", "table1", "title"))

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount*(rowCount+1)/2)
			require.NoError(t, reader.Close())

			isSorted := sort.SliceIsSorted(rows, func(i, j int) bool {
				res, err := rows[i].ValuesByPosition[1].Compare(rows[j].ValuesByPosition[1])
				require.NoError(t, err)
				return res > 0
			})
			require.True(t, isSorted)

			for j, n := 0, 0; n < rowCount; n++ {
				for i := 0; i <= n; i++ {
					require.Equal(t, int64(rowCount-n), rows[j].ValuesByPosition[1].RawValue())
					j++
				}
			}
		})

		t.Run("order by aggregated function", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), MAX(age) FROM table1 ORDER BY MAX(age)", nil)
			require.NoError(t, err)
			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(rowCount*(rowCount+1)/2))
			require.Equal(t, rows[0].ValuesByPosition[1].RawValue(), int64(rowCount))
			require.NoError(t, reader.Close())

			reader, err = engine.Query(context.Background(), nil, "SELECT title, COUNT(*), SUM(age) FROM table1 GROUP BY title ORDER BY SUM(table1.age) DESC", nil)
			require.NoError(t, err)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.True(t, specs.Index.IsPrimary())
			require.False(t, specs.DescOrder)

			orderBy := reader.OrderBy()
			require.Equal(t, orderBy[0].Selector(), EncodeSelector("SUM", "table1", "age"))

			rows, err = ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount)

			require.NoError(t, reader.Close())

			for i, row := range rows {
				n := rowCount - i
				require.Len(t, row.ValuesByPosition, 3)
				require.Equal(t, row.ValuesByPosition[0].RawValue(), fmt.Sprintf("title%d", n))
				require.Equal(t, row.ValuesByPosition[1].RawValue(), int64(n))
				require.Equal(t, row.ValuesByPosition[2].RawValue(), int64(n*(n+1)/2))
			}
		})
	})

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX on table1(age)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX on table1(title, age)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX on table1(title, age, id)", nil)
	require.NoError(t, err)

	t.Run("group by with indexes", func(t *testing.T) {
		t.Run("group by covered by index", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT title, COUNT(*) FROM table1 GROUP BY title", nil)
			require.NoError(t, err)
			defer reader.Close()

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, 2)

			require.Equal(t, specs.Index.cols[0].Name(), "title")
			require.Equal(t, specs.Index.cols[1].Name(), "age")

			for _, row := range rows {
				m, err := strconv.ParseInt(strings.TrimPrefix(row.ValuesByPosition[0].RawValue().(string), "title"), 10, 64)
				require.NoError(t, err)
				require.Equal(t, row.ValuesByPosition[1].RawValue().(int64), m)
			}
		})

		t.Run("group by and order by covered by index", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), title, age, id FROM table1 GROUP BY title, age, id ORDER BY title DESC, id ASC, age ASC", nil)
			require.NoError(t, err)
			defer reader.Close()

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount*(rowCount+1)/2)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, 3)

			require.Equal(t, specs.Index.cols[0].Name(), "title")
			require.Equal(t, specs.Index.cols[1].Name(), "age")
			require.Equal(t, specs.Index.cols[2].Name(), "id")

			for _, row := range rows {
				require.Equal(t, row.ValuesByPosition[0].RawValue().(int64), int64(1))
			}
			checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "title"), EncodeSelector("", "table1", "age"), EncodeSelector("", "table1", "id")}, []int{-1, 1, 1}, "table1")
		})

		t.Run("index covers group by but not order by", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), title, age FROM table1 GROUP BY title, age ORDER BY age DESC", nil)
			require.NoError(t, err)
			defer reader.Close()

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount*(rowCount+1)/2)

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.Index.cols, 2)

			require.Equal(t, specs.Index.cols[0].Name(), "title")
			require.Equal(t, specs.Index.cols[1].Name(), "age")

			for _, row := range rows {
				require.Equal(t, row.ValuesByPosition[0].RawValue().(int64), int64(1))
			}
			checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "age")}, []int{-1}, "table1")
		})

		t.Run("preferred index doesn't cover group by and order by", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, "SELECT COUNT(*), title, age FROM table1 USE INDEX ON(age) GROUP BY title, age ORDER BY age DESC", nil)
			require.NoError(t, err)
			defer reader.Close()

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.groupBySortExps, 2)
			require.Len(t, specs.orderBySortExps, 1)
			require.Len(t, specs.Index.cols, 1)
			require.Equal(t, specs.Index.cols[0].Name(), "age")

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount*(rowCount+1)/2)

			for _, row := range rows {
				require.Equal(t, row.ValuesByPosition[0].RawValue().(int64), int64(1))
			}
			checkRowsAreSorted(t, rows, []string{EncodeSelector("", "table1", "age")}, []int{-1}, "table1")
		})

		t.Run("index covers group by and order by because of unitary filter", func(t *testing.T) {
			reader, err := engine.Query(context.Background(), nil, fmt.Sprintf("SELECT COUNT(*), title, age FROM table1 WHERE title = 'title%d' GROUP BY title, age ORDER BY age DESC", rowCount), nil)
			require.NoError(t, err)
			defer reader.Close()

			specs := reader.ScanSpecs()
			require.NotNil(t, specs.Index)
			require.Len(t, specs.groupBySortExps, 0)
			require.Len(t, specs.orderBySortExps, 0)

			require.Len(t, specs.Index.cols, 2)
			require.Equal(t, specs.Index.cols[0].Name(), "title")
			require.Equal(t, specs.Index.cols[1].Name(), "age")

			rows, err := ReadAllRows(context.Background(), reader)
			require.NoError(t, err)
			require.Len(t, rows, rowCount)

			expectedTitle := fmt.Sprintf("title%d", rowCount)
			for i, row := range rows {
				require.Equal(t, row.ValuesByPosition[0].RawValue().(int64), int64(1))
				require.Equal(t, row.ValuesByPosition[1].RawValue().(string), expectedTitle)
				require.Equal(t, row.ValuesByPosition[2].RawValue().(int64), int64(rowCount-i))
			}
		})
	})

	t.Run("group by with subquery", func(t *testing.T) {
		reader, err := engine.Query(context.Background(), nil, fmt.Sprintf("SELECT COUNT(*), title FROM (SELECT * FROM table1 WHERE title = 'title%d') GROUP BY title", rowCount), nil)
		require.NoError(t, err)
		defer reader.Close()

		rows, err := ReadAllRows(context.Background(), reader)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].ValuesByPosition[0].RawValue(), int64(rowCount))
	})
}

func TestGroupByHaving(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, age INTEGER, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(active)", nil)
	require.NoError(t, err)

	rowCount := 10
	base := 40

	for i := 0; i < rowCount; i++ {
		params := make(map[string]interface{}, 4)
		params["id"] = i
		params["title"] = fmt.Sprintf("title%d", i)
		params["age"] = base + i
		params["active"] = i%2 == 0

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (id, title, age, active) VALUES (@id, @title, @age, @active)", params)
		require.NoError(t, err)
	}

	_, err = engine.Query(context.Background(), nil, "SELECT active, COUNT(*), SUM(age1) FROM table1 WHERE active != null HAVING AVG(age) >= MIN(age)", nil)
	require.ErrorIs(t, err, ErrHavingClauseRequiresGroupClause)

	r, err := engine.Query(context.Background(), nil, `
			SELECT active, COUNT(*), SUM(age1)
			FROM table1
			WHERE active != null
			GROUP BY active
			HAVING AVG(age) >= MIN(age)
			ORDER BY active`, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)
	require.Nil(t, r)

	r, err = engine.Query(context.Background(), nil, `
			SELECT active, COUNT(*), SUM(age1)
			FROM table1
			WHERE AVG(age) >= MIN(age)
			GROUP BY active
			ORDER BY active`, nil)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)
	require.Nil(t, r)

	r, err = engine.Query(context.Background(), nil, "SELECT active, COUNT(id) FROM table1 GROUP BY active ORDER BY active", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrLimitedCount)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, `
			SELECT active, COUNT(*)
			FROM table1
			GROUP BY active
			HAVING AVG(age) >= MIN(age1)
			ORDER BY active`, nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, `
		SELECT active, COUNT(*) as c, MIN(age), MAX(age), AVG(age), SUM(age)
		FROM table1
		GROUP BY active
		HAVING COUNT(*) <= SUM(age) AND
				MIN(age) <= MAX(age) AND
				AVG(age) <= MAX(age) AND
				MAX(age) < SUM(age)  AND
				AVG(age) >= MIN(age) AND
				SUM(age) > 0
		ORDER BY active DESC`, nil)
	require.NoError(t, err)

	_, err = r.Columns(context.Background())
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 6)

		require.Equal(t, i == 0, row.ValuesBySelector[EncodeSelector("", "table1", "active")].RawValue())

		require.Equal(t, int64(rowCount/2), row.ValuesBySelector[EncodeSelector("", "table1", "c")].RawValue())

		if i%2 == 0 {
			require.Equal(t, int64(base), row.ValuesBySelector[EncodeSelector("", "table1", "col2")].RawValue())
			require.Equal(t, int64(base+rowCount-2), row.ValuesBySelector[EncodeSelector("", "table1", "col3")].RawValue())
		} else {
			require.Equal(t, int64(base+1), row.ValuesBySelector[EncodeSelector("", "table1", "col2")].RawValue())
			require.Equal(t, int64(base+rowCount-1), row.ValuesBySelector[EncodeSelector("", "table1", "col3")].RawValue())
		}
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestJoins(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, fkid2 INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table2 (id INTEGER, amount INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table11 RENAME TO table3", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table1 RENAME TO table3", nil)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf(`
			UPSERT INTO table1 (id, title, fkid1, fkid2) VALUES (%d, 'title%d', %d, %d)`, i, i, rowCount-1-i, i), nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table2 (id, amount) VALUES (%d, %d)", rowCount-1-i, i*i), nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil)
		require.NoError(t, err)
	}

	t.Run("should not find any matching row", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
		SELECT table1.title, table2.amount, table3.age
		FROM (SELECT * FROM table2 WHERE amount = 1)
		INNER JOIN table1 ON table2.id = table1.fkid1 AND (table2.amount > 0 OR table2.amount > 0+1)
		INNER JOIN table3 ON table1.fkid2 = table3.id AND table3.age < 30`, nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should find one matching row", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
		SELECT t1.title, t2.amount, t3.age
		FROM (SELECT id, amount FROM table2 WHERE amount = 1) AS t2
		INNER JOIN table1 AS t1 ON t2.id = t1.fkid1 AND t2.amount > 0
		INNER JOIN table3 AS t3 ON t1.fkid2 = t3.id AND t3.age > 30`, nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Len(t, row.ValuesBySelector, 3)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should resolve every inserted row", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
			SELECT id, title, table2.amount, table3.age
			FROM table1 INNER JOIN table2 ON table1.fkid1 = table2.id
			INNER JOIN table3 ON table1.fkid2 = table3.id
			WHERE table1.id >= 0 AND table3.age >= 30
			ORDER BY id DESC`, nil)
		require.NoError(t, err)

		cols, err := r.Columns(context.Background())
		require.NoError(t, err)
		require.Len(t, cols, 4)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Len(t, row.ValuesBySelector, 4)

			require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "table1", "id")].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "table1", "title")].RawValue())
			require.Equal(t, int64((rowCount-1-i)*(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "table2", "amount")].RawValue())
			require.Equal(t, int64(30+(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "table3", "age")].RawValue())
		}

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("should return error when joining nonexistent table", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
		SELECT title
		FROM table1
		INNER JOIN table22 ON table1.id = table11.fkid1`, nil)
		require.NoError(t, err)

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrTableDoesNotExist)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestJoinsWithNullIndexes(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE table1 (id INTEGER, fkid2 INTEGER, PRIMARY KEY id);
		CREATE TABLE table2 (id INTEGER, id2 INTEGER, val INTEGER, PRIMARY KEY id);
		CREATE INDEX ON table2(id2);

		INSERT INTO table2(id, id2, val) VALUES (1, 1, 100), (2, null, 200);
		INSERT INTO table1(id, fkid2) VALUES (10, 1), (20, null);
	`, nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil, `
			SELECT table2.val
			FROM table1 INNER JOIN table2 ON table1.fkid2 = table2.id2
			ORDER BY table1.id`, nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 1)

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 1)
	require.EqualValues(t, 100, row.ValuesBySelector[EncodeSelector("", "table2", "val")].RawValue())

	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Len(t, row.ValuesBySelector, 1)
	require.EqualValues(t, 200, row.ValuesBySelector[EncodeSelector("", "table2", "val")].RawValue())

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = r.Close()
	require.NoError(t, err)
}

func TestJoinsWithJointTable(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table2 (id INTEGER AUTO_INCREMENT, amount INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table12 (id INTEGER AUTO_INCREMENT, fkid1 INTEGER, fkid2 INTEGER, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table1 (name) VALUES ('name1'), ('name2'), ('name3')", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table2 (amount) VALUES (10), (20), (30)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table12 (fkid1, fkid2, active) VALUES (1,1,false),(1,2,true),(1,3,true)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table12 (fkid1, fkid2, active) VALUES (2,1,false),(2,2,false),(2,3,true)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table12 (fkid1, fkid2, active) VALUES (3,1,false),(3,2,false),(3,3,false)", nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil, `
		SELECT q.name, t2.amount, t12.active
		FROM (SELECT * FROM table1 where name = 'name1') q
		INNER JOIN table12 t12 on t12.fkid1 = q.id
		INNER JOIN table2 t2  on t12.fkid2 = t2.id
		WHERE t12.active = true`, nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 3)

	for i := 0; i < 2; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 3)

		require.Equal(t, "name1", row.ValuesBySelector[EncodeSelector("", "q", "name")].RawValue())
		require.Equal(t, int64(20+i*10), row.ValuesBySelector[EncodeSelector("", "t2", "amount")].RawValue())
		require.Equal(t, true, row.ValuesBySelector[EncodeSelector("", "t12", "active")].RawValue())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestNestedJoins(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, fkid1 INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table2 (id INTEGER, amount INTEGER, fkid1 INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table3 (id INTEGER, age INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table1 (id, title, fkid1) VALUES (%d, 'title%d', %d)", i, i, rowCount-1-i), nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table2 (id, amount, fkid1) VALUES (%d, %d, %d)", rowCount-1-i, i*i, i), nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf("UPSERT INTO table3 (id, age) VALUES (%d, %d)", i, 30+i), nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, `
		SELECT id, title, t2.amount AS total_amount, t3.age
		FROM table1 t1
		INNER JOIN table2 t2 ON (fkid1 = t2.id AND title != NULL)
		INNER JOIN table3 t3 ON t2.fkid1 = t3.id
		ORDER BY id DESC`, nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 4)

	for i := 0; i < rowCount; i++ {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 4)

		require.Equal(t, int64(rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "t1", "id")].RawValue())
		require.Equal(t, fmt.Sprintf("title%d", rowCount-1-i), row.ValuesBySelector[EncodeSelector("", "t1", "title")].RawValue())
		require.Equal(t, int64((rowCount-1-i)*(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "t2", "total_amount")].RawValue())
		require.Equal(t, int64(30+(rowCount-1-i)), row.ValuesBySelector[EncodeSelector("", "t3", "age")].RawValue())
	}

	err = r.Close()
	require.NoError(t, err)
}

func TestLeftJoins(t *testing.T) {
	e := setupCommonTest(t)

	_, _, err := e.Exec(
		context.Background(),
		nil,
		`
		CREATE TABLE customers (
			customer_id INTEGER,
			customer_name VARCHAR(50),
			email VARCHAR(100),

			PRIMARY KEY customer_id
		);

		CREATE TABLE products (
			product_id INTEGER,
			product_name VARCHAR(50),
			price FLOAT,

			PRIMARY KEY product_id
		);

		CREATE TABLE orders (
			order_id INTEGER,
			customer_id INTEGER,
			order_date TIMESTAMP,

			PRIMARY KEY order_id
		);

		CREATE TABLE order_items (
			order_item_id INTEGER,
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,

			PRIMARY KEY order_item_id
		);

		INSERT INTO customers (customer_id, customer_name, email)
		VALUES
		(1, 'Alice Johnson', 'alice@example.com'),
		(2, 'Bob Smith', 'bob@example.com'),
		(3, 'Charlie Brown', 'charlie@example.com');

		INSERT INTO products (product_id, product_name, price)
		VALUES
		(1, 'Laptop', 1200.00),
		(2, 'Smartphone', 800.00),
		(3, 'Tablet', 400.00);

		INSERT INTO orders (order_id, customer_id, order_date)
		VALUES
		(101, 1, '2024-11-01'::TIMESTAMP),
		(102, 2, '2024-11-02'::TIMESTAMP),
		(103, 1, '2024-11-03'::TIMESTAMP);

		INSERT INTO order_items (order_item_id, order_id, product_id, quantity)
		VALUES
		(1, 101, 1, 2),
		(2, 101, 2, 1),
		(3, 102, 3, 3),
		(4, 103, 2, 2);
		`,
		nil,
	)
	require.NoError(t, err)

	assertQueryShouldProduceResults(
		t,
		e,
		`SELECT c.customer_id, c.customer_name, c.email, o.order_id, o.order_date
		FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id
		ORDER BY c.customer_id, o.order_date;`,
		`
		SELECT *
		FROM (
			VALUES
				(1, 'Alice Johnson', 'alice@example.com', 101, '2024-11-01'::TIMESTAMP),
				(1, 'Alice Johnson', 'alice@example.com', 103, '2024-11-03'::TIMESTAMP),
				(2, 'Bob Smith', 'bob@example.com', 102, '2024-11-02'::TIMESTAMP),
				(3, 'Charlie Brown', 'charlie@example.com', NULL, NULL)
		)`,
	)

	assertQueryShouldProduceResults(
		t,
		e,
		`
		SELECT
			c.customer_name,
			c.email,
			o.order_id,
			o.order_date,
			p.product_name,
			oi.quantity,
			p.price,
			(oi.quantity * p.price) AS total_price
		FROM
			products p
		LEFT JOIN order_Items oi ON p.product_id = oi.product_id
		LEFT JOIN orders o ON oi.order_id = o.order_id
		LEFT JOIN customers c ON o.customer_id = c.customer_id
		ORDER BY o.order_date, c.customer_name;`,
		`
		SELECT *
		FROM (
			VALUES
				('Alice Johnson', 'alice@example.com', 101, '2024-11-01'::TIMESTAMP, 'Laptop', 2, 1200.00, 2400.00),
				('Alice Johnson', 'alice@example.com', 101, '2024-11-01'::TIMESTAMP, 'Smartphone', 1, 800.00, 800.00),
				('Bob Smith', 'bob@example.com', 102, '2024-11-02'::TIMESTAMP, 'Tablet', 3, 400.00, 1200.00),
				('Alice Johnson', 'alice@example.com', 103, '2024-11-03'::TIMESTAMP, 'Smartphone', 2, 800.00, 1600.00)
		)`,
	)
}

func TestReOpening(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, name VARCHAR[30], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
	require.NoError(t, err)

	engine, err = NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, name VARCHAR[30], PRIMARY KEY id)", nil)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(name)", nil)
	require.ErrorIs(t, err, ErrIndexAlreadyExists)
}

func TestSubQuery(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR, active BOOLEAN, payload BLOB, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 0; i < rowCount; i++ {
		encPayload := hex.EncodeToString([]byte(fmt.Sprintf("blob%d", i)))
		_, _, err = engine.Exec(context.Background(), nil, fmt.Sprintf(`
			UPSERT INTO table1 (id, title, active, payload) VALUES (%d, 'title%d', %v, x'%s')
		`, i, i, i%2 == 0, encPayload), nil)
		require.NoError(t, err)
	}

	r, err := engine.Query(context.Background(), nil, `
		SELECT id, title t
		FROM (SELECT id, title, active FROM table1) t2
		WHERE active AND t2.id >= 0`, nil)
	require.NoError(t, err)

	cols, err := r.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 2)

	for i := 0; i < rowCount; i += 2 {
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Len(t, row.ValuesBySelector, 2)

		require.Equal(t, int64(i), row.ValuesBySelector[EncodeSelector("", "t2", "id")].RawValue())
		require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector[EncodeSelector("", "t2", "t")].RawValue())
	}

	err = r.Close()
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "UPSERT INTO table1 (id, title) VALUES (0, 'title0')", nil)
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE active", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil, "SELECT id, title, active FROM (SELECT id, title, active FROM table1) WHERE title", nil)
	require.NoError(t, err)

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = r.Close()
	require.NoError(t, err)
}

func TestJoinsWithSubquery(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithAutocommit(true))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `
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
	`, nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil, `
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
		nil)
	require.NoError(t, err)

	row, err := r.Read(context.Background())
	require.NoError(t, err)

	require.Len(t, row.ValuesBySelector, 5)
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "c", "id")].RawValue())
	require.Equal(t, "Isidro Behnen", row.ValuesBySelector[EncodeSelector("", "c", "customer_name")].RawValue())
	require.Equal(t, int64(24), row.ValuesBySelector[EncodeSelector("", "c", "age")].RawValue())
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "r", "customerid")].RawValue())
	require.Equal(t, int64(1), row.ValuesBySelector[EncodeSelector("", "r", "review_count")].RawValue())

	err = r.Close()
	require.NoError(t, err)
}

func TestInferParameters(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	stmt := "CREATE DATABASE db1"

	params, err := engine.InferParameters(context.Background(), nil, stmt)
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParametersPreparedStmts(context.Background(), nil, []SQLStmt{&CreateDatabaseStmt{}})
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParameters(context.Background(), nil, stmt)
	require.NoError(t, err)
	require.Empty(t, params)

	params, err = engine.InferParametersPreparedStmts(context.Background(), nil, []SQLStmt{&CreateDatabaseStmt{}})
	require.NoError(t, err)
	require.Empty(t, params)

	stmt = "CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"

	_, _, err = engine.Exec(context.Background(), nil, stmt, nil)
	require.NoError(t, err)

	_, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, title) VALUES (@id, @title);")
	require.NoError(t, err)

	_, err = engine.InferParameters(context.Background(), nil, "invalid sql stmt")
	require.ErrorIs(t, err, ErrParsingError)
	require.EqualError(t, err, "parsing error: syntax error: unexpected IDENTIFIER at position 7")

	_, err = engine.InferParametersPreparedStmts(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	params, err = engine.InferParameters(context.Background(), nil, stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters(context.Background(), nil, "USE DATABASE db1")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters(context.Background(), nil, "USE SNAPSHOT BEFORE TX 10")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters(context.Background(), nil, stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	pstmt, err := ParseSQL(strings.NewReader(stmt))
	require.NoError(t, err)
	require.Len(t, pstmt, 1)

	_, err = engine.InferParametersPreparedStmts(context.Background(), nil, pstmt)
	require.NoError(t, err)

	params, err = engine.InferParameters(context.Background(), nil, "ALTER TABLE mytableSE ADD COLUMN note VARCHAR")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters(context.Background(), nil, "ALTER TABLE mytableSE RENAME COLUMN note TO newNote")
	require.NoError(t, err)
	require.Len(t, params, 0)

	stmt = "CREATE INDEX ON mytable(active)"

	params, err = engine.InferParameters(context.Background(), nil, stmt)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.Exec(context.Background(), nil, stmt, nil)
	require.NoError(t, err)

	params, err = engine.InferParameters(context.Background(), nil, "BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@id, @title); COMMIT;")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, VarcharType, params["title"])

	params, err = engine.InferParameters(context.Background(), nil, "BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@id, @title); ROLLBACK;")
	require.NoError(t, err)
	require.Len(t, params, 2)

	params, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, title) VALUES (1, 'title1')")
	require.NoError(t, err)
	require.Len(t, params, 0)

	params, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, title) VALUES (1, 'title1'), (@id2, @title2)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id2"])
	require.Equal(t, VarcharType, params["title2"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE (id - 1) > (@id + (@id+1))")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable t1 INNER JOIN mytable t2 ON t1.id = t2.id WHERE id > @id")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["id"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > @id AND (NOT @active OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["id"])
	require.Equal(t, BooleanType, params["active"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > ? AND (NOT ? OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, IntegerType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > $2 AND (NOT $1 OR active)")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, IntegerType, params["param2"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT COUNT(*) FROM mytable GROUP BY active HAVING @param1 = COUNT(*) ORDER BY active")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT COUNT(*), MIN(id) FROM mytable GROUP BY active HAVING @param1 < MIN(id) ORDER BY active")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @active AND title LIKE 't+'")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["active"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM TABLES() WHERE name = @tablename")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, VarcharType, params["tablename"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM INDEXES('mytable') idxs WHERE idxs.\"unique\" = @unique")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["unique"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM COLUMNS('mytable') WHERE name = @column")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, VarcharType, params["column"])
}

func TestInferParametersPrepared(t *testing.T) {
	engine := setupCommonTest(t)

	stmts, err := ParseSQL(strings.NewReader("CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)"))
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	params, err := engine.InferParametersPreparedStmts(context.Background(), nil, stmts)
	require.NoError(t, err)
	require.Len(t, params, 0)

	_, _, err = engine.ExecPreparedStmts(context.Background(), nil, stmts, nil)
	require.NoError(t, err)
}

func TestInferParametersUnbounded(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	params, err := engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 = @param2")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, AnyType, params["param1"])
	require.Equal(t, AnyType, params["param2"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 AND @param2")
	require.NoError(t, err)
	require.Len(t, params, 2)
	require.Equal(t, BooleanType, params["param1"])
	require.Equal(t, BooleanType, params["param2"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 != NULL")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, AnyType, params["param1"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 != NOT NULL")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 AND active)")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, BooleanType, params["param1"])

	params, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE @param1 != NULL AND (@param1 <= mytable.id)")
	require.NoError(t, err)
	require.Len(t, params, 1)
	require.Equal(t, IntegerType, params["param1"])
}

func TestInferParametersInvalidCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE mytable(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, title) VALUES (@param1, @param1)")
	require.ErrorIs(t, err, ErrInferredMultipleTypes)

	_, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, title) VALUES (@param1)")
	require.ErrorIs(t, err, ErrInvalidNumberOfValues)

	_, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable1(id, title) VALUES (@param1, @param2)")
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.InferParameters(context.Background(), nil, "INSERT INTO mytable(id, note) VALUES (@param1, @param2)")
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > @param1 AND (@param1 OR active)")
	require.ErrorIs(t, err, ErrInferredMultipleTypes)

	_, err = engine.InferParameters(context.Background(), nil, "BEGIN TRANSACTION; INSERT INTO mytable(id, title) VALUES (@param1, @param1); COMMIT;")
	require.ErrorIs(t, err, ErrInferredMultipleTypes)

	_, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > INVALID_FUNCTION()")
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = engine.InferParameters(context.Background(), nil, "SELECT * FROM mytable WHERE id > CAST(wrong_column_name AS INTEGER)")
	require.ErrorIs(t, err, ErrColumnDoesNotExist)
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
			&Integer{val: 0},
			12,
		},
		{
			"large integer",
			[]byte{0, 0, 0, 8, 0, 0, 0, 0, 127, 255, 255, 255},
			IntegerType,
			&Integer{val: math.MaxInt32},
			12,
		},
		{
			"large integer padded",
			[]byte{0, 0, 0, 8, 0, 0, 0, 0, 127, 255, 255, 255, 1, 1, 1},
			IntegerType,
			&Integer{val: math.MaxInt32},
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
		"e-prefix.M.\x80a",
	))
	require.ErrorIs(t, err, ErrCorruptedData)
	require.Nil(t, encPKVals)

	fullValue := append(
		[]byte("e-prefix.M."),
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
	_, _, err := EncodeValueAsKey(&NullValue{}, IntegerType, 0)
	require.ErrorIs(t, err, ErrInvalidValue)

	_, _, err = EncodeValueAsKey(&Varchar{val: "a"}, VarcharType, MaxKeyLen+1)
	require.ErrorIs(t, err, ErrMaxKeyLengthExceeded)

	_, _, err = EncodeValueAsKey(&Varchar{val: "a"}, "NOTATYPE", MaxKeyLen)
	require.ErrorIs(t, err, ErrInvalidValue)

	t.Run("varchar cases", func(t *testing.T) {
		_, _, err = EncodeValueAsKey(&Bool{val: true}, VarcharType, 10)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, _, err = EncodeValueAsKey(&Varchar{val: "abc"}, VarcharType, 1)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)
	})

	t.Run("integer cases", func(t *testing.T) {
		_, _, err = EncodeValueAsKey(&Bool{val: true}, IntegerType, 8)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, _, err = EncodeValueAsKey(&Integer{val: int64(10)}, IntegerType, 4)
		require.ErrorIs(t, err, ErrCorruptedData)
	})

	t.Run("boolean cases", func(t *testing.T) {
		_, _, err = EncodeValueAsKey(&Varchar{val: "abc"}, BooleanType, 1)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, _, err = EncodeValueAsKey(&Bool{val: true}, BooleanType, 2)
		require.ErrorIs(t, err, ErrCorruptedData)
	})

	t.Run("blob cases", func(t *testing.T) {
		_, _, err = EncodeValueAsKey(&Varchar{val: "abc"}, BLOBType, 3)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, _, err = EncodeValueAsKey(&Blob{val: []byte{1, 2, 3}}, BLOBType, 2)
		require.ErrorIs(t, err, ErrMaxLengthExceeded)
	})

	t.Run("timestamp cases", func(t *testing.T) {
		_, _, err = EncodeValueAsKey(&Bool{val: true}, TimestampType, 8)
		require.ErrorIs(t, err, ErrInvalidValue)

		_, _, err = EncodeValueAsKey(&Integer{val: int64(10)}, TimestampType, 4)
		require.ErrorIs(t, err, ErrCorruptedData)
	})
}

func TestIndexingNullableColumns(t *testing.T) {
	engine := setupCommonTest(t)

	exec := func(t *testing.T, stmt string) *SQLTx {
		ret, _, err := engine.Exec(context.Background(), nil, stmt, nil)
		require.NoError(t, err)
		return ret
	}

	query := func(t *testing.T, stmt string, expectedRows ...*Row) {
		reader, err := engine.Query(context.Background(), nil, stmt, nil)
		require.NoError(t, err)

		for _, expectedRow := range expectedRows {
			row, err := reader.Read(context.Background())
			require.NoError(t, err)

			require.EqualValues(t, expectedRow, row)
		}

		_, err = reader.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = reader.Close()
		require.NoError(t, err)
	}

	colVal := func(t *testing.T, v interface{}, tp SQLValueType) TypedValue {
		switch v := v.(type) {
		case nil:
			return &NullValue{t: tp}
		case int:
			return &Integer{val: int64(v)}
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
		idVal := &Integer{val: id}
		v1Val := colVal(t, v1, IntegerType)
		v2Val := colVal(t, v2, VarcharType)

		return &Row{
			ValuesByPosition: []TypedValue{
				idVal,
				v1Val,
				v2Val,
			},
			ValuesBySelector: map[string]TypedValue{
				EncodeSelector("", "table1", "id"): idVal,
				EncodeSelector("", "table1", "v1"): v1Val,
				EncodeSelector("", "table1", "v2"): v2Val,
			},
		}
	}

	t2Row := func(id int64, v1, v2, v3, v4 interface{}) *Row {
		idVal := &Integer{val: id}
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
				EncodeSelector("", "table2", "id"): idVal,
				EncodeSelector("", "table2", "v1"): v1Val,
				EncodeSelector("", "table2", "v2"): v2Val,
				EncodeSelector("", "table2", "v3"): v3Val,
				EncodeSelector("", "table2", "v4"): v4Val,
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

	t.Run("succeed creating table with two indexes", func(t *testing.T) {

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
		_, _, err := engine.Exec(context.Background(), nil, "INSERT INTO table2(v1, v2, v3, v4) VALUES(1, '2', null, null)", nil)
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
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(title)", nil)
	require.NoError(t, err)

	rowCount := 10
	for i := 0; i < rowCount; i++ {
		_, txs, err := engine.Exec(context.Background(), nil, "INSERT INTO table1(title) VALUES (@title)", map[string]interface{}{"title": fmt.Sprintf("title%d", i)})
		require.NoError(t, err)
		require.Len(t, txs, 1)

		hdr := txs[0].TxHeader()

		t.Run("querying data with future date should not return any row", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT id, title FROM table1 AFTER CAST(@ts AS TIMESTAMP)", map[string]interface{}{"ts": hdr.Ts})
			require.NoError(t, err)

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with a greater tx should not return any row", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT id, title FROM table1 AFTER TX @tx", map[string]interface{}{"tx": hdr.ID})
			require.NoError(t, err)

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data since tx date should return the last row", func(t *testing.T) {
			q := "SELECT id, title FROM table1 SINCE CAST(@ts AS TIMESTAMP) UNTIL now()"

			params, err := engine.InferParameters(context.Background(), nil, q)
			require.NoError(t, err)
			require.NotNil(t, params)
			require.Len(t, params, 1)
			require.Equal(t, AnyType, params["ts"])

			r, err := engine.Query(context.Background(), nil, q, map[string]interface{}{"ts": hdr.Ts})
			require.NoError(t, err)

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i+1), row.ValuesBySelector["(table1.id)"].RawValue())

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with since tx id should return the last row", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT id, title FROM table1 SINCE TX @tx", map[string]interface{}{"tx": hdr.ID})
			require.NoError(t, err)

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i+1), row.ValuesBySelector["(table1.id)"].RawValue())

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("querying data with until current tx ordering desc by name should return always the first row", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT id FROM table1 UNTIL TX @tx ORDER BY title ASC", map[string]interface{}{"tx": hdr.ID})
			require.NoError(t, err)

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(1), row.ValuesBySelector["(table1.id)"].RawValue())

			err = r.Close()
			require.NoError(t, err)
		})

		time.Sleep(1 * time.Second)
	}

	t.Run("querying data with until current time should return all rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 SINCE TX 1 UNTIL now()", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("querying data since an older date should return all rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 SINCE '2021-12-03'", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("querying data since an older date should return all rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 SINCE CAST('2021-12-03' AS TIMESTAMP)", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestHistoricalQueries(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER, title VARCHAR[50], _rev INTEGER, PRIMARY KEY id)", nil)
	require.ErrorIs(t, err, ErrReservedWord)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER, title VARCHAR[50], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(title)", nil)
	require.NoError(t, err)

	rowCount := 10

	for i := 1; i <= rowCount; i++ {
		_, txs, err := engine.Exec(context.Background(), nil, "UPSERT INTO table1(id, title) VALUES (1, @title)", map[string]interface{}{"title": fmt.Sprintf("title%d", i)})
		require.NoError(t, err)
		require.Len(t, txs, 1)
	}

	t.Run("querying historical data should return data from older to newer revisions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT _rev, title FROM (HISTORY OF table1)", nil)
		require.NoError(t, err)

		for i := 1; i <= rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i), row.ValuesByPosition[0].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesByPosition[1].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("querying historical data in desc order should return data from newer to older revisions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT _rev, title FROM (HISTORY OF table1) order by id desc", nil)
		require.NoError(t, err)

		for i := rowCount; i > 0; i-- {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, int64(i), row.ValuesByPosition[0].RawValue())
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesByPosition[1].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestUnionOperator(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1(title)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table2(id INTEGER AUTO_INCREMENT, name VARCHAR[30], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table_unknown UNION SELECT COUNT(*) FROM table1", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) FROM table1 UNION SELECT COUNT(*) FROM table_unknown", nil)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 UNION SELECT id, title FROM table1", nil)
	require.ErrorIs(t, err, ErrColumnMismatchInUnionStmt)

	_, err = engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 UNION SELECT title FROM table1", nil)
	require.ErrorIs(t, err, ErrColumnMismatchInUnionStmt)

	_, err = engine.InferParameters(context.Background(), nil, "SELECT title FROM table1 UNION SELECT name FROM table2")
	require.NoError(t, err)

	_, err = engine.InferParameters(context.Background(), nil, "SELECT title FROM table1 UNION invalid stmt")
	require.ErrorIs(t, err, ErrParsingError)

	rowCount := 10
	for i := 0; i < rowCount; i++ {
		_, _, err := engine.Exec(context.Background(), nil, "INSERT INTO table1(title) VALUES (@title)", map[string]interface{}{"title": fmt.Sprintf("title%d", i)})
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table2(name) VALUES (@name)", map[string]interface{}{"name": fmt.Sprintf("name%d", i)})
		require.NoError(t, err)
	}

	t.Run("default union should filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
			SELECT COUNT(*) as c FROM table1
			UNION
			SELECT COUNT(*) FROM table1
			UNION
			SELECT COUNT(*) c FROM table1 t1
		`, nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union all should not filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT COUNT(*) as c FROM table1 UNION ALL SELECT COUNT(*) FROM table1", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, int64(rowCount), row.ValuesBySelector["(table1.c)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union should filter out duplicated rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT title FROM table1 order by title desc UNION SELECT title FROM table1", nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", rowCount-i-1), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("union with subqueries over different tables", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, "SELECT title FROM table1 UNION SELECT name FROM table2", nil)
		require.NoError(t, err)

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("title%d", i), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		for i := 0; i < rowCount; i++ {
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.NotNil(t, row)
			require.Equal(t, fmt.Sprintf("name%d", i), row.ValuesBySelector["(table1.title)"].RawValue())
		}

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestTemporalQueriesEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id)", nil)
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
			r, err := engine.Query(context.Background(), nil, c.query, c.params)
			require.NoError(t, err)

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, c.err)

			err = r.Close()
			require.NoError(t, err)
		})
	}
}

func TestTemporalQueriesDeletedRows(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1(id INTEGER, title VARCHAR[50], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, tx1, err := engine.Exec(context.Background(), nil,
			"INSERT INTO table1(id, title) VALUES(@id, @title)",
			map[string]interface{}{
				"id":    i,
				"title": fmt.Sprintf("title%d", i),
			},
		)
		require.NoError(t, err)
		require.Len(t, tx1, 1)
	}

	_, tx2, err := engine.Exec(context.Background(), nil, "DELETE FROM table1 WHERE id = 5", nil)
	require.NoError(t, err)
	require.Len(t, tx2, 1)

	// Update value that is topologically before the deleted entry when scanning primary index
	_, _, err = engine.Exec(context.Background(), nil, "UPDATE table1 SET title = 'updated_title2' WHERE id = 2", nil)
	require.NoError(t, err)

	// Update value that is topologically after the deleted entry when scanning primary index
	_, _, err = engine.Exec(context.Background(), nil, "UPDATE table1 SET title = 'updated_title8' WHERE id = 8", nil)
	require.NoError(t, err)

	// Reinsert deleted entry
	_, tx3, err := engine.Exec(context.Background(), nil, "INSERT INTO table1(id, title) VALUES(5, 'title5')", nil)
	require.NoError(t, err)
	require.Len(t, tx3, 1)

	// The sequence of operations is:
	//       Crate table
	//  tx1: INSERT id=0..9
	//  tx2: DELETE id=5    \
	//       UPDATE id=2     >- temporal query over the range
	//       UPDATE id=8    /
	//  tx3: INSERT id=5

	res, err := engine.Query(
		context.Background(), nil,
		"SELECT id FROM table1 SINCE TX @since BEFORE TX @before",
		map[string]interface{}{
			"since":  tx2[0].txHeader.ID,
			"before": tx3[0].txHeader.ID,
		},
	)
	require.NoError(t, err)

	row, err := res.Read(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, row.ValuesByPosition[0].RawValue())

	row, err = res.Read(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 8, row.ValuesByPosition[0].RawValue())

	_, err = res.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)

	err = res.Close()
	require.NoError(t, err)
}

func TestMultiDBCatalogQueries(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	dbs := []string{"db1", "db2"}
	handler := &multidbHandlerMock{
		user: &mockUser{
			username:      "user",
			sqlPrivileges: allPrivileges,
		},
	}

	opts := DefaultOptions().
		WithPrefix(sqlPrefix).
		WithMultiDBHandler(handler)

	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	handler.dbs = dbs
	handler.engine = engine

	t.Run("with a handler, multi database stmts are delegated to the handler", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, `
			BEGIN TRANSACTION;
				CREATE DATABASE db1;
			COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec(context.Background(), nil, `
			BEGIN TRANSACTION;
				CREATE USER user1 WITH PASSWORD 'user1Password!' READ;
			COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec(context.Background(), nil, `
			BEGIN TRANSACTION;
				ALTER USER user1 WITH PASSWORD 'user1Password!' READ;
			COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec(context.Background(), nil, `
			BEGIN TRANSACTION;
				DROP USER user1;
			COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec(context.Background(), nil, `
			BEGIN TRANSACTION;
				GRANT ALL PRIVILEGES ON DATABASE defaultdb TO USER myuser;
			COMMIT;
		`, nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		_, _, err = engine.Exec(context.Background(), nil, "CREATE DATABASE db1", nil)
		require.ErrorIs(t, err, ErrNoSupported)

		_, _, err = engine.Exec(context.Background(), nil, "USE DATABASE db1", nil)
		require.NoError(t, err)

		ntx, ctxs, err := engine.Exec(context.Background(), nil, "USE DATABASE db1; USE DATABASE db2", nil)
		require.NoError(t, err)
		require.Nil(t, ntx)
		require.Len(t, ctxs, 2)
		require.Zero(t, ctxs[0].UpdatedRows())
		require.Zero(t, ctxs[1].UpdatedRows())

		_, _, err = engine.Exec(context.Background(), nil, "BEGIN TRANSACTION; USE DATABASE db1; COMMIT;", nil)
		require.ErrorIs(t, err, ErrNonTransactionalStmt)

		t.Run("unconditional database query", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT * FROM DATABASES() WHERE name LIKE 'db*'", nil)
			require.NoError(t, err)

			for _, db := range dbs {
				row, err := r.Read(context.Background())
				require.NoError(t, err)
				require.NotNil(t, row)
				require.NotNil(t, row)
				require.Equal(t, db, row.ValuesBySelector["(databases.name)"].RawValue())
			}

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("show databases", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SHOW DATABASES", nil)
			require.NoError(t, err)

			for _, db := range dbs {
				row, err := r.Read(context.Background())
				require.NoError(t, err)
				require.NotNil(t, row)
				require.NotNil(t, row)
				require.Equal(t, db, row.ValuesBySelector["(databases.name)"].RawValue())
			}

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})

		t.Run("show users", func(t *testing.T) {
			rows, err := engine.queryAll(context.Background(), nil, "SHOW USERS", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "user", rows[0].ValuesByPosition[0].RawValue())
		})

		t.Run("list users", func(t *testing.T) {
			rows, err := engine.queryAll(context.Background(), nil, "SELECT * FROM USERS()", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "user", rows[0].ValuesByPosition[0].RawValue())
		})

		t.Run("query databases using conditions with table and column aliasing", func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, "SELECT dbs.name as dbname FROM DATABASES() as dbs WHERE name LIKE 'db*'", nil)
			require.NoError(t, err)

			for _, db := range dbs {
				row, err := r.Read(context.Background())
				require.NoError(t, err)
				require.NotNil(t, row)
				require.NotNil(t, row)
				require.Equal(t, db, row.ValuesBySelector["(dbs.dbname)"].RawValue())
			}

			_, err = r.Read(context.Background())
			require.ErrorIs(t, err, ErrNoMoreRows)

			err = r.Close()
			require.NoError(t, err)
		})
	})
}

type mockUser struct {
	username      string
	permission    Permission
	sqlPrivileges []SQLPrivilege
}

func (u *mockUser) Username() string {
	return u.username
}

func (u *mockUser) Permission() Permission {
	return u.permission
}

func (u *mockUser) SQLPrivileges() []SQLPrivilege {
	return u.sqlPrivileges
}

type multidbHandlerMock struct {
	dbs    []string
	user   *mockUser
	engine *Engine
}

func (h *multidbHandlerMock) ListDatabases(ctx context.Context) ([]string, error) {
	return h.dbs, nil
}

func (h *multidbHandlerMock) CreateDatabase(ctx context.Context, db string, ifNotExists bool) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) GrantSQLPrivileges(ctx context.Context, database, username string, privileges []SQLPrivilege) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) RevokeSQLPrivileges(ctx context.Context, database, username string, privileges []SQLPrivilege) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) UseDatabase(ctx context.Context, db string) error {
	return nil
}

func (h *multidbHandlerMock) GetLoggedUser(ctx context.Context) (User, error) {
	if h.user == nil {
		return nil, fmt.Errorf("no logged user")
	}
	return h.user, nil
}

func (h *multidbHandlerMock) ListUsers(ctx context.Context) ([]User, error) {
	return []User{h.user}, nil
}

func (h *multidbHandlerMock) CreateUser(ctx context.Context, username, password string, permission Permission) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) AlterUser(ctx context.Context, username, password string, permission Permission) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) DropUser(ctx context.Context, username string) error {
	return ErrNoSupported
}

func (h *multidbHandlerMock) ExecPreparedStmts(
	ctx context.Context,
	opts *TxOptions,
	stmts []SQLStmt,
	params map[string]interface{},
) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	return h.engine.ExecPreparedStmts(ctx, nil, stmts, params)
}

func TestSingleDBCatalogQueries(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE mytable1(id INTEGER NOT NULL AUTO_INCREMENT, title VARCHAR[256], PRIMARY KEY id);

		CREATE TABLE mytable2(id INTEGER NOT NULL, name VARCHAR[100], active BOOLEAN, PRIMARY KEY id);
	`, nil)
	require.NoError(t, err)

	tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), tx, `
		CREATE INDEX ON mytable1(title);

		CREATE INDEX ON mytable2(name);
		CREATE UNIQUE INDEX ON mytable2(name, active);
	`, nil)
	require.NoError(t, err)

	defer tx.Cancel()

	t.Run("querying tables without any condition should return all tables", func(t *testing.T) {
		r, err := engine.Query(context.Background(), tx, "SELECT * FROM TABLES()", nil)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(tables.name)"].RawValue())

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(tables.name)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("querying tables with name equality comparison should return only one table", func(t *testing.T) {
		r, err := engine.Query(context.Background(), tx, "SELECT * FROM TABLES() WHERE name = 'mytable2'", nil)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(tables.name)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("querying tables with name equality comparison using parameters should return only one table", func(t *testing.T) {
		params := make(map[string]interface{})
		params["name"] = "mytable2"

		r, err := engine.Query(context.Background(), tx, "SELECT * FROM TABLES() WHERE name = @name", params)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(tables.name)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("show tables", func(t *testing.T) {
		r, err := engine.Query(context.Background(), tx, "SHOW TABLES", nil)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(tables.name)"].RawValue())

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(tables.name)"].RawValue())

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional index query should return all the indexes of mytable1", func(t *testing.T) {
		params := map[string]interface{}{
			"tableName": "mytable1",
		}
		r, err := engine.Query(context.Background(), tx, "SELECT * FROM INDEXES(@tableName)", params)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(indexes.table)"].RawValue())
		require.Equal(t, "mytable1(id)", row.ValuesBySelector["(indexes.name)"].RawValue())
		require.True(t, row.ValuesBySelector["(indexes.unique)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(indexes.primary)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(indexes.table)"].RawValue())
		require.Equal(t, "mytable1(title)", row.ValuesBySelector["(indexes.name)"].RawValue())
		require.False(t, row.ValuesBySelector["(indexes.unique)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(indexes.primary)"].RawValue().(bool))

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional index query should return all the indexes of mytable2", func(t *testing.T) {
		r, err := engine.Query(context.Background(), tx, "SELECT * FROM INDEXES('mytable2')", nil)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(indexes.table)"].RawValue())
		require.Equal(t, "mytable2(id)", row.ValuesBySelector["(indexes.name)"].RawValue())
		require.True(t, row.ValuesBySelector["(indexes.unique)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(indexes.primary)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(indexes.table)"].RawValue())
		require.Equal(t, "mytable2(name)", row.ValuesBySelector["(indexes.name)"].RawValue())
		require.False(t, row.ValuesBySelector["(indexes.unique)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(indexes.primary)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(indexes.table)"].RawValue())
		require.Equal(t, "mytable2(name,active)", row.ValuesBySelector["(indexes.name)"].RawValue())
		require.True(t, row.ValuesBySelector["(indexes.unique)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(indexes.primary)"].RawValue().(bool))

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional column query should return all the columns of mytable1", func(t *testing.T) {
		params := map[string]interface{}{
			"tableName": "mytable1",
		}

		r, err := engine.Query(context.Background(), tx, "SELECT * FROM COLUMNS(@tableName)", params)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(columns.table)"].RawValue())
		require.Equal(t, "id", row.ValuesBySelector["(columns.name)"].RawValue())
		require.Equal(t, IntegerType, row.ValuesBySelector["(columns.type)"].RawValue())
		require.Equal(t, int64(8), row.ValuesBySelector["(columns.max_length)"].RawValue())
		require.False(t, row.ValuesBySelector["(columns.nullable)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.auto_increment)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.indexed)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.primary)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.unique)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable1", row.ValuesBySelector["(columns.table)"].RawValue())
		require.Equal(t, "title", row.ValuesBySelector["(columns.name)"].RawValue())
		require.Equal(t, VarcharType, row.ValuesBySelector["(columns.type)"].RawValue())
		require.Equal(t, int64(256), row.ValuesBySelector["(columns.max_length)"].RawValue())
		require.True(t, row.ValuesBySelector["(columns.nullable)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.auto_increment)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.indexed)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.primary)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.unique)"].RawValue().(bool))

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})

	t.Run("unconditional column query should return all the columns of mytable2", func(t *testing.T) {
		r, err := engine.Query(context.Background(), tx, "SELECT * FROM COLUMNS('mytable2')", nil)
		require.NoError(t, err)

		defer r.Close()

		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(columns.table)"].RawValue())
		require.Equal(t, "id", row.ValuesBySelector["(columns.name)"].RawValue())
		require.Equal(t, IntegerType, row.ValuesBySelector["(columns.type)"].RawValue())
		require.Equal(t, int64(8), row.ValuesBySelector["(columns.max_length)"].RawValue())
		require.False(t, row.ValuesBySelector["(columns.nullable)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.auto_increment)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.indexed)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.primary)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.unique)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(columns.table)"].RawValue())
		require.Equal(t, "name", row.ValuesBySelector["(columns.name)"].RawValue())
		require.Equal(t, VarcharType, row.ValuesBySelector["(columns.type)"].RawValue())
		require.Equal(t, int64(100), row.ValuesBySelector["(columns.max_length)"].RawValue())
		require.True(t, row.ValuesBySelector["(columns.nullable)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.auto_increment)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.indexed)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.primary)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.unique)"].RawValue().(bool))

		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "mytable2", row.ValuesBySelector["(columns.table)"].RawValue())
		require.Equal(t, "active", row.ValuesBySelector["(columns.name)"].RawValue())
		require.Equal(t, BooleanType, row.ValuesBySelector["(columns.type)"].RawValue())
		require.Equal(t, int64(1), row.ValuesBySelector["(columns.max_length)"].RawValue())
		require.True(t, row.ValuesBySelector["(columns.nullable)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.auto_increment)"].RawValue().(bool))
		require.True(t, row.ValuesBySelector["(columns.indexed)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.primary)"].RawValue().(bool))
		require.False(t, row.ValuesBySelector["(columns.unique)"].RawValue().(bool))

		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
	})
}

func TestMVCC(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, payload BLOB[2], PRIMARY KEY id);", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (title);", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (active);", nil)
	require.NoError(t, err)

	t.Run("read conflict should be detected when a new index was created by another transaction", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "CREATE INDEX ON table1 (payload);", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when processing transactions without overlapping rows", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "INSERT INTO table1 (id, title, active, payload) VALUES (2, 'title2', false, x'00A2');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when processing transactions with overlapping rows", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when processing transactions with invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 USE INDEX ON id WHERE id > 0", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		for {
			_, err = rowReader.Read(context.Background())
			if err != nil {
				require.ErrorIs(t, err, ErrNoMoreRows)
				break
			}
		}

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (2, 'title2', false, x'00A2');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when processing transactions with non-invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 USE INDEX ON id WHERE id > 10", nil)
		require.NoError(t, err)

		_, err = rowReader.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (2, 'title2', false, x'00A2');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when processing transactions with invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "DELETE FROM table1 WHERE id > 0", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (2, 'title2', false, x'00A2');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when processing transactions with non-invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "DELETE FROM table1 WHERE id > 2", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (2, 'title2', false, x'00A2');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when processing transactions with invalidated queries in desc order", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (10, 'title10', true, x'0A10');", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 USE INDEX ON id WHERE id < 10 ORDER BY id DESC", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		for {
			_, err = rowReader.Read(context.Background())
			if err != nil {
				require.ErrorIs(t, err, ErrNoMoreRows)
				break
			}
		}

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (10, 'title10', false, x'0A10');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when processing transactions with non invalidated queries in desc order", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (11, 'title11', true, x'0A11');", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 USE INDEX ON id WHERE id < 10 ORDER BY id DESC", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		for {
			_, err = rowReader.Read(context.Background())
			if err != nil {
				require.ErrorIs(t, err, ErrNoMoreRows)
				break
			}
		}

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (10, 'title10', false, x'0A10');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.NoError(t, err)
	})

	t.Run("no read conflict should be detected when processing transactions with non invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (11, 'title11', true, x'0A11');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (12, 'title12', true, x'0A12');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 LIMIT 2", nil)
		require.NoError(t, err)

		for {
			_, err = rowReader.Read(context.Background())
			if err != nil {
				require.ErrorIs(t, err, ErrNoMoreRows)
				break
			}
		}

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (10, 'title10', false, x'0A10');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when processing transactions with invalidated queries", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (11, 'title11', true, x'0A11');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "UPSERT INTO table1 (id, title, active, payload) VALUES (12, 'title12', true, x'0A12');", nil)
		require.NoError(t, err)

		rowReader, err := engine.Query(context.Background(), tx2, "SELECT * FROM table1 ORDER BY id DESC LIMIT 1 OFFSET 1", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		for {
			_, err = rowReader.Read(context.Background())
			if err != nil {
				require.ErrorIs(t, err, ErrNoMoreRows)
				break
			}
		}

		err = rowReader.Close()
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "UPSERT INTO table1 (id, title, active, payload) VALUES (10, 'title10', false, x'0A10');", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when processing transactions with invalidated catalog changes", func(t *testing.T) {
		tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "CREATE TABLE mytable1 (id INTEGER, PRIMARY KEY id);", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "CREATE TABLE mytable1 (id INTEGER, PRIMARY KEY id);", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
		require.ErrorIs(t, err, store.ErrTxReadConflict)
	})
}

func TestMVCCWithExternalCommitAllowance(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true).WithExternalCommitAllowance(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	go func() {
		time.Sleep(1 * time.Second)
		st.AllowCommitUpto(1)
	}()

	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, PRIMARY KEY id);", nil)
	require.NoError(t, err)

	tx1, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
	require.NoError(t, err)

	tx2, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), tx2, "INSERT INTO table1 (id, title, active) VALUES (1, 'title1', true);", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), tx2, "INSERT INTO table1 (id, title, active) VALUES (2, 'title2', false);", nil)
	require.NoError(t, err)

	go func() {
		time.Sleep(1 * time.Second)
		st.AllowCommitUpto(2)
	}()

	_, _, err = engine.Exec(context.Background(), tx1, "COMMIT;", nil)
	require.NoError(t, err)

	go func() {
		time.Sleep(1 * time.Second)
		st.AllowCommitUpto(3)
	}()

	_, _, err = engine.Exec(context.Background(), tx2, "COMMIT;", nil)
	require.NoError(t, err)
}

func TestConcurrentInsertions(t *testing.T) {
	workers := 10

	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true).WithMaxConcurrency(workers))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, `
		CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, payload BLOB[2], PRIMARY KEY id);
		CREATE INDEX ON table1 (title);
	`, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(i int) {
			tx, _, err := engine.Exec(context.Background(), nil, "BEGIN TRANSACTION;", nil)
			if err != nil {
				panic(err)
			}

			_, _, err = engine.Exec(context.Background(), tx,
				"UPSERT INTO table1 (id, title, active, payload) VALUES (@id, 'title', true, x'00A1');",
				map[string]interface{}{
					"id": i,
				},
			)
			if err != nil {
				panic(err)
			}

			_, _, err = engine.Exec(context.Background(), tx, "COMMIT;", nil)
			if err != nil {
				panic(err)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestSQLTxWithClosedContext(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE table1 (id INTEGER, title VARCHAR[10], active BOOLEAN, payload BLOB[2], PRIMARY KEY id);", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (title);", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "CREATE INDEX ON table1 (active);", nil)
	require.NoError(t, err)

	t.Run("transaction creation should fail with a cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := engine.Exec(ctx, nil, "BEGIN TRANSACTION;", nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("transaction commit should fail with a cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		tx, _, err := engine.Exec(ctx, nil, "BEGIN TRANSACTION;", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(ctx, tx, "INSERT INTO table1 (id, title, active, payload) VALUES (1, 'title1', true, x'00A1');", nil)
		require.NoError(t, err)

		cancel()

		_, _, err = engine.Exec(ctx, tx, "COMMIT;", nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func setupCommonTestWithOptions(t *testing.T, sopts *store.Options) (*Engine, *store.ImmuStore) {
	st, err := store.Open(t.TempDir(), sopts.WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { closeStore(t, st) })

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	return engine, st
}

func TestCopyCatalogToTx(t *testing.T) {
	fileSize := 1024

	opts := store.DefaultOptions()
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(10)).WithFileSize(fileSize)

	engine, st := setupCommonTestWithOptions(t, opts)

	exec := func(t *testing.T, stmt string) *SQLTx {
		ret, _, err := engine.Exec(context.Background(), nil, stmt, nil)
		require.NoError(t, err)
		return ret
	}

	query := func(t *testing.T, stmt string, expectedRows ...*Row) {
		reader, err := engine.Query(context.Background(), nil, stmt, nil)
		require.NoError(t, err)

		for _, expectedRow := range expectedRows {
			row, err := reader.Read(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, expectedRow, row)
		}

		_, err = reader.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)

		err = reader.Close()
		require.NoError(t, err)
	}

	colVal := func(t *testing.T, v interface{}, tp SQLValueType) TypedValue {
		switch v := v.(type) {
		case nil:
			return &NullValue{t: tp}
		case int:
			return &Integer{val: int64(v)}
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

	tRow := func(
		table string,
		id int64,
		v1, v2, v3 interface{},
	) *Row {
		idVal := &Integer{val: id}
		v1Val := colVal(t, v1, IntegerType)
		v2Val := colVal(t, v2, VarcharType)
		v3Val := colVal(t, v3, AnyType)

		return &Row{
			ValuesByPosition: []TypedValue{
				idVal,
				v1Val,
				v3Val,
				v2Val,
			},
			ValuesBySelector: map[string]TypedValue{
				EncodeSelector("", table, "id"):      idVal,
				EncodeSelector("", table, "name"):    v1Val,
				EncodeSelector("", table, "amount"):  v3Val,
				EncodeSelector("", table, "surname"): v2Val,
			},
		}
	}

	// create two tables
	exec(t, "CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, name VARCHAR[50], amount INTEGER, PRIMARY KEY id)")
	exec(t, "CREATE UNIQUE INDEX ON table1 (name)")
	exec(t, "CREATE UNIQUE INDEX ON table1 (name, amount)")
	query(t, "SELECT * FROM table1")

	exec(t, "CREATE TABLE table2 (id INTEGER AUTO_INCREMENT, name VARCHAR[50], amount INTEGER, PRIMARY KEY id)")
	exec(t, "CREATE UNIQUE INDEX ON table2 (name)")
	exec(t, "CREATE UNIQUE INDEX ON table2 (name, amount)")
	query(t, "SELECT * FROM table2")

	t.Run("should fail due to unique index", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil, "INSERT INTO table1 (name, amount) VALUES ('name1', 10), ('name1', 10)", nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)
	})

	// insert some data
	var deleteUptoTx *store.TxHeader

	t.Run("insert few transactions", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			tx, err := st.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			key := []byte(fmt.Sprintf("key_%d", i))
			value := make([]byte, fileSize)

			err = tx.Set(key, nil, value)
			require.NoError(t, err)

			deleteUptoTx, err = tx.Commit(context.Background())
			require.NoError(t, err)
		}
	})

	// alter table to add a new column to both tables
	t.Run("alter table and add data", func(t *testing.T) {
		exec(t, "ALTER TABLE table1 ADD COLUMN surname VARCHAR")
		exec(t, "INSERT INTO table1(name, surname, amount) VALUES('Foo', 'Bar', 0)")
		exec(t, "INSERT INTO table1(name, surname, amount) VALUES('Fin', 'Baz', 0)")

		exec(t, "ALTER TABLE table2 ADD COLUMN surname VARCHAR")
		exec(t, "INSERT INTO table2(name, surname, amount) VALUES('Foo', 'Bar', 0)")
		exec(t, "INSERT INTO table2(name, surname, amount) VALUES('Fin', 'Baz', 0)")
	})

	// copy current catalog for recreating the catalog for database/table
	t.Run("succeed copying catalog for db", func(t *testing.T) {
		tx, err := engine.store.NewTx(context.Background(), store.DefaultTxOptions())
		require.NoError(t, err)

		err = engine.CopyCatalogToTx(context.Background(), tx)
		require.NoError(t, err)

		hdr, err := tx.Commit(context.Background())
		require.NoError(t, err)
		// ensure that the last committed txn is the one we just committed
		require.Equal(t, hdr.ID, st.LastCommittedTxID())
	})

	// delete txns in the store upto a certain txn
	t.Run("succeed truncating sql catalog", func(t *testing.T) {
		hdr, err := st.ReadTxHeader(deleteUptoTx.ID, false, false)
		require.NoError(t, err)
		require.NoError(t, st.TruncateUptoTx(hdr.ID))
	})

	// add more data in table post truncation
	t.Run("add data post truncation", func(t *testing.T) {
		exec(t, "INSERT INTO table1(name, surname, amount) VALUES('John', 'Doe', 0)")
		exec(t, "INSERT INTO table1(name, surname, amount) VALUES('Smith', 'John', 0)")

		exec(t, "INSERT INTO table2(name, surname, amount) VALUES('John', 'Doe', 0)")
		exec(t, "INSERT INTO table2(name, surname, amount) VALUES('Smith', 'John', 0)")

	})

	// check if can query the table with new catalogue
	t.Run("succeed loading catalog from latest schema", func(t *testing.T) {
		query(t,
			"SELECT * FROM table1",
			tRow("table1", 1, "Foo", "Bar", 0),
			tRow("table1", 2, "Fin", "Baz", 0),
			tRow("table1", 3, "John", "Doe", 0),
			tRow("table1", 4, "Smith", "John", 0),
		)

		query(t,
			"SELECT * FROM table2",
			tRow("table2", 1, "Foo", "Bar", 0),
			tRow("table2", 2, "Fin", "Baz", 0),
			tRow("table2", 3, "John", "Doe", 0),
			tRow("table2", 4, "Smith", "John", 0),
		)

	})

	t.Run("indexing should work with new catalogue", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil, "INSERT INTO table1 (name, amount) VALUES ('name1', 10), ('name1', 10)", nil)
		require.ErrorIs(t, err, store.ErrKeyAlreadyExists)

		// should use primary index by default
		r, err := engine.Query(context.Background(), nil, "SELECT * FROM table1", nil)
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
}

func BenchmarkInsertInto(b *testing.B) {
	workerCount := 100
	txCount := 10
	eCount := 100

	opts := store.DefaultOptions().
		WithMultiIndexing(true).
		WithSynced(true).
		WithMaxActiveTransactions(100).
		WithMaxConcurrency(workerCount)

	st, err := store.Open(b.TempDir(), opts)
	if err != nil {
		b.Fail()
	}

	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	if err != nil {
		b.Fail()
	}

	_, ctxs, err := engine.Exec(context.Background(), nil, `
			CREATE TABLE mytable1(id VARCHAR[30], title VARCHAR[50], PRIMARY KEY id);
			CREATE INDEX ON mytable1(title);
	`, nil)
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(workerCount)

		for w := 0; w < workerCount; w++ {
			go func(r, w int) {
				for i := 0; i < txCount; i++ {
					txOpts := DefaultTxOptions().
						WithExplicitClose(true).
						WithUnsafeMVCC(true).
						WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return ctxs[0].txHeader.ID })

					tx, err := engine.NewTx(context.Background(), txOpts)
					if err != nil {
						b.Fail()
					}

					for j := 0; j < eCount; j++ {
						params := map[string]interface{}{
							"id":    fmt.Sprintf("id_%d_%d_%d_%d", r, w, i, j),
							"title": fmt.Sprintf("title_%d_%d_%d_%d", r, w, i, j),
						}

						_, _, err = engine.Exec(context.Background(), tx, "INSERT INTO mytable1(id, title) VALUES (@id, @title);", params)
						if err != nil {
							b.Fail()
						}

					}

					err = tx.Commit(context.Background())
					if err != nil {
						b.Fail()
					}
				}

				wg.Done()
			}(i, w)
		}

		wg.Wait()
	}
}

func BenchmarkNotIndexedOrderBy(b *testing.B) {
	st, err := store.Open(b.TempDir(), store.DefaultOptions().WithMultiIndexing(true).WithLogger(logger.NewMemoryLoggerWithLevel(logger.LogError)))
	if err != nil {
		b.Fail()
	}

	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix).WithSortBufferSize(1024))
	if err != nil {
		b.Fail()
	}

	_, _, err = engine.Exec(context.Background(), nil, `CREATE TABLE mytable(id INTEGER AUTO_INCREMENT, title VARCHAR[50], PRIMARY KEY id);`, nil)
	if err != nil {
		b.Fail()
	}

	for nBatch := 0; nBatch < 100; nBatch++ {
		tx, err := engine.NewTx(context.Background(), DefaultTxOptions().WithExplicitClose(true))
		if err != nil {
			b.Fail()
		}

		nRows := 1000
		for i := 0; i < nRows; i++ {
			_, _, err := engine.Exec(context.Background(), tx, "INSERT INTO mytable(title) VALUES (@title)", map[string]interface{}{
				"title": fmt.Sprintf("title%d", rand.Int()),
			})
			if err != nil {
				b.Fail()
			}
		}

		err = tx.Commit(context.Background())
		if err != nil {
			b.Fail()
		}
	}

	b.ResetTimer()

	start := time.Now()
	reader, err := engine.Query(context.Background(), nil, "SELECT * FROM mytable ORDER BY title ASC LIMIT 1", nil)
	if err != nil {
		b.Fail()
	}
	defer reader.Close()

	_, err = reader.Read(context.Background())
	if err != nil {
		b.Fail()
	}
	fmt.Println("Elapsed:", time.Since(start))
}

func TestLikeWithNullableColumns(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, "CREATE TABLE mytable (id INTEGER AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO mytable(title) VALUES (NULL), ('title1')", nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil, "SELECT id, title FROM mytable WHERE title LIKE '.*'", nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)

	require.Len(t, row.ValuesByPosition, 2)
	require.EqualValues(t, 2, row.ValuesByPosition[0].RawValue())
	require.EqualValues(t, "title1", row.ValuesByPosition[1].RawValue())

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

type BrokenCatalogTestSuite struct {
	suite.Suite

	path   string
	st     *store.ImmuStore
	engine *Engine
}

func TestBrokenCatalogTestSuite(t *testing.T) {
	suite.Run(t, new(BrokenCatalogTestSuite))
}

func (t *BrokenCatalogTestSuite) SetupTest() {
	t.path = t.T().TempDir()

	st, err := store.Open(t.path, store.DefaultOptions().WithMultiIndexing(true))
	t.Require().NoError(err)

	t.st = st

	t.engine, err = NewEngine(t.st, DefaultOptions().WithPrefix(sqlPrefix))
	t.Require().NoError(err)

	_, _, err = t.engine.Exec(
		context.Background(),
		nil,
		`
		CREATE TABLE test(
			id INTEGER AUTO_INCREMENT,
			var VARCHAR,
			b BOOLEAN,
			PRIMARY KEY(id)
		)
		`, nil)
	t.Require().NoError(err)

	// Tests in teh suite require specific IDs to be assigned
	// we check below if those are as expected
	tx, err := t.engine.NewTx(context.Background(), DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	tab, err := tx.catalog.GetTableByName("test")
	t.Require().NoError(err)
	t.Require().EqualValues(1, tab.id)

	for id, name := range map[uint32]string{
		1: "id",
		2: "var",
		3: "b",
	} {
		col, err := tab.GetColumnByName(name)
		t.Require().NoError(err)
		t.Require().EqualValues(id, col.id)
	}
}

func (t *BrokenCatalogTestSuite) TearDownTest() {
	defer os.RemoveAll(t.path)

	if t.st != nil {
		err := t.st.Close()
		t.Require().NoError(err)
	}
}

func (t *BrokenCatalogTestSuite) getColEntry(colID uint32) (k, v []byte, vref store.ValueRef) {
	tx, err := t.st.NewTx(context.Background(), store.DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	reader, err := tx.NewKeyReader(store.KeyReaderSpec{
		Prefix: MapKey(sqlPrefix, catalogColumnPrefix, EncodeID(1), EncodeID(1), EncodeID(colID)),
	})
	t.Require().NoError(err)
	defer reader.Close()

	k, vref, err = reader.Read(context.Background())
	t.Require().NoError(err)

	v, err = vref.Resolve()
	t.Require().NoError(err)

	return k, v, vref
}

func (t *BrokenCatalogTestSuite) TestCanNotSetExpiredEntryInCatalog() {
	k, v, _ := t.getColEntry(2)

	md := store.NewKVMetadata()
	err := md.ExpiresAt(time.Now().Add(time.Hour))
	t.Require().NoError(err)

	tx, err := t.st.NewTx(context.Background(), store.DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	tx.Set(k, md, v)

	c := newCatalog(sqlPrefix)
	err = c.load(context.Background(), tx)

	t.Require().ErrorIs(err, ErrBrokenCatalogColSpecExpirable)
}

func (t *BrokenCatalogTestSuite) TestErrorWhenColSpecIsToShort() {
	k, v, vref := t.getColEntry(2)

	tx, err := t.st.NewTx(context.Background(), store.DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	err = tx.Delete(context.Background(), k)
	t.Require().NoError(err)

	err = tx.Set(k[:len(k)-1], vref.KVMetadata(), v)
	t.Require().NoError(err)

	c := newCatalog(sqlPrefix)
	err = c.load(context.Background(), tx)

	t.Require().ErrorIs(err, ErrCorruptedData)
}

func (t *BrokenCatalogTestSuite) TestErrorColSpecNotSequential() {
	tx, err := t.engine.NewTx(context.Background(), DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	err = persistColumn(tx, &Column{
		id:    100,
		table: &Table{id: 1},
	})
	t.Require().NoError(err)

	c := newCatalog(sqlPrefix)
	err = c.load(context.Background(), tx.tx)

	t.Require().ErrorIs(err, ErrCorruptedData)
}

func (t *BrokenCatalogTestSuite) TestErrorColSpecDuplicate() {
	tx, err := t.engine.NewTx(context.Background(), DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	// the type is part of the key, write another column with same id as the primary key
	err = persistColumn(tx, &Column{
		id:      1,
		colType: BLOBType,
		table:   &Table{id: 1},
	})
	t.Require().NoError(err)

	c := newCatalog(sqlPrefix)
	err = c.load(context.Background(), tx.tx)

	t.Require().ErrorIs(err, ErrCorruptedData)
}

func (t *BrokenCatalogTestSuite) TestErrorDroppedPrimaryIndexColumn() {
	tx, err := t.engine.NewTx(context.Background(), DefaultTxOptions())
	t.Require().NoError(err)
	defer tx.Cancel()

	// the type is part of the key, write another column with same id as the primary key
	err = persistColumnDeletion(context.Background(), tx, &Column{
		id:      1,
		colType: IntegerType,
		table:   &Table{id: 1},
	})
	t.Require().NoError(err)

	c := newCatalog(sqlPrefix)

	err = c.load(context.Background(), tx.tx)
	t.Require().ErrorIs(err, ErrColumnDoesNotExist)
}

func TestCheckConstraints(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions())
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		`CREATE TABLE table_with_checks (
			id INTEGER AUTO_INCREMENT,
			account VARCHAR,
			in_balance FLOAT,
			out_balance FLOAT,
			balance FLOAT,
			metadata JSON,

			CONSTRAINT metadata_check CHECK metadata->'usr' IS NOT NULL,
			CHECK (account IS NULL) OR (account LIKE '^account_.*'),
			CONSTRAINT in_out_balance_sum CHECK (in_balance + out_balance = balance),
			CHECK (in_balance >= 0),
			CHECK (out_balance <= 0),
			CHECK (balance >= 0),

			PRIMARY KEY id
		)`, nil,
	)
	require.NoError(t, err)

	t.Run("check constraint violation", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table_with_checks(account, in_balance, out_balance, balance, metadata) VALUES ('account_one', 10, -1.5, 8.5, '{"usr": "user"}')`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table_with_checks(account, in_balance, out_balance, balance, metadata) VALUES ('account', 20, -1.0, 19.0, '{"usr": "user"}')`, nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table_with_checks(account, in_balance, out_balance, balance, metadata) VALUES ('account_two', 10, 1.5, 11.5, '{"usr": "user"}')`, nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table_with_checks(account, in_balance, out_balance, balance, metadata) VALUES ('account_two', -1, 2.5, 1.5, '{"usr": "user"}')`, nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil, `INSERT INTO table_with_checks(account, in_balance, out_balance, balance, metadata) VALUES ('account_two', 10, -1.5, 9.0, '{"usr": "user"}')`, nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil,
			`UPDATE table_with_checks
		SET
			in_balance = in_balance - 1,
			out_balance = out_balance + 1
		WHERE id = 1`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil,
			`UPDATE table_with_checks
		SET
			out_balance = out_balance - 1,
			balance = balance - 1
		WHERE id = 1`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "UPDATE table_with_checks SET in_balance = in_balance + 1 WHERE id = 1", nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil, "UPDATE table_with_checks SET in_balance = NULL", nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)
	})

	t.Run("drop constraint", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP CONSTRAINT metadata_check", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP CONSTRAINT in_out_balance_sum", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP CONSTRAINT in_out_balance_sum", nil)
		require.ErrorIs(t, err, ErrConstraintNotFound)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table_with_checks(account, in_balance, out_balance, balance) VALUES (NULL, 10, -1.5, 9.0)", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table_with_checks(account, in_balance, out_balance, balance) VALUES ('account_three', -1, -1.5, 9.0)", nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)

		_, _, err = engine.Exec(context.Background(), nil, "INSERT INTO table_with_checks(account, in_balance, out_balance, balance) VALUES ('account_three', 10, 1.5, 9.0)", nil)
		require.ErrorIs(t, err, ErrCheckConstraintViolation)
	})

	t.Run("drop column with constraint", func(t *testing.T) {
		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP COLUMN account", nil)
		require.ErrorIs(t, err, ErrCannotDropColumn)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP CONSTRAINT table_with_checks_check1", nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil, "ALTER TABLE table_with_checks DROP COLUMN account", nil)
		require.NoError(t, err)
	})

	t.Run("unsupported check expressions", func(t *testing.T) {
		_, _, err := engine.Exec(
			context.Background(),
			nil,
			`CREATE TABLE table_with_invalid_checks (
				id INTEGER AUTO_INCREMENT,

				CHECK EXISTS (SELECT * FROM mytable),

				PRIMARY KEY id
			)`, nil,
		)
		require.ErrorIs(t, err, ErrNoSupported)

		_, _, err = engine.Exec(
			context.Background(),
			nil,
			`CREATE TABLE table_with_invalid_checks (
				id INTEGER AUTO_INCREMENT,

				CHECK id IN (SELECT * FROM mytable),

				PRIMARY KEY id
			)`, nil,
		)
		require.ErrorIs(t, err, ErrNoSupported)
	})
}

func TestQueryTxMetadata(t *testing.T) {
	opts := store.DefaultOptions().WithMultiIndexing(true)
	opts.WithIndexOptions(opts.IndexOpts.WithMaxActiveSnapshots(1))

	st, err := store.Open(t.TempDir(), opts)
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st,
		DefaultOptions().WithPrefix(sqlPrefix).WithParseTxMetadataFunc(func(b []byte) (map[string]interface{}, error) {
			var md map[string]interface{}
			err := json.Unmarshal(b, &md)
			return md, err
		}),
	)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(), nil,
		`
		CREATE TABLE mytbl (
			id INTEGER AUTO_INCREMENT,

			PRIMARY KEY(id)
		)`, nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		extra, err := json.Marshal(map[string]interface{}{
			"n": i + 1,
		})
		require.NoError(t, err)

		txOpts := DefaultTxOptions().WithExtra(extra)
		tx, err := engine.NewTx(context.Background(), txOpts)
		require.NoError(t, err)

		_, _, err = engine.Exec(
			context.Background(),
			tx,
			fmt.Sprintf("INSERT INTO mytbl(id) VALUES (%d)", i+1),
			nil,
		)
		require.NoError(t, err)
	}

	rows, err := engine.queryAll(
		context.Background(),
		nil,
		"SELECT _tx_metadata->'n' FROM mytbl",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 10)

	for i, row := range rows {
		n := row.ValuesBySelector[EncodeSelector("", "mytbl", "_tx_metadata->'n'")].RawValue()
		require.Equal(t, float64(i+1), n)
	}

	engine.parseTxMetadata = nil

	_, err = engine.queryAll(
		context.Background(),
		nil,
		"SELECT _tx_metadata->'n' FROM mytbl",
		nil,
	)
	require.ErrorContains(t, err, "unable to parse tx metadata")

	engine.parseTxMetadata = func(b []byte) (map[string]interface{}, error) {
		return nil, fmt.Errorf("parse error")
	}

	_, err = engine.queryAll(
		context.Background(),
		nil,
		"SELECT _tx_metadata->'n' FROM mytbl",
		nil,
	)
	require.ErrorIs(t, err, ErrInvalidTxMetadata)
}

func TestGrantSQLPrivileges(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	dbs := []string{"db1", "db2"}
	handler := &multidbHandlerMock{
		dbs: dbs,
		user: &mockUser{
			username:      "myuser",
			permission:    PermissionReadOnly,
			sqlPrivileges: []SQLPrivilege{SQLPrivilegeSelect},
		},
	}

	opts := DefaultOptions().
		WithPrefix(sqlPrefix).
		WithMultiDBHandler(handler)

	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	handler.dbs = dbs
	handler.engine = engine

	tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		tx,
		"CREATE TABLE mytable(id INTEGER, PRIMARY KEY id);",
		nil,
	)
	require.ErrorIs(t, err, ErrAccessDenied)

	handler.user.sqlPrivileges =
		append(handler.user.sqlPrivileges, SQLPrivilegeCreate)

	_, _, err = engine.Exec(
		context.Background(),
		tx,
		"CREATE TABLE mytable(id INTEGER, PRIMARY KEY id);",
		nil,
	)
	require.ErrorIs(t, err, ErrAccessDenied)

	handler.user.permission = PermissionReadWrite

	_, _, err = engine.Exec(
		context.Background(),
		tx,
		"CREATE TABLE mytable(id INTEGER, PRIMARY KEY id);",
		nil,
	)
	require.NoError(t, err)

	checkGrants := func(sql string) {
		rows, err := engine.queryAll(context.Background(), nil, sql, nil)
		require.NoError(t, err)
		require.Len(t, rows, 2)

		usr := rows[0].ValuesByPosition[0].RawValue().(string)
		privilege := rows[0].ValuesByPosition[1].RawValue().(string)

		require.Equal(t, usr, "myuser")
		require.Equal(t, privilege, string(SQLPrivilegeSelect))

		usr = rows[1].ValuesByPosition[0].RawValue().(string)
		privilege = rows[1].ValuesByPosition[1].RawValue().(string)
		require.Equal(t, usr, "myuser")
		require.Equal(t, privilege, string(SQLPrivilegeCreate))
	}

	checkGrants("SHOW GRANTS")
	checkGrants("SHOW GRANTS FOR myuser")
}

func TestFunctions(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		"CREATE TABLE mytable(id INTEGER, PRIMARY KEY id)",
		nil,
	)
	require.NoError(t, err)

	_, _, err = engine.Exec(
		context.Background(),
		nil,
		"INSERT INTO mytable(id) VALUES (1)",
		nil,
	)
	require.NoError(t, err)

	t.Run("timestamp functions", func(t *testing.T) {
		_, err := engine.queryAll(context.Background(), nil, "SELECT NOW(1) FROM mytable", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		rows, err := engine.queryAll(context.Background(), nil, "SELECT NOW() FROM mytable", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		require.IsType(t, time.Time{}, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("uuid functions", func(t *testing.T) {
		_, err := engine.queryAll(context.Background(), nil, "SELECT RANDOM_UUID(1) FROM mytable", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		rows, err := engine.queryAll(context.Background(), nil, "SELECT RANDOM_UUID() FROM mytable", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		require.IsType(t, uuid.UUID{}, rows[0].ValuesByPosition[0].RawValue())
	})

	t.Run("string functions", func(t *testing.T) {
		t.Run("length", func(t *testing.T) {
			_, err := engine.queryAll(context.Background(), nil, "SELECT LENGTH(NULL, 1) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			_, err = engine.queryAll(context.Background(), nil, "SELECT LENGTH(10) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			rows, err := engine.queryAll(context.Background(), nil, "SELECT LENGTH(NULL) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.True(t, rows[0].ValuesByPosition[0].IsNull())
			require.Equal(t, IntegerType, rows[0].ValuesByPosition[0].Type())

			rows, err = engine.queryAll(context.Background(), nil, "SELECT LENGTH('immudb'), LENGTH('') FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, int64(6), rows[0].ValuesByPosition[0].RawValue().(int64))
			require.Equal(t, int64(0), rows[0].ValuesByPosition[1].RawValue().(int64))
		})

		t.Run("substring", func(t *testing.T) {
			_, err := engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 0, 6, true) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			_, err = engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 0, 6) FROM mytable", nil)
			require.ErrorContains(t, err, "parameter 'position' must be greater than zero")

			_, err = engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 1, -1) FROM mytable", nil)
			require.ErrorContains(t, err, "parameter 'length' cannot be negative")

			rows, err := engine.queryAll(context.Background(), nil, "SELECT SUBSTRING(NULL, 8, 0) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.True(t, rows[0].ValuesByPosition[0].IsNull())
			require.Equal(t, VarcharType, rows[0].ValuesByPosition[0].Type())

			rows, err = engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 8, 0) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "", rows[0].ValuesByPosition[0].RawValue().(string))

			rows, err = engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 8, 6) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "immudb", rows[0].ValuesByPosition[0].RawValue().(string))

			rows, err = engine.queryAll(context.Background(), nil, "SELECT SUBSTRING('Hello, immudb!', 8, 100) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "immudb!", rows[0].ValuesByPosition[0].RawValue().(string))
		})

		t.Run("trim", func(t *testing.T) {
			_, err := engine.queryAll(context.Background(), nil, "SELECT TRIM(1) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			_, err = engine.queryAll(context.Background(), nil, "SELECT TRIM(NULL, 1) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			rows, err := engine.queryAll(context.Background(), nil, "SELECT TRIM(NULL) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.True(t, rows[0].ValuesByPosition[0].IsNull())
			require.Equal(t, VarcharType, rows[0].ValuesByPosition[0].Type())

			rows, err = engine.queryAll(context.Background(), nil, "SELECT TRIM('      \t\n\r        Hello, immudb!  ') FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "Hello, immudb!", rows[0].ValuesByPosition[0].RawValue().(string))
		})

		t.Run("concat", func(t *testing.T) {
			_, err := engine.queryAll(context.Background(), nil, "SELECT CONCAT() FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			_, err = engine.queryAll(context.Background(), nil, "SELECT CONCAT('ciao', NULL, true) FROM mytable", nil)
			require.ErrorContains(t, err, "'CONCAT' function doesn't accept arguments of type BOOL")

			rows, err := engine.queryAll(context.Background(), nil, "SELECT CONCAT('Hello', ', ', NULL, 'immudb', NULL, '!') FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "Hello, immudb!", rows[0].ValuesByPosition[0].RawValue().(string))
		})

		t.Run("upper/lower", func(t *testing.T) {
			_, err := engine.queryAll(context.Background(), nil, "SELECT UPPER(1) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			_, err = engine.queryAll(context.Background(), nil, "SELECT LOWER(NULL, 1) FROM mytable", nil)
			require.ErrorIs(t, err, ErrIllegalArguments)

			rows, err := engine.queryAll(context.Background(), nil, "SELECT UPPER(NULL), LOWER(NULL) FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.True(t, rows[0].ValuesByPosition[0].IsNull())
			require.True(t, rows[0].ValuesByPosition[1].IsNull())

			rows, err = engine.queryAll(context.Background(), nil, "SELECT UPPER('immudb'), LOWER('IMMUDB') FROM mytable", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)

			require.Equal(t, "IMMUDB", rows[0].ValuesByPosition[0].RawValue().(string))
			require.Equal(t, "immudb", rows[0].ValuesByPosition[1].RawValue().(string))
		})
	})

	t.Run("json functions", func(t *testing.T) {
		_, err := engine.queryAll(context.Background(), nil, "SELECT JSON_TYPEOF(true) FROM mytable", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = engine.queryAll(context.Background(), nil, "SELECT JSON_TYPEOF('{}'::JSON, 1) FROM mytable", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		rows, err := engine.queryAll(context.Background(), nil, "SELECT JSON_TYPEOF(NULL) FROM mytable", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Nil(t, rows[0].ValuesByPosition[0].RawValue())

		rows, err = engine.queryAll(context.Background(), nil, "SELECT JSON_TYPEOF('{}'::JSON) FROM mytable", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "OBJECT", rows[0].ValuesByPosition[0].RawValue().(string))
	})
}

func TestTableResolver(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	r := &mockTableResolver{
		name: "my_table",
		cols: []ColDescriptor{
			{Column: "varchar_col", Type: VarcharType},
			{Column: "int_col", Type: IntegerType},
			{Column: "bool_col", Type: BooleanType},
		},
		values: [][]ValueExp{{NewVarchar("test"), NewInteger(1), NewBool(true)}},
	}

	engine, err := NewEngine(
		st,
		DefaultOptions().
			WithPrefix(sqlPrefix).
			WithTableResolvers(r),
	)
	require.NoError(t, err)

	assertQueryShouldProduceResults(
		t,
		engine,
		"SELECT int_col, varchar_col, bool_col FROM my_table",
		`SELECT * FROM (
			VALUES
				(1, 'test', true)
		)`,
	)
}

func assertQueryShouldProduceResults(t *testing.T, e *Engine, query, resultQuery string) {
	queryReader, err := e.Query(context.Background(), nil, query, nil)
	require.NoError(t, err)
	defer queryReader.Close()

	resultReader, err := e.Query(context.Background(), nil, resultQuery, nil)
	require.NoError(t, err)
	defer resultReader.Close()

	for {
		actualRow, actualErr := queryReader.Read(context.Background())
		expectedRow, expectedErr := resultReader.Read(context.Background())
		require.Equal(t, expectedErr, actualErr)

		if errors.Is(actualErr, ErrNoMoreRows) {
			break
		}
		require.Equal(t, expectedRow.ValuesByPosition, actualRow.ValuesByPosition)
	}
}

type mockTableResolver struct {
	name   string
	cols   []ColDescriptor
	values [][]ValueExp
}

func (r *mockTableResolver) Table() string {
	return r.name
}

func (r *mockTableResolver) Resolve(ctx context.Context, tx *SQLTx, alias string) (RowReader, error) {
	return NewValuesRowReader(
		tx,
		nil,
		r.cols,
		false,
		r.name,
		r.values,
	)
}
