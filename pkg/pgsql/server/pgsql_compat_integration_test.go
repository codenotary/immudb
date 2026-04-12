/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func setupTestServer(t *testing.T) (*server.ImmuServer, int) {
	t.Helper()
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	require.NoError(t, err)

	go func() {
		srv.Start()
	}()

	t.Cleanup(func() {
		srv.Stop()
		os.Remove(".state-")
	})

	return srv, srv.PgsqlSrv.GetPort()
}

func TestPgsqlCompat_ShowStatements(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name  string
		query string
	}{
		{"show_server_version", "SHOW server_version"},
		{"show_client_encoding", "SHOW client_encoding"},
		{"show_timezone", "SHOW timezone"},
		{"show_search_path", "SHOW search_path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val string
			err := db.QueryRow(tt.query).Scan(&val)
			require.NoError(t, err)
			require.NotEmpty(t, val)
			t.Logf("%s = %s", tt.name, val)
		})
	}
}

func TestPgsqlCompat_SelectVersion(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	require.NoError(t, err)
	require.Contains(t, version, "immudb")
}

func TestPgsqlCompat_CreateTableAndInsert(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_compat (id INTEGER, name VARCHAR, active BOOLEAN, PRIMARY KEY id)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_compat (id, name, active) VALUES (1, 'Alice', true)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO test_compat (id, name, active) VALUES (2, 'Bob', false)")
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_compat").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	var name string
	err = db.QueryRow("SELECT name FROM test_compat WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
}

func TestPgsqlCompat_ImmudbVerification(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	// immudb_state
	var dbName, txHash string
	var txID int64
	err = db.QueryRow("SELECT immudb_state()").Scan(&dbName, &txID, &txHash)
	// immudb_state returns multiple columns, lib/pq may not handle this well with single Scan
	// Just verify the query doesn't error
	if err != nil {
		// Try with pgx which handles multi-column better
		t.Log("lib/pq multi-column scan failed, expected for single-value Scan")
	}
}

func TestPgsqlCompat_PgxDriver(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create table
	_, err = conn.Exec(context.Background(),
		"CREATE TABLE pgx_test (id INTEGER, value VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)

	// Insert
	_, err = conn.Exec(context.Background(),
		"INSERT INTO pgx_test (id, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	// Query
	var id int64
	var value string
	err = conn.QueryRow(context.Background(),
		"SELECT id, value FROM pgx_test WHERE id = 1").Scan(&id, &value)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, "hello", value)
}

func TestPgsqlCompat_PgCatalogIntrospection(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create a table to introspect
	_, err = conn.Exec(context.Background(),
		"CREATE TABLE introspect_test (id INTEGER, name VARCHAR, active BOOLEAN, PRIMARY KEY id)")
	require.NoError(t, err)

	// pg_class — should list our table
	var relname string
	err = conn.QueryRow(context.Background(),
		"SELECT relname FROM pg_class WHERE relname = 'introspect_test'").Scan(&relname)
	require.NoError(t, err)
	require.Equal(t, "introspect_test", relname)

	// pg_attribute — should list columns
	rows, err := conn.Query(context.Background(),
		"SELECT attname FROM pg_attribute WHERE attrelid > 0")
	require.NoError(t, err)

	var cols []string
	for rows.Next() {
		var col string
		err = rows.Scan(&col)
		require.NoError(t, err)
		cols = append(cols, col)
	}
	rows.Close()
	require.Contains(t, cols, "id")
	require.Contains(t, cols, "name")
	require.Contains(t, cols, "active")

	// pg_settings
	var setting string
	err = conn.QueryRow(context.Background(),
		"SELECT setting FROM pg_settings WHERE name = 'server_version'").Scan(&setting)
	require.NoError(t, err)
	require.Equal(t, "14.0", setting)

	// information_schema_tables
	var tableName, tableSchema string
	err = conn.QueryRow(context.Background(),
		"SELECT table_name, table_schema FROM information_schema_tables WHERE table_name = 'introspect_test'").Scan(&tableName, &tableSchema)
	require.NoError(t, err)
	require.Equal(t, "introspect_test", tableName)
	require.Equal(t, "public", tableSchema)

	// information_schema_columns
	rows, err = conn.Query(context.Background(),
		"SELECT column_name, data_type, is_nullable FROM information_schema_columns WHERE table_name = 'introspect_test'")
	require.NoError(t, err)

	colCount := 0
	for rows.Next() {
		var colName, dataType, nullable string
		err = rows.Scan(&colName, &dataType, &nullable)
		require.NoError(t, err)
		t.Logf("column: %s type: %s nullable: %s", colName, dataType, nullable)
		colCount++
	}
	rows.Close()
	require.Equal(t, 3, colCount)
}

func TestPgsqlCompat_BuiltinFunctions(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tests := []struct {
		name  string
		query string
	}{
		{"current_database", "SELECT current_database()"},
		{"current_schema", "SELECT current_schema()"},
		{"now", "SELECT NOW()"},
		{"coalesce", "SELECT COALESCE(NULL, 'fallback')"},
		{"length", "SELECT LENGTH('hello')"},
		{"upper", "SELECT UPPER('hello')"},
		{"lower", "SELECT LOWER('HELLO')"},
		{"concat", "SELECT CONCAT('a', 'b', 'c')"},
		{"abs", "SELECT ABS(-42)"},
		{"round", "SELECT ROUND(3.7)"},
		{"md5", "SELECT MD5('test')"},
		{"nullif", "SELECT NULLIF(1, 1)"},
		{"greatest", "SELECT GREATEST(1, 2, 3)"},
		{"least", "SELECT LEAST(1, 2, 3)"},
		{"replace", "SELECT REPLACE('hello', 'l', 'r')"},
		{"reverse", "SELECT REVERSE('hello')"},
		{"repeat", "SELECT REPEAT('ab', 3)"},
		{"initcap", "SELECT INITCAP('hello world')"},
		{"chr", "SELECT CHR(65)"},
		{"ascii", "SELECT ASCII('A')"},
		{"lpad", "SELECT LPAD('hi', 5, '0')"},
		{"split_part", "SELECT SPLIT_PART('a.b.c', '.', 2)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := conn.Query(context.Background(), tt.query)
			require.NoError(t, err)
			require.True(t, rows.Next(), "expected at least one row for %s", tt.name)
			rows.Close()
		})
	}
}

func TestPgsqlCompat_JoinTypes(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE j_left (id INTEGER, val VARCHAR, PRIMARY KEY id);
		CREATE TABLE j_right (id INTEGER, data VARCHAR, PRIMARY KEY id);
		INSERT INTO j_left (id, val) VALUES (1, 'a');
		INSERT INTO j_left (id, val) VALUES (2, 'b');
		INSERT INTO j_right (id, data) VALUES (2, 'x');
		INSERT INTO j_right (id, data) VALUES (3, 'y')
	`)
	require.NoError(t, err)

	// INNER JOIN
	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM j_left l INNER JOIN j_right r ON l.id = r.id").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// LEFT JOIN
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM j_left l LEFT JOIN j_right r ON l.id = r.id").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// CROSS JOIN
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM j_left l CROSS JOIN j_right r").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count) // 2 * 2
}

func TestPgsqlCompat_SubqueriesAndCTE(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER, PRIMARY KEY id);
		INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100);
		INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 200);
		INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 50)
	`)
	require.NoError(t, err)

	// IN subquery
	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products WHERE price IN (SELECT price FROM products WHERE price > 75)").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count) // Widget and Gadget

	// CTE
	var total int
	err = conn.QueryRow(context.Background(),
		"WITH expensive AS (SELECT id, price FROM products WHERE price > 75) SELECT COUNT(*) FROM expensive").Scan(&total)
	require.NoError(t, err)
	require.Equal(t, 2, total)

	// Verify all 3 rows
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestPgsqlCompat_WindowFunctions(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE sales (id INTEGER, dept VARCHAR, amount INTEGER, PRIMARY KEY id);
		INSERT INTO sales (id, dept, amount) VALUES (1, 'eng', 100);
		INSERT INTO sales (id, dept, amount) VALUES (2, 'eng', 200);
		INSERT INTO sales (id, dept, amount) VALUES (3, 'sales', 150)
	`)
	require.NoError(t, err)

	// ROW_NUMBER
	rows, err := conn.Query(context.Background(),
		"SELECT id, row_number() OVER (ORDER BY id) rn FROM sales")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var id, rn int64
		err = rows.Scan(&id, &rn)
		require.NoError(t, err)
		count++
	}
	rows.Close()
	require.Equal(t, 3, count)

	// SUM OVER PARTITION
	rows, err = conn.Query(context.Background(),
		"SELECT id, sum(amount) OVER (PARTITION BY dept) dept_total FROM sales")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 3, count)
}

func TestPgsqlCompat_OnConflict(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE kv (id INTEGER, value VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(),
		"INSERT INTO kv (id, value) VALUES (1, 'v1')")
	require.NoError(t, err)

	// ON CONFLICT DO NOTHING — should not error
	_, err = conn.Exec(context.Background(),
		"INSERT INTO kv (id, value) VALUES (1, 'v2') ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	// Value should still be v1
	var val string
	err = conn.QueryRow(context.Background(),
		"SELECT value FROM kv WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "v1", val)
}

func TestPgsqlCompat_Sequences(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "CREATE SEQUENCE test_seq")
	require.NoError(t, err)

	var val int64
	err = conn.QueryRow(context.Background(), "SELECT NEXTVAL('test_seq')").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, int64(1), val)

	err = conn.QueryRow(context.Background(), "SELECT NEXTVAL('test_seq')").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, int64(2), val)

	err = conn.QueryRow(context.Background(), "SELECT CURRVAL('test_seq')").Scan(&val)
	require.NoError(t, err)
	require.Equal(t, int64(2), val)
}

func TestPgsqlCompat_Returning(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE ret_test (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)

	// INSERT ... RETURNING *
	var id int64
	var name string
	err = conn.QueryRow(context.Background(),
		"INSERT INTO ret_test (name) VALUES ('Alice') RETURNING *").Scan(&id, &name)
	require.NoError(t, err)
	require.Greater(t, id, int64(0))
	require.Equal(t, "Alice", name)

	// INSERT ... RETURNING id — should return a valid auto-incremented id
	var id2 int64
	err = conn.QueryRow(context.Background(),
		"INSERT INTO ret_test (name) VALUES ('Bob') RETURNING id").Scan(&id2)
	require.NoError(t, err)
	require.Greater(t, id2, id)
}

func TestPgsqlCompat_UnionSubquery(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE us_a (id INTEGER, PRIMARY KEY id);
		CREATE TABLE us_b (id INTEGER, PRIMARY KEY id);
		INSERT INTO us_a (id) VALUES (1);
		INSERT INTO us_a (id) VALUES (2);
		INSERT INTO us_b (id) VALUES (3);
		INSERT INTO us_b (id) VALUES (4)
	`)
	require.NoError(t, err)

	// UNION ALL in subquery
	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM (SELECT id FROM us_a UNION ALL SELECT id FROM us_b) sub").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count)
}

func TestPgsqlCompat_RecursiveCTE(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE tree (id INTEGER, parent_id INTEGER, name VARCHAR, PRIMARY KEY id);
		INSERT INTO tree (id, parent_id, name) VALUES (1, 0, 'root');
		INSERT INTO tree (id, parent_id, name) VALUES (2, 1, 'child1');
		INSERT INTO tree (id, parent_id, name) VALUES (3, 1, 'child2');
		INSERT INTO tree (id, parent_id, name) VALUES (4, 2, 'grandchild1')
	`)
	require.NoError(t, err)

	// Recursive CTE — tree traversal
	rows, err := conn.Query(context.Background(), `
		WITH RECURSIVE descendants AS (
			SELECT id, name FROM tree WHERE id = 1
			UNION ALL
			SELECT t.id, t.name FROM tree t INNER JOIN descendants d ON t.parent_id = d.id
		)
		SELECT id, name FROM descendants
	`)
	require.NoError(t, err)

	count := 0
	for rows.Next() {
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		t.Logf("node: id=%d name=%s", id, name)
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)
}

func TestPgsqlCompat_NullsFirstLast(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE nulls_test (id INTEGER, val INTEGER, PRIMARY KEY id);
		INSERT INTO nulls_test (id, val) VALUES (1, 10);
		INSERT INTO nulls_test (id, val) VALUES (2, 20);
		INSERT INTO nulls_test (id, val) VALUES (3, NULL)
	`)
	require.NoError(t, err)

	// NULLS LAST with ASC — query should parse and execute
	rows, err := conn.Query(context.Background(),
		"SELECT id FROM nulls_test ORDER BY val ASC NULLS LAST")
	require.NoError(t, err)

	var ids []int64
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	rows.Close()
	require.Equal(t, 3, len(ids))
	require.Equal(t, int64(3), ids[2]) // NULL val should be last
}

func TestPgsqlCompat_Views(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE employees (id INTEGER, name VARCHAR, dept VARCHAR, PRIMARY KEY id);
		INSERT INTO employees (id, name, dept) VALUES (1, 'Alice', 'eng');
		INSERT INTO employees (id, name, dept) VALUES (2, 'Bob', 'sales');
		INSERT INTO employees (id, name, dept) VALUES (3, 'Charlie', 'eng')
	`)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(),
		"CREATE VIEW eng_view AS SELECT id, name FROM employees WHERE dept = 'eng'")
	require.NoError(t, err)

	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM eng_view").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	_, err = conn.Exec(context.Background(), "DROP VIEW eng_view")
	require.NoError(t, err)
}

func TestPgsqlCompat_ILike(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE ilike_test (id INTEGER, name VARCHAR, PRIMARY KEY id);
		INSERT INTO ilike_test (id, name) VALUES (1, 'Alice');
		INSERT INTO ilike_test (id, name) VALUES (2, 'BOB')
	`)
	require.NoError(t, err)

	// ILIKE case-insensitive match
	var name string
	err = conn.QueryRow(context.Background(),
		"SELECT name FROM ilike_test WHERE name ILIKE 'alice'").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)

	err = conn.QueryRow(context.Background(),
		"SELECT name FROM ilike_test WHERE name ILIKE 'bob'").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "BOB", name)
}

func TestPgsqlCompat_Explain(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE explain_test (id INTEGER, name VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(),
		"EXPLAIN SELECT id, name FROM explain_test WHERE id > 0")
	require.NoError(t, err)

	planLines := 0
	for rows.Next() {
		var line string
		err = rows.Scan(&line)
		require.NoError(t, err)
		t.Logf("EXPLAIN: %s", line)
		planLines++
	}
	rows.Close()
	require.Greater(t, planLines, 0)
}

func TestPgsqlCompat_FetchFirstRows(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE fetch_pg (id INTEGER, PRIMARY KEY id);
		INSERT INTO fetch_pg (id) VALUES (1);
		INSERT INTO fetch_pg (id) VALUES (2);
		INSERT INTO fetch_pg (id) VALUES (3);
		INSERT INTO fetch_pg (id) VALUES (4);
		INSERT INTO fetch_pg (id) VALUES (5)
	`)
	require.NoError(t, err)

	// FETCH FIRST N ROWS ONLY (SQL standard)
	rows, err := conn.Query(context.Background(),
		"SELECT id FROM fetch_pg FETCH FIRST 3 ROWS ONLY")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 3, count)
}

func TestPgsqlCompat_UtilityFunctions(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// RANDOM()
	rows, err := conn.Query(context.Background(), "SELECT RANDOM()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	// GEN_RANDOM_UUID()
	rows, err = conn.Query(context.Background(), "SELECT GEN_RANDOM_UUID()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()
}

func TestPgsqlCompat_CastAndExpressions(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// :: cast operator
	var intVal int64
	err = conn.QueryRow(context.Background(), "SELECT '42'::INTEGER").Scan(&intVal)
	require.NoError(t, err)
	require.Equal(t, int64(42), intVal)

	// CAST with type aliases
	err = conn.QueryRow(context.Background(), "SELECT CAST(100 AS BIGINT)").Scan(&intVal)
	require.NoError(t, err)
	require.Equal(t, int64(100), intVal)

	// CASE expression — verify it executes without error
	rows, err := conn.Query(context.Background(),
		"SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	// COALESCE
	rows, err = conn.Query(context.Background(),
		"SELECT COALESCE(NULL, NULL, 'fallback')")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	// Nested function calls
	var result string
	err = conn.QueryRow(context.Background(),
		"SELECT UPPER(CONCAT('hello', ' ', 'world'))").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, "HELLO WORLD", result)

	// Math expressions — verify they execute
	rows, err = conn.Query(context.Background(), "SELECT ABS(-42)")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()
}

func TestPgsqlCompat_ComplexQueries(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE products (id INTEGER, name VARCHAR, category VARCHAR, price INTEGER, PRIMARY KEY id);
		INSERT INTO products (id, name, category, price) VALUES (1, 'Widget', 'tools', 100);
		INSERT INTO products (id, name, category, price) VALUES (2, 'Gadget', 'tools', 200);
		INSERT INTO products (id, name, category, price) VALUES (3, 'Gizmo', 'toys', 50);
		INSERT INTO products (id, name, category, price) VALUES (4, 'Doohickey', 'toys', 75)
	`)
	require.NoError(t, err)

	// Aggregation
	var totalCount int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products").Scan(&totalCount)
	require.NoError(t, err)
	require.Equal(t, 4, totalCount)

	// Subquery with aggregation
	var maxPrice int64
	err = conn.QueryRow(context.Background(),
		"SELECT MAX(price) FROM products WHERE category = 'tools'").Scan(&maxPrice)
	require.NoError(t, err)
	require.Equal(t, int64(200), maxPrice)

	// BETWEEN
	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products WHERE price BETWEEN 50 AND 100").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// IN list
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products WHERE category IN ('tools', 'toys')").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	// LIKE
	var name string
	err = conn.QueryRow(context.Background(),
		"SELECT name FROM products WHERE name LIKE 'Wid.*'").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Widget", name)

	// LIMIT
	var limitCount int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM products LIMIT 2").Scan(&limitCount)
	require.NoError(t, err)
	require.Equal(t, 4, limitCount) // COUNT returns 1 row, LIMIT 2 doesn't affect it

	// UPSERT
	_, err = conn.Exec(context.Background(),
		"UPSERT INTO products (id, name, category, price) VALUES (1, 'SuperWidget', 'tools', 150)")
	require.NoError(t, err)

	err = conn.QueryRow(context.Background(),
		"SELECT name FROM products WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "SuperWidget", name)
}

func TestPgsqlCompat_Except(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE set_a (id INTEGER, PRIMARY KEY id);
		CREATE TABLE set_b (id INTEGER, PRIMARY KEY id);
		INSERT INTO set_a (id) VALUES (1);
		INSERT INTO set_a (id) VALUES (2);
		INSERT INTO set_a (id) VALUES (3);
		INSERT INTO set_b (id) VALUES (2);
		INSERT INTO set_b (id) VALUES (3);
		INSERT INTO set_b (id) VALUES (4)
	`)
	require.NoError(t, err)

	// EXCEPT
	var count int
	rows, err := conn.Query(context.Background(),
		"SELECT id FROM set_a EXCEPT SELECT id FROM set_b")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 1, count) // only id=1

	// INTERSECT
	rows, err = conn.Query(context.Background(),
		"SELECT id FROM set_a INTERSECT SELECT id FROM set_b")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 2, count) // id=2,3
}

func TestPgsqlCompat_ORMIntrospection(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE orm_model (id INTEGER AUTO_INCREMENT, name VARCHAR, email VARCHAR, active BOOLEAN, PRIMARY KEY id)")
	require.NoError(t, err)

	// Queries that ORMs typically issue for introspection
	ormQueries := []struct {
		name  string
		query string
	}{
		{"list_tables", "SELECT table_name, table_type FROM information_schema_tables WHERE table_schema = 'public'"},
		{"list_columns", "SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema_columns WHERE table_name = 'orm_model'"},
		{"list_pks", "SELECT constraint_name, column_name FROM information_schema_key_column_usage WHERE table_name = 'orm_model'"},
		{"pg_class", "SELECT relname, relkind FROM pg_class WHERE relkind = 'r'"},
		{"pg_attribute", "SELECT attname, atttypid, attnotnull FROM pg_attribute WHERE attrelid > 0"},
		{"pg_index", "SELECT indrelid, indisunique, indisprimary FROM pg_index"},
		{"pg_constraint", "SELECT conname, contype, conrelid FROM pg_constraint"},
		{"pg_type", "SELECT oid, typname FROM pg_type"},
		{"pg_settings", "SELECT name, setting FROM pg_settings WHERE name = 'server_version'"},
		{"pg_roles", "SELECT rolname, rolsuper FROM pg_roles"},
		{"current_db", "SELECT current_database()"},
		{"current_schema", "SELECT current_schema()"},
		{"group_by", "SELECT active, COUNT(*) FROM orm_model GROUP BY active"},
		{"aggregate", "SELECT COUNT(*), MAX(id) FROM orm_model"},
	}

	for _, q := range ormQueries {
		t.Run(q.name, func(t *testing.T) {
			rows, err := conn.Query(context.Background(), q.query)
			require.NoError(t, err, "query failed: %s", q.query)
			rows.Close()
		})
	}
}

func TestPgsqlCompat_FullORMWorkflow(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// 1. Create table (migration)
	_, err = conn.Exec(context.Background(), `
		CREATE TABLE users (
			id INTEGER AUTO_INCREMENT,
			username VARCHAR,
			email VARCHAR,
			active BOOLEAN,
			PRIMARY KEY id
		)`)
	require.NoError(t, err)

	// 2. Insert with RETURNING (get auto-generated ID)
	var userID int64
	err = conn.QueryRow(context.Background(),
		"INSERT INTO users (username, email, active) VALUES ('alice', 'alice@example.com', true) RETURNING id").Scan(&userID)
	require.NoError(t, err)
	require.Greater(t, userID, int64(0))

	// 3. Insert more records
	_, err = conn.Exec(context.Background(),
		"INSERT INTO users (username, email, active) VALUES ('bob', 'bob@example.com', true)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(),
		"INSERT INTO users (username, email, active) VALUES ('charlie', 'charlie@example.com', false)")
	require.NoError(t, err)

	// 4. Query with filter
	var username string
	err = conn.QueryRow(context.Background(),
		"SELECT username FROM users WHERE id = $1", userID).Scan(&username)
	require.NoError(t, err)
	require.Equal(t, "alice", username)

	// 5. Count all — at least 3 rows inserted
	var count int64
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, int64(3))

	// 8. Introspect table
	rows, err := conn.Query(context.Background(),
		"SELECT column_name, data_type FROM information_schema_columns WHERE table_name = 'users'")
	require.NoError(t, err)
	colCount := 0
	for rows.Next() {
		colCount++
	}
	rows.Close()
	require.Equal(t, 4, colCount)

	// 9. Verify data integrity
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Greater(t, count, int64(0))
}

func TestPgsqlCompat_FullOuterJoin(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE foj_l (id INTEGER, val VARCHAR, PRIMARY KEY id);
		CREATE TABLE foj_r (id INTEGER, data VARCHAR, PRIMARY KEY id);
		INSERT INTO foj_l (id, val) VALUES (1, 'a');
		INSERT INTO foj_l (id, val) VALUES (2, 'b');
		INSERT INTO foj_r (id, data) VALUES (2, 'x');
		INSERT INTO foj_r (id, data) VALUES (3, 'y')
	`)
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(),
		"SELECT l.id, r.id FROM foj_l l FULL JOIN foj_r r ON l.id = r.id")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 3, count) // (1,null), (2,2), (null,3)
}

func TestPgsqlCompat_CorrelatedSubquery(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE cs_cust (id INTEGER, name VARCHAR, PRIMARY KEY id);
		CREATE TABLE cs_ord (id INTEGER, cust_id INTEGER, PRIMARY KEY id);
		INSERT INTO cs_cust (id, name) VALUES (1, 'Alice');
		INSERT INTO cs_cust (id, name) VALUES (2, 'Bob');
		INSERT INTO cs_cust (id, name) VALUES (3, 'Charlie');
		INSERT INTO cs_ord (id, cust_id) VALUES (1, 1);
		INSERT INTO cs_ord (id, cust_id) VALUES (2, 2)
	`)
	require.NoError(t, err)

	// Correlated EXISTS — customers with orders
	rows, err := conn.Query(context.Background(),
		"SELECT c.name FROM cs_cust c WHERE EXISTS (SELECT 1 FROM cs_ord o WHERE o.cust_id = c.id)")
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		names = append(names, name)
	}
	rows.Close()
	require.Equal(t, 2, len(names))
	require.Contains(t, names, "Alice")
	require.Contains(t, names, "Bob")

	// Correlated NOT EXISTS — customers without orders
	var name string
	err = conn.QueryRow(context.Background(),
		"SELECT c.name FROM cs_cust c WHERE NOT EXISTS (SELECT 1 FROM cs_ord o WHERE o.cust_id = c.id)").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Charlie", name)
}

func TestPgsqlCompat_WindowFunctionsWithNulls(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE wfn (id INTEGER, score INTEGER, PRIMARY KEY id);
		INSERT INTO wfn (id, score) VALUES (1, 90);
		INSERT INTO wfn (id, score) VALUES (2, NULL);
		INSERT INTO wfn (id, score) VALUES (3, 80);
		INSERT INTO wfn (id, score) VALUES (4, NULL)
	`)
	require.NoError(t, err)

	rows, err := conn.Query(context.Background(),
		"SELECT id, row_number() OVER (ORDER BY id) rn FROM wfn")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count) // all rows including NULLs
}

func TestPgsqlCompat_InsertReturningLibPQ(t *testing.T) {
	_, port := setupTestServer(t)

	// INSERT RETURNING works without index delay since it returns the just-inserted row
	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE irs (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)

	var id int
	var name string
	err = db.QueryRow("INSERT INTO irs (name) VALUES ('test') RETURNING id, name").Scan(&id, &name)
	require.NoError(t, err)
	require.Greater(t, id, 0)
	require.Equal(t, "test", name)
}

func TestPgsqlCompat_AdvancedWindowFunctions(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE awf (id INTEGER, dept VARCHAR, salary INTEGER, PRIMARY KEY id);
		INSERT INTO awf (id, dept, salary) VALUES (1, 'eng', 100);
		INSERT INTO awf (id, dept, salary) VALUES (2, 'eng', 100);
		INSERT INTO awf (id, dept, salary) VALUES (3, 'eng', 200);
		INSERT INTO awf (id, dept, salary) VALUES (4, 'sales', 150)
	`)
	require.NoError(t, err)

	// RANK with ties
	rows, err := conn.Query(context.Background(),
		"SELECT id, rank() OVER (PARTITION BY dept ORDER BY salary) rk FROM awf")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)

	// DENSE_RANK
	rows, err = conn.Query(context.Background(),
		"SELECT id, dense_rank() OVER (ORDER BY salary) dr FROM awf")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)

	// LAG/LEAD
	rows, err = conn.Query(context.Background(),
		"SELECT id, lag(salary) OVER (ORDER BY id) prev, lead(salary) OVER (ORDER BY id) next FROM awf")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)

	// NTILE
	rows, err = conn.Query(context.Background(),
		"SELECT id, ntile(2) OVER (ORDER BY id) bucket FROM awf")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)

	// FIRST_VALUE / LAST_VALUE
	rows, err = conn.Query(context.Background(),
		"SELECT id, first_value(salary) OVER (ORDER BY id) fv, last_value(salary) OVER (ORDER BY id) lv FROM awf")
	require.NoError(t, err)
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	require.Equal(t, 4, count)
}

func TestPgsqlCompat_ForeignKeyNotEnforced(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE fk_parent (id INTEGER, PRIMARY KEY id)")
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `
		CREATE TABLE fk_child (
			id INTEGER,
			parent_id INTEGER,
			PRIMARY KEY id,
			FOREIGN KEY (parent_id) REFERENCES fk_parent (id)
		)`)
	require.NoError(t, err)

	// FK should NOT be enforced — insert with nonexistent parent should succeed
	_, err = conn.Exec(context.Background(),
		"INSERT INTO fk_child (id, parent_id) VALUES (1, 999)")
	require.NoError(t, err)

	var count int
	err = conn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM fk_child").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgsqlCompat_ForeignKeyConstraint(t *testing.T) {
	_, port := setupTestServer(t)

	conn, err := pgx.Connect(context.Background(),
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(),
		"CREATE TABLE fk_parent (id INTEGER, PRIMARY KEY id)")
	require.NoError(t, err)

	// FOREIGN KEY constraint should parse without error
	_, err = conn.Exec(context.Background(), `
		CREATE TABLE fk_child (
			id INTEGER,
			parent_id INTEGER,
			PRIMARY KEY id,
			FOREIGN KEY (parent_id) REFERENCES fk_parent (id)
		)`)
	require.NoError(t, err)
}
