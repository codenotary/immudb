/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package sys_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInfoSchemaTables_ReflectsUserTables asserts every CREATE TABLE
// produces exactly one row in information_schema.tables with
// table_type='BASE TABLE'. Alembic / Flyway / JDBC all filter on this
// to enumerate user tables during connect.
func TestInfoSchemaTables_ReflectsUserTables(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE t_alpha (id INTEGER, PRIMARY KEY id)`)
	exec(t, e, `CREATE TABLE t_beta (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e,
		`SELECT table_name, table_type FROM information_schema_tables
		 WHERE table_schema = 'public' ORDER BY table_name`)
	require.Len(t, rows, 2)
	require.Equal(t, "t_alpha", rows[0][0])
	require.Equal(t, "BASE TABLE", rows[0][1])
	require.Equal(t, "t_beta", rows[1][0])
}

// TestInfoSchemaColumns_ReflectsColumns asserts the per-column row
// shape matches what clients hardcode: is_nullable 'YES'/'NO',
// data_type strings, character_maximum_length for VARCHAR.
func TestInfoSchemaColumns_ReflectsColumns(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE employees (
		id INTEGER AUTO_INCREMENT,
		email VARCHAR[128] NOT NULL,
		bio VARCHAR,
		PRIMARY KEY id
	)`)

	rows := query(t, e,
		`SELECT column_name, data_type, is_nullable, character_maximum_length
		 FROM information_schema_columns
		 WHERE table_name = 'employees' ORDER BY ordinal_position`)
	require.Len(t, rows, 3)

	// id: bigint, NO null (PK), no max len
	require.Equal(t, "id", rows[0][0])
	require.Equal(t, "bigint", rows[0][1])
	require.Equal(t, "NO", rows[0][2])

	// email: text (our legacy shape for VARCHAR), NO, 128
	require.Equal(t, "email", rows[1][0])
	require.Equal(t, "text", rows[1][1])
	require.Equal(t, "NO", rows[1][2])
	require.Equal(t, int64(128), rows[1][3])

	// bio: text, YES, NULL
	require.Equal(t, "bio", rows[2][0])
	require.Equal(t, "text", rows[2][1])
	require.Equal(t, "YES", rows[2][2])
}

// TestInfoSchemaSchemata_ThreeStaticRows pins the three logical
// schemas immudb exposes. Matches pg_namespace's three rows but with
// the info_schema column shape.
func TestInfoSchemaSchemata_ThreeStaticRows(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e,
		`SELECT schema_name FROM information_schema_schemata ORDER BY schema_name`)
	require.Len(t, rows, 3)
	require.Equal(t, "information_schema", rows[0][0])
	require.Equal(t, "pg_catalog", rows[1][0])
	require.Equal(t, "public", rows[2][0])
}

// TestInfoSchemaKeyColumnUsage_PrimaryKey asserts PK columns appear
// with constraint_name = <table>_pkey (matching pg_constraint's naming
// convention), ordinal_position counting from 1.
func TestInfoSchemaKeyColumnUsage_PrimaryKey(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE orders (id INTEGER, code VARCHAR[16], PRIMARY KEY id)`)

	rows := query(t, e,
		`SELECT constraint_name, column_name, ordinal_position
		 FROM information_schema_key_column_usage
		 WHERE table_name = 'orders'`)
	require.Len(t, rows, 1)
	require.Equal(t, "orders_pkey", rows[0][0])
	require.Equal(t, "id", rows[0][1])
	require.Equal(t, int64(1), rows[0][2])
}

// TestInfoSchemaTableConstraints_PrimaryKeyAndUnique asserts both PK
// ('PRIMARY KEY') and UNIQUE ('UNIQUE') constraints show up with the
// SQL-standard constraint_type strings.
func TestInfoSchemaTableConstraints_PrimaryKeyAndUnique(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE items (id INTEGER, sku VARCHAR[32], PRIMARY KEY id)`)
	exec(t, e, `CREATE UNIQUE INDEX ON items (sku)`)

	rows := query(t, e,
		`SELECT constraint_name, constraint_type
		 FROM information_schema_table_constraints
		 WHERE table_name = 'items' ORDER BY constraint_type`)
	require.GreaterOrEqual(t, len(rows), 2)

	seen := map[string]bool{}
	for _, r := range rows {
		seen[r[1].(string)] = true
	}
	require.True(t, seen["PRIMARY KEY"], "expected a PRIMARY KEY constraint row")
	require.True(t, seen["UNIQUE"], "expected a UNIQUE constraint row")
}

// TestInfoSchema_JoinAcrossTables exercises the main reason A4 exists:
// clients (Hibernate, JDBC DatabaseMetaData.getColumns) issue JOINs
// between info_schema views. The pre-A4 canned handler could not
// serve these; the engine passthrough does.
func TestInfoSchema_JoinAcrossTables(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE customers (id INTEGER, email VARCHAR[64], PRIMARY KEY id)`)

	// Classic "columns of all tables with a PK" JOIN.
	rows := query(t, e,
		`SELECT c.column_name, k.constraint_name
		 FROM information_schema_columns c
		 JOIN information_schema_key_column_usage k
		   ON c.table_name = k.table_name AND c.column_name = k.column_name
		 WHERE c.table_name = 'customers'`)
	require.Len(t, rows, 1)
	require.Equal(t, "id", rows[0][0])
	require.Equal(t, "customers_pkey", rows[0][1])
}
