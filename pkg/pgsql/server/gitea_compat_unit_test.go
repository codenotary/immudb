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

package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These unit tests pin the pure-function pieces of the XORM/Gitea compat
// work (commit 3cf2bb46). The three pg_catalog-view handlers themselves
// need a live session and are exercised in gitea_handlers_integration_test.go.

// TestCommandTagFor pins the CommandComplete tag produced for every verb
// pq / JDBC / XORM read off the wire to drive their transaction state
// machine. The previous catch-all "ok" response confused pq's
// transaction state machine with "unexpected command tag ok" right after
// BEGIN/COMMIT.
func TestCommandTagFor(t *testing.T) {
	cases := []struct {
		sql, want string
	}{
		{"BEGIN", "BEGIN"},
		{"  begin  ", "BEGIN"},
		{"START TRANSACTION", "BEGIN"},
		{"start  transaction read write", "BEGIN"},
		{"COMMIT", "COMMIT"},
		{"END", "COMMIT"},
		{"ROLLBACK", "ROLLBACK"},
		{"ABORT", "ROLLBACK"},
		{"INSERT INTO t VALUES (1)", "INSERT 0 0"},
		{"insert into t values (1)", "INSERT 0 0"},
		{"UPDATE t SET x = 1", "UPDATE 0"},
		{"DELETE FROM t", "DELETE 0"},
		{"SELECT * FROM t", "SELECT 0"},
		{"FETCH 10 FROM c", "FETCH 0"},
		{"CREATE TABLE t (id INT)", "CREATE TABLE"},
		{"create  unique  index ix ON t (a)", "CREATE UNIQUE INDEX"},
		{"DROP INDEX ix", "DROP INDEX"},
		{"ALTER TABLE t ADD COLUMN x INT", "ALTER TABLE"},
		{"TRUNCATE t", "TRUNCATE"},
		{"SET search_path = 'public'", "SET"},
		{"EXPLAIN SELECT 1", "EXPLAIN"},
		// Unknown verb → fall back to "ok" so existing behaviour is preserved.
		{"WHATEVER unknown", "ok"},
		{"", "ok"},
	}
	for _, c := range cases {
		got := commandTagFor(c.sql)
		require.Equalf(t, c.want, got, "commandTagFor(%q)", c.sql)
	}
}

// TestInferParamColsFromSQL verifies the $N placeholder counter used in
// the Extended Query Parse path for wire-emulated queries. Without this,
// ParameterDescription advertised zero parameters and every bind with
// N>0 values tripped pq's "got N parameters but the statement requires 0".
func TestInferParamColsFromSQL(t *testing.T) {
	t.Run("no placeholders returns nil", func(t *testing.T) {
		require.Nil(t, inferParamColsFromSQL("SELECT 1"))
		require.Nil(t, inferParamColsFromSQL("SELECT * FROM pg_tables"))
	})

	t.Run("counts highest marker", func(t *testing.T) {
		cols := inferParamColsFromSQL("SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3")
		require.Len(t, cols, 3)
		require.Equal(t, "param1", cols[0].Column)
		require.Equal(t, "param3", cols[2].Column)
	})

	t.Run("uses max not count", func(t *testing.T) {
		// $2 alone still advertises 2 params; lib/pq numbers from 1 and expects
		// exactly max($N) descriptors.
		cols := inferParamColsFromSQL("SELECT * FROM t WHERE a = $2")
		require.Len(t, cols, 2)
	})

	t.Run("skips $N inside string literals", func(t *testing.T) {
		// '$1' is masked by maskStringLiterals before the regex runs.
		// The only unquoted marker is $2, so we must advertise 2 params.
		cols := inferParamColsFromSQL(`SELECT * FROM t WHERE a = '$1 literal' AND b = $2`)
		require.Len(t, cols, 2)
	})

	t.Run("zero placeholders in literal-only query", func(t *testing.T) {
		require.Nil(t, inferParamColsFromSQL(`SELECT 'cost $1 per unit'`))
	})
}

// TestInjectAddColumnKeyword pins the `ALTER TABLE x ADD <coldef>` →
// `ALTER TABLE x ADD COLUMN <coldef>` rewrite. PG accepts both; immudb
// requires the explicit COLUMN. The rewrite must NOT fire for
// constraint forms (ADD CONSTRAINT / FOREIGN KEY / PRIMARY KEY /
// UNIQUE / CHECK / INDEX).
func TestInjectAddColumnKeyword(t *testing.T) {
	cases := []struct{ in, want string }{
		// Simple column add — inject COLUMN.
		{"ALTER TABLE t ADD name VARCHAR", "ALTER TABLE t ADD COLUMN name VARCHAR"},
		// Quoted identifier — still an identifier, still inject.
		{`ALTER TABLE t ADD "name" VARCHAR`, `ALTER TABLE t ADD COLUMN "name" VARCHAR`},
		// Already has COLUMN — idempotent.
		{"ALTER TABLE t ADD COLUMN name VARCHAR", "ALTER TABLE t ADD COLUMN name VARCHAR"},
		// Constraint forms — leave alone.
		{"ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)", "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)"},
		{"ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u(id)", "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u(id)"},
		{"ALTER TABLE t ADD PRIMARY KEY (id)", "ALTER TABLE t ADD PRIMARY KEY (id)"},
		{"ALTER TABLE t ADD UNIQUE (email)", "ALTER TABLE t ADD UNIQUE (email)"},
		{"ALTER TABLE t ADD CHECK (age > 0)", "ALTER TABLE t ADD CHECK (age > 0)"},
		{"ALTER TABLE t ADD INDEX ix (a)", "ALTER TABLE t ADD INDEX ix (a)"},
	}
	for _, c := range cases {
		got := injectAddColumnKeyword(c.in)
		require.Equalf(t, c.want, got, "injectAddColumnKeyword(%q)", c.in)
	}
}

// TestORMIntrospectionRegexes pins the three match rules that route
// pg_catalog-view queries to the emulated handlers instead of the
// engine. A false-positive match on these routes a regular query into
// the wrong handler; a false-negative makes ORM introspection fall
// through to the engine and surface the canned 1-row NULL response
// that breaks XORM Scan.
func TestORMIntrospectionRegexes(t *testing.T) {
	t.Run("pgTablesRe", func(t *testing.T) {
		matches := []string{
			`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
			`SELECT * FROM pg_catalog.pg_tables`,
			`select tablename from PG_TABLES where tablename = $1`,
		}
		nonMatches := []string{
			`SELECT * FROM my_tables`,
			`SELECT * FROM pg_tables_view_wannabe`,
			`SELECT * FROM pg_type`,
		}
		for _, q := range matches {
			require.Truef(t, pgTablesRe.MatchString(q), "pgTablesRe should match %q", q)
		}
		for _, q := range nonMatches {
			require.Falsef(t, pgTablesRe.MatchString(q), "pgTablesRe must NOT match %q", q)
		}
	})

	t.Run("xormColumnsRe", func(t *testing.T) {
		// XORM emits a big multi-JOIN SELECT with the distinctive
		// `column_name, column_default` signature on pg_attribute.
		matches := []string{
			`SELECT column_name, column_default, is_nullable, data_type FROM pg_attribute JOIN pg_class ON …`,
			`select column_name , column_default from pg_attribute where attrelid = 'x'::regclass`,
		}
		nonMatches := []string{
			// Missing column_default.
			`SELECT column_name FROM pg_attribute`,
			// Missing pg_attribute.
			`SELECT column_name, column_default FROM information_schema.columns`,
		}
		for _, q := range matches {
			require.Truef(t, xormColumnsRe.MatchString(q), "xormColumnsRe should match %q", q)
		}
		for _, q := range nonMatches {
			require.Falsef(t, xormColumnsRe.MatchString(q), "xormColumnsRe must NOT match %q", q)
		}
	})

	t.Run("pgIndexesRe", func(t *testing.T) {
		matches := []string{
			`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 't'`,
			`SELECT * FROM pg_catalog.pg_indexes`,
		}
		nonMatches := []string{
			`SELECT * FROM my_indexes`,
			`SELECT * FROM pg_stat_user_indexes`,
		}
		for _, q := range matches {
			require.Truef(t, pgIndexesRe.MatchString(q), "pgIndexesRe should match %q", q)
		}
		for _, q := range nonMatches {
			require.Falsef(t, pgIndexesRe.MatchString(q), "pgIndexesRe must NOT match %q", q)
		}
	})
}

// TestRemovePGCatalogReferences_DDLNormalization covers the DDL rewrites
// applied in bulk before a DDL statement reaches the immudb grammar:
// BOOL→BOOLEAN, CHAR(N)→VARCHAR[N], redundant `NULL` strip after a
// type or DEFAULT, and the quoted-schema-prefix strip. Each was added
// in 3cf2bb46 to accommodate XORM / Hibernate / JDBC DDL forms that
// PG accepts but immudb's grammar does not.
func TestRemovePGCatalogReferences_DDLNormalization(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		wantContains []string // all must appear in output
		wantAbsent   []string // none may appear in output
	}{
		{
			name:         "BOOL to BOOLEAN",
			in:           `CREATE TABLE t (flag BOOL NOT NULL)`,
			wantContains: []string{"BOOLEAN"},
			wantAbsent:   []string{" BOOL "},
		},
		{
			name:         "CHAR(N) to VARCHAR[N]",
			in:           `CREATE TABLE t (code CHAR(4) NOT NULL)`,
			wantContains: []string{"VARCHAR[4]"},
			wantAbsent:   []string{"CHAR(4)"},
		},
		{
			name:         "redundant NULL after type is stripped",
			in:           `CREATE TABLE t (name VARCHAR NULL)`,
			wantContains: []string{"VARCHAR"},
			wantAbsent:   []string{"VARCHAR NULL"},
		},
		{
			// removePGCatalogReferences also unquotes identifiers and adds a
			// default PRIMARY KEY for CREATE TABLE without one, so assert
			// only on the schema-prefix removal (not on identifier quoting).
			name:         "quoted schema prefix stripped",
			in:           `CREATE TABLE "public"."t" (id INTEGER)`,
			wantContains: []string{"CREATE TABLE"},
			wantAbsent:   []string{`"public".`, `"public" .`, `public.`},
		},
		{
			name:         "pg_catalog prefix stripped",
			in:           `SELECT * FROM pg_catalog.pg_type`,
			wantContains: []string{"pg_type"},
			wantAbsent:   []string{"pg_catalog."},
		},
		{
			// k3s/kine issues this CREATE TABLE when bootstrapping its
			// schema. The `COLLATE "C"` must be stripped for immudb to
			// parse it (immudb has no collation support). Note other
			// passes also rewrite this to add IF NOT EXISTS and a
			// default PRIMARY KEY — we assert only on the COLLATE strip.
			name:         "COLLATE quoted-identifier stripped",
			in:           `CREATE TABLE kine (name text COLLATE "C", id INTEGER)`,
			wantContains: []string{"kine", "name"},
			wantAbsent:   []string{"COLLATE", `"C"`},
		},
		{
			name:         "COLLATE bare-identifier stripped",
			in:           `CREATE TABLE t (s VARCHAR[64] COLLATE ucs_basic)`,
			wantContains: []string{"VARCHAR[64]"},
			wantAbsent:   []string{"COLLATE", "ucs_basic"},
		},
		{
			// Inline UNIQUE must become a trailing CREATE UNIQUE INDEX
			// — NOT be silently stripped (the prior regex broke
			// uniqueness guarantees). Assert the UNIQUE keyword is gone
			// from the column definition AND a CREATE UNIQUE INDEX
			// referencing the column has been appended.
			name: "inline UNIQUE becomes CREATE UNIQUE INDEX",
			in:   `CREATE TABLE t (id INTEGER, email VARCHAR(255) UNIQUE)`,
			wantContains: []string{
				"CREATE UNIQUE INDEX",
				"ON t(email)",
			},
			wantAbsent: []string{"UNIQUE)"},
		},
		{
			// Table-level UNIQUE (col1, col2) produces a CREATE UNIQUE
			// INDEX on the composite. The UNIQUE clause itself is
			// removed from the column list.
			name: "table-level UNIQUE(a,b) becomes CREATE UNIQUE INDEX",
			in:   `CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE (a, b))`,
			wantContains: []string{
				"CREATE UNIQUE INDEX",
				"ON t(a, b)",
			},
			wantAbsent: []string{"UNIQUE ("},
		},
		{
			// PG emits CREATE INDEX with a name; immudb's grammar
			// rejects names. Strip to the unnamed form.
			name:         "CREATE INDEX name stripped",
			in:           `CREATE INDEX idx_foo ON t(col)`,
			wantContains: []string{"CREATE INDEX ON t(col)"},
			wantAbsent:   []string{"idx_foo"},
		},
		{
			name:         "CREATE UNIQUE INDEX IF NOT EXISTS name stripped",
			in:           `CREATE UNIQUE INDEX IF NOT EXISTS idx_foo ON t(col)`,
			wantContains: []string{"CREATE UNIQUE INDEX IF NOT EXISTS ON t(col)"},
			wantAbsent:   []string{"idx_foo"},
		},
		{
			// PG pg_dump emits CREATE VIEW with an outer column-list that
			// renames the SELECT's output columns; immudb's grammar only
			// accepts `CREATE VIEW name AS dqlstmt`. Strip the list and
			// rely on AS aliases in the SELECT (which pg_dump always
			// emits alongside).
			name: "CREATE VIEW column-list stripped (quoted)",
			in: `CREATE VIEW "v"("a", "b")
AS
SELECT "t"."x" AS "a", "t"."y" AS "b" FROM "t"`,
			wantContains: []string{"CREATE VIEW v AS", "SELECT"},
			wantAbsent:   []string{`("a", "b")`, `("a",`},
		},
		{
			name:         "CREATE VIEW IF NOT EXISTS column-list stripped (bare)",
			in:           `CREATE VIEW IF NOT EXISTS v(a, b) AS SELECT 1 AS a, 2 AS b`,
			wantContains: []string{"CREATE VIEW IF NOT EXISTS v AS"},
			wantAbsent:   []string{"v(a, b)", "(a, b)"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := removePGCatalogReferences(c.in)
			for _, s := range c.wantContains {
				require.Containsf(t, got, s, "input %q → %q; expected substring %q", c.in, got, s)
			}
			for _, s := range c.wantAbsent {
				require.Falsef(t, strings.Contains(got, s),
					"input %q → %q; unwanted substring %q still present", c.in, got, s)
			}
		})
	}
}
