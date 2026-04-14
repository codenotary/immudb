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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
)

func TestSession_QueriesMachine(t *testing.T) {
	var tests = []struct {
		name       string
		in         func(conn net.Conn)
		out        error
		portals    map[string]*portal
		statements map[string]*statement
	}{
		{
			name: "unsupported message",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//unsupported message
				c2.Write([]byte("_"))
				unsupported := make([]byte, 500)
				c2.Read(unsupported)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "fail first ready for query message",
			in: func(c2 net.Conn) {
				c2.Close()
			},
			out: errors.New("io: read/write on closed pipe"),
		},
		{
			name: "connection is closed",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "wait for sync",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("wrong_st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Write(h.Msg('S', []byte{0}))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))
			},
			out: nil,
		},
		{
			name: "error on parse-infer parameters",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("wrong statement"), h.I16(1), h.I32(0)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))

			},
			out: nil,
		},
		{
			name: "statement already present",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))

			},
			out: nil,
		},
		{
			name: "error on parse complete",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S statement not found",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("wrong st")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S ParameterDescription error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("st")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S RowDescription error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("st")})))
				rowDescription := make([]byte, len(bmessages.ParameterDescription(nil)))
				c2.Read(rowDescription)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe P portal not found",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'P'}, h.S("port")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe P row desc error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S(";"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('S', []byte{0}))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'P'}, h.S("port")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "sync error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('S', []byte{0}))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query results error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("_wrong_")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("set test")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query command ready for query error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("set test")))
				cc := make([]byte, len(bmessages.CommandComplete([]byte(`ok`))))
				c2.Read(cc)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "bind portal already present error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete = make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "bind named param error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Close()
			},
			out: nil,
			statements: map[string]*statement{
				"st": {
					Name:         "st",
					SQLStatement: "test",
					PreparedStmt: nil,
					Params: []sql.ColDescriptor{{
						Column: "test",
						Type:   "INTEGER",
					}},
					Results: nil,
				},
			},
		},
		{
			name: "bind complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "execute write result error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
			portals: map[string]*portal{
				"port": {
					Statement: &statement{
						SQLStatement: "test",
					},
					Parameters: []*schema.NamedParam{
						{
							Name: "test",
						},
					},
					ResultColumnFormatCodes: []int16{1},
				},
			},
		},
		{
			name: "execute command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "execute command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "version info error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("select version()")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "flush",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('H', nil))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "schema info",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("select current_schema()")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "table help",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery(bmessages.TxStatusIdle)))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S(tableHelpPrefix)))
				c2.Close()
			},
			out: nil,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("qm scenario %d: %s", i, tt.name), func(t *testing.T) {

			c1, c2 := net.Pipe()

			mr := &messageReader{
				conn: c1,
			}

			s := session{
				log:        logger.NewSimpleLogger("test", os.Stdout),
				mr:         mr,
				statements: make(map[string]*statement),
				portals:    make(map[string]*portal),
				db:         &mockDB{},
			}

			if tt.statements != nil {
				s.statements = tt.statements
			}
			if tt.portals != nil {
				s.portals = tt.portals
			}
			go tt.in(c2)

			err := s.QueryMachine()

			require.Equal(t, tt.out, err)
		})
	}
}

type mockDB struct {
	database.DB
}

func (db *mockDB) SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error) {
	return nil, fmt.Errorf("dummy error")
}

// TestRemovePGCatalogReferencesPreservesStringLiterals guards the masking
// introduced for B.1: the quote-stripping rewrites in pgTypeReplacements
// are not SQL-aware and used to mangle JSON / identifier-looking bytes
// inside single-quoted literals, breaking simple-query INSERTs of JSON.
func TestRemovePGCatalogReferencesPreservesStringLiterals(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // substring that MUST survive in the output
	}{
		{
			name: "json object in literal",
			in:   `INSERT INTO t (id, d) VALUES ('a', '{"k":1}')`,
			want: `'{"k":1}'`,
		},
		{
			name: "nested json in literal",
			in:   `INSERT INTO t (id, d) VALUES ('b', '{"users":[{"name":"bob"}]}')`,
			want: `'{"users":[{"name":"bob"}]}'`,
		},
		{
			name: "json string primitive in literal",
			in:   `INSERT INTO t (id, d) VALUES ('c', '"plain"')`,
			want: `'"plain"'`,
		},
		{
			name: "keyword-looking word quoted inside literal",
			in:   `UPDATE t SET note = 'say "select" out loud' WHERE id = 1`,
			want: `'say "select" out loud'`,
		},
		{
			name: "escaped single quote inside literal",
			in:   `UPDATE t SET s = 'a''b"c"d' WHERE id = 1`,
			want: `'a''b"c"d'`,
		},
		{
			name: "quoted plain identifier OUTSIDE a literal still unwrapped",
			in:   `SELECT "col" FROM t`,
			want: `SELECT col FROM t`,
		},
		{
			name: "quoted reserved identifier OUTSIDE a literal still escaped",
			in:   `SELECT "select" FROM t`,
			want: `SELECT _select FROM t`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := removePGCatalogReferences(tc.in)
			require.Contains(t, got, tc.want, "normalised SQL: %s", got)
		})
	}
}

// TestStripQuotedSchemaPrefix pins the quoted-schema-qualifier strip:
// XORM / Hibernate / JDBC DDL frequently uses `"public"."table"` and
// the bare immudb grammar has no schema.table production, so leaving
// the dot in tripped the parser with `unexpected DOT, expecting '('`.
func TestStripQuotedSchemaPrefix(t *testing.T) {
	cases := map[string]string{
		`CREATE TABLE IF NOT EXISTS "public"."version" ("id" BIGINT)`:        `CREATE TABLE IF NOT EXISTS "version" ("id" BIGINT)`,
		`SELECT * FROM "public"."users" WHERE id = 1`:                        `SELECT * FROM "users" WHERE id = 1`,
		`DROP TABLE "public"."t"`:                                            `DROP TABLE "t"`,
		`SELECT 1 FROM "pg_catalog"."pg_tables"`:                             `SELECT 1 FROM "pg_tables"`,
		`SELECT * FROM "information_schema"."columns"`:                       `SELECT * FROM "columns"`,
		// Whitespace between the prefix and the dot is consumed; the
		// double-space cleanup happens later in removePGCatalogReferences.
		`SELECT "public" . "users" FROM x`: `SELECT  "users" FROM x`,
		// Must NOT touch quoted lists where the second token isn't a
		// schema-qualified identifier (no `.` follows the closing quote).
		`SELECT "public", "users" FROM x`:                                    `SELECT "public", "users" FROM x`,
	}
	for in, want := range cases {
		got := stripQuotedSchemaPrefix(in)
		if got != want {
			t.Errorf("stripQuotedSchemaPrefix(%q)\n   got  %q\n   want %q", in, got, want)
		}
	}
}

// TestMaskStringLiteralsRoundTrip covers the masker itself, which carries
// the load-bearing SQL-escape logic for the B.1 fix.
func TestMaskStringLiteralsRoundTrip(t *testing.T) {
	inputs := []string{
		``,
		`SELECT 1`,
		`SELECT 'hello'`,
		`SELECT 'a''b'`,                       // escaped quote
		`INSERT VALUES ('x', '{"k":1}', 'y')`, // multiple literals
		`SELECT '' FROM t`,                    // empty literal
		`SELECT 'a', "b", 'c'`,                // mixed with identifier
	}
	for _, in := range inputs {
		masked, restore := maskStringLiterals(in)
		require.NotContains(t, masked, "'", "masked still has a quote: %q", masked)
		require.Equal(t, in, restore(masked), "roundtrip for %q", in)
	}

	// Unterminated literal: emit the tail verbatim. We don't promise no
	// quotes in the masked form here — only that nothing panics.
	masked, restore := maskStringLiterals(`UPDATE t SET s = 'unterminated`)
	require.Equal(t, `UPDATE t SET s = 'unterminated`, restore(masked))
}
