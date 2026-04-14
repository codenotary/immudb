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

package server

import (
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
)

const (
	helpPrefix      = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname"
	tableHelpPrefix = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like '"

	maxRowsPerMessage = 1024
)

func (s *session) QueryMachine() error {
	var waitForSync = false

	_, err := s.writeMessage(bm.ReadyForQuery())
	if err != nil {
		return err
	}

	for {
		msg, extQueryMode, err := s.nextMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.log.Warningf("connection is closed")
				return nil
			}
			s.HandleError(err)
			continue
		}

		// When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
		// then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal
		// message processing. (But note that no skipping occurs if an error is detected while processing Sync — this
		// ensures that there is one and only one ReadyForQuery sent for each Sync.)
		if waitForSync && extQueryMode {
			if _, ok := msg.(fm.SyncMsg); !ok {
				continue
			}
		}

		switch v := msg.(type) {
		case fm.TerminateMsg:
			return s.mr.CloseConnection()
		case fm.QueryMsg:
			statements := v.GetStatements()

			if statements == helpPrefix {
				statements = "show tables"
			}

			if strings.HasPrefix(statements, tableHelpPrefix) {
				tableName := strings.Split(strings.TrimPrefix(statements, tableHelpPrefix), "'")[0]
				statements = fmt.Sprintf("select column_name as tq, column_name as tow, column_name as tn, column_name as COLUMN_NAME, type_name as DATA_TYPE, type_name as TYPE_NAME, type_name as p, type_name as l, type_name as s, type_name as r, is_nullable as NULLABLE, column_name as rk, column_name as cd, type_name as SQL_DATA_TYPE, type_name as sts, column_name as coll, type_name as orp, is_nullable as IS_NULLABLE, type_name as dz, type_name as ft, type_name as iau, type_name as pn, column_name as toi, column_name as btd, column_name as tmo, column_name as tin from table(%s)", tableName)
			}

			// Handle COPY ... FROM stdin
			if strings.Contains(strings.ToUpper(statements), "COPY") {
				s.log.Infof("pgcompat: found COPY in statement (first 200): %.200q", statements)
			}
			if table, cols, ok := parseCopyStatement(statements); ok {
				s.log.Infof("pgcompat: COPY detected for table=%s cols=%d", table, len(cols))
				if err := s.handleCopyFromStdin(table, cols); err != nil {
					s.HandleError(err)
				}
				if _, err = s.writeMessage(bm.ReadyForQuery()); err != nil {
					waitForSync = extQueryMode
				}
				continue
			}

			err := s.fetchAndWriteResults(statements, nil, nil, extQueryMode)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
			}

			if _, err = s.writeMessage(bm.ReadyForQuery()); err != nil {
				waitForSync = extQueryMode
			}
		case fm.ParseMsg:
			_, ok := s.statements[v.DestPreparedStatementName]
			// unnamed prepared statement overrides previous
			if ok && v.DestPreparedStatementName != "" {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("statement '%s' already present", v.DestPreparedStatementName))
				continue
			}

			var paramCols []sql.ColDescriptor
			var resCols []sql.ColDescriptor
			var stmt sql.SQLStmt

			// Apply the same pg → immudb SQL translations (type aliases,
			// reserved-word renames, etc.) to the Extended Query Parse
			// message contents that the simple-query path runs. Without
			// this step, a CREATE TABLE sent via Parse/Bind/Execute
			// creates a column named "_key" (because quoted "key" gets
			// renamed only in the simple path), and the subsequent
			// INSERT or SELECT under Extended Query looks up the
			// un-renamed "key" and fails with "column does not exist".
			v.Statements = removePGCatalogReferences(v.Statements)

			emulableCmd := s.isEmulableInternally(v.Statements)
			if s.isInBlackList(v.Statements) {
				// blacklisted — skip parsing
			} else if emulableCmd != nil {
				// Emulable (pg_catalog etc) — precompute result columns
				if probe, ok := emulableCmd.(*pgAdminProbe); ok {
					resCols = extractResultCols(probe.sql)
				}
			} else if true {
				stmts, err := sql.ParseSQL(strings.NewReader(v.Statements))
				if err != nil {
					waitForSync = extQueryMode
					s.HandleError(err)
					continue
				}

				// Note: as stated in the pgsql spec, the query string contained in a Parse message cannot include more than one SQL statement;
				// else a syntax error is reported. This restriction does not exist in the simple-query protocol, but it does exist
				// in the extended protocol, because allowing prepared statements or portals to contain multiple commands would
				// complicate the protocol unduly.
				if len(stmts) > 1 {
					waitForSync = extQueryMode
					s.HandleError(pserr.ErrMaxStmtNumberExceeded)
					continue
				}
				if paramCols, resCols, err = s.inferParamAndResultCols(stmts[0]); err != nil {
					waitForSync = extQueryMode
					s.HandleError(err)
					continue
				}
			}

			_, err = s.writeMessage(bm.ParseComplete())
			if err != nil {
				waitForSync = extQueryMode
				continue
			}

			newStatement := &statement{
				// if no name is provided empty string marks the unnamed prepared statement
				Name:         v.DestPreparedStatementName,
				Params:       paramCols,
				SQLStatement: v.Statements,
				PreparedStmt: stmt,
				Results:      resCols,
			}

			s.statements[v.DestPreparedStatementName] = newStatement

		case fm.DescribeMsg:
			// The Describe message (statement variant) specifies the name of an existing prepared statement
			// (or an empty string for the unnamed prepared statement). The response is a ParameterDescription
			// message describing the parameters needed by the statement, followed by a RowDescription message
			// describing the rows that will be returned when the statement is eventually executed (or a NoData
			// message if the statement will not return rows). ErrorResponse is issued if there is no such prepared
			// statement. Note that since Bind has not yet been issued, the formats to be used for returned columns
			// are not yet known to the backend; the format code fields in the RowDescription message will be zeroes
			// in this case.
			if v.DescType == "S" {
				st, ok := s.statements[v.Name]
				if !ok {
					waitForSync = extQueryMode
					s.HandleError(fmt.Errorf("statement '%s' not found", v.Name))
					continue
				}

				if _, err = s.writeMessage(bm.ParameterDescription(st.Params)); err != nil {
					waitForSync = extQueryMode
					continue
				}

				if s.isEmulableInternally(st.SQLStatement) != nil && st.Results != nil {
					// Use plain column names for emulable queries
					if _, err := s.writeMessage(buildMultiColRowDescription(st.Results)); err != nil {
						waitForSync = extQueryMode
						continue
					}
				} else {
					if _, err := s.writeMessage(bm.RowDescription(st.Results, nil)); err != nil {
						waitForSync = extQueryMode
						continue
					}
				}
			}
			// The Describe message (portal variant) specifies the name of an existing portal (or an empty string
			// for the unnamed portal). The response is a RowDescription message describing the rows that will be
			// returned by executing the portal; or a NoData message if the portal does not contain a query that
			// will return rows; or ErrorResponse if there is no such portal.
			if v.DescType == "P" {
				portal, ok := s.portals[v.Name]
				if !ok {
					waitForSync = extQueryMode
					s.HandleError(fmt.Errorf("portal '%s' not found", v.Name))
					continue
				}

				if s.isEmulableInternally(portal.Statement.SQLStatement) != nil && portal.Statement.Results != nil {
					if _, err = s.writeMessage(buildMultiColRowDescription(portal.Statement.Results)); err != nil {
						waitForSync = extQueryMode
						continue
					}
				} else {
					if _, err = s.writeMessage(bm.RowDescription(portal.Statement.Results, portal.ResultColumnFormatCodes)); err != nil {
						waitForSync = extQueryMode
						continue
					}
				}
			}
		case fm.SyncMsg:
			waitForSync = false
			s.writeMessage(bm.ReadyForQuery())
		case fm.BindMsg:
			_, ok := s.portals[v.DestPortalName]
			// unnamed portal overrides previous
			if ok && v.DestPortalName != "" {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("portal '%s' already present", v.DestPortalName))
				continue
			}

			st, ok := s.statements[v.PreparedStatementName]
			if !ok {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("statement '%s' not found", v.PreparedStatementName))
				continue
			}

			encodedParams, err := buildNamedParams(st.Params, v.ParamVals)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
				continue
			}

			if _, err = s.writeMessage(bm.BindComplete()); err != nil {
				waitForSync = extQueryMode
				continue
			}

			newPortal := &portal{
				Name:                    v.DestPortalName,
				Statement:               st,
				Parameters:              encodedParams,
				ResultColumnFormatCodes: v.ResultColumnFormatCodes,
			}

			s.portals[v.DestPortalName] = newPortal
		case fm.Execute:
			//query execution
			portal, ok := s.portals[v.PortalName]
			if !ok {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("portal '%s' not found", v.PortalName))
				continue
			}

			delete(s.portals, v.PortalName)

			err := s.fetchAndWriteResults(portal.Statement.SQLStatement,
				portal.Parameters,
				portal.ResultColumnFormatCodes,
				extQueryMode,
			)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
			}
		case fm.FlushMsg:
			// there is no buffer to be flushed
		default:
			waitForSync = extQueryMode
			s.HandleError(pserr.ErrUnknowMessageType)
		}
	}
}

func (s *session) fetchAndWriteResults(statements string, parameters []*schema.NamedParam, resultColumnFormatCodes []int16, extQueryMode bool) error {
	if s.isInBlackList(statements) {
		_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
		return err
	}

	if i := s.isEmulableInternally(statements); i != nil {
		s.log.Infof("pgcompat: emulating query internally (extQueryMode=%v)", extQueryMode)
		if probe, ok := i.(*pgAdminProbe); ok && extQueryMode {
			// Extended protocol: Describe already sent RowDescription, only send DataRow
			if err := s.handlePgSystemQueryDataOnly(probe.sql); err != nil {
				return err
			}
			_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
			return err
		}
		if err := s.tryToHandleInternally(i); err != nil && err != pserr.ErrMessageCannotBeHandledInternally {
			return err
		}

		_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
		return err
	}

	s.log.Infof("pgcompat: executing query via SQL engine: %.100s", statements)
	var err error
	cacheKey := removePGCatalogReferences(statements)
	var stmts []sql.SQLStmt
	if s.stmtCache != nil {
		stmts = s.stmtCache[cacheKey]
	}
	if stmts == nil {
		stmts, err = sql.ParseSQL(strings.NewReader(cacheKey))
		if err != nil {
			return err
		}
		if s.stmtCache != nil {
			// Evict the oldest entry when the cache is full (FIFO).
			if len(s.stmtCache) >= stmtCacheSize {
				oldest := s.stmtCacheKeys[0]
				s.stmtCacheKeys = s.stmtCacheKeys[1:]
				delete(s.stmtCache, oldest)
			}
			s.stmtCache[cacheKey] = stmts
			s.stmtCacheKeys = append(s.stmtCacheKeys, cacheKey)
		}
	}

	for _, stmt := range stmts {
		switch st := stmt.(type) {
		case *sql.UseDatabaseStmt:
			if err := s.useDatabase(st.DB); err != nil {
				return err
			}
			continue
		case sql.DataSource:
			if err = s.query(st, parameters, resultColumnFormatCodes, extQueryMode); err != nil {
				return err
			}
		default:
			if err = s.exec(st, parameters, resultColumnFormatCodes, extQueryMode); err != nil {
				return err
			}
		}
	}

	_, err = s.writeMessage(bm.CommandComplete([]byte("ok")))
	if err != nil {
		return err
	}

	return nil
}

var pgTypeReplacements = []struct {
	re   *regexp.Regexp
	repl string
}{
	// Strip double quotes from identifiers, but keep them if the identifier
	// starts with a digit. SQL reserved words get prefixed with underscore.
	// Quoted identifiers: prefix digit-start names with t_, prefix reserved words with _
	{regexp.MustCompile(`"(\d\w*)"`), "t_$1"},
	{regexp.MustCompile(`"((?i:group|order|key|index|table|column|type|year|date|time|check|default|desc|asc|select|from|where|set|grant|user|role|limit|offset|values|primary|foreign|create|drop|alter|insert|update|delete|begin|commit|rollback|having|between|like|in|is|not|null|and|or|cast|case|when|then|else|end|join|on|as|distinct|all|any|exists|union|except|intersect|natural|cross|full|outer|inner|left|right|using|returning|with|recursive|password|database|transaction))"`), "_$1"},
	{regexp.MustCompile(`"(\w+)"`), "$1"},

	// Strip ::type casts FIRST — before type name translation
	// (prevents ::text from being converted to ::VARCHAR[4096])
	{regexp.MustCompile(`::[\w]+`), ""},

	// DEFAULT nextval('...'::regclass) → strip (AUTO_INCREMENT handles it)
	{regexp.MustCompile(`(?i)\s*DEFAULT\s+nextval\s*\([^)]+\)`), ""},
	// DEFAULT ('now'...) → DEFAULT NOW()
	{regexp.MustCompile(`(?i)\s*DEFAULT\s+\(\s*'now'\s*\)\s*`), " DEFAULT NOW() "},

	// Type aliases — order matters (longer matches first)
	{regexp.MustCompile(`(?i)\btimestamp\s*\(\s*\d+\s*\)\s+without\s+time\s+zone\b`), "TIMESTAMP"},
	{regexp.MustCompile(`(?i)\btimestamp\s*\(\s*\d+\s*\)\s+with\s+time\s+zone\b`), "TIMESTAMP"},
	{regexp.MustCompile(`(?i)\btimestamp\s+without\s+time\s+zone\b`), "TIMESTAMP"},
	{regexp.MustCompile(`(?i)\btimestamp\s+with\s+time\s+zone\b`), "TIMESTAMP"},
	// Rails emits `timestamp(6)` for datetime columns; immudb TIMESTAMP
	// has no fractional-second-precision knob, drop the (N) qualifier.
	{regexp.MustCompile(`(?i)\btimestamp\s*\(\s*\d+\s*\)`), "TIMESTAMP"},
	{regexp.MustCompile(`(?i)\bcharacter\s+varying\s*\(\s*(\d+)\s*\)`), "VARCHAR[$1]"},
	{regexp.MustCompile(`(?i)\bcharacter\s*\(\s*(\d+)\s*\)`), "VARCHAR[$1]"},
	// Unsized variant: Rails' `t.string` (and Rails's internal
	// schema_migrations DDL) emits bare `character varying` without a
	// size qualifier. immudb's bare VARCHAR is effectively unlimited
	// and therefore cannot be used as an index key. Cap to 256 bytes
	// so primary-key / unique-index creation succeeds for well-formed
	// Rails DDL. Callers that need a larger text column can use TEXT
	// which maps to VARCHAR[4096] elsewhere in this translator.
	{regexp.MustCompile(`(?i)\bcharacter\s+varying\b`), "VARCHAR[256]"},
	{regexp.MustCompile(`(?i)\bvarchar\b(?:\s*\()`), "VARCHAR_PAREN_FIXME("},
	{regexp.MustCompile(`(?i)\bVARCHAR_PAREN_FIXME\(\s*(\d+)\s*\)`), "VARCHAR[$1]"},
	{regexp.MustCompile(`(?i)\bdouble\s+precision\b`), "FLOAT"},
	{regexp.MustCompile(`(?i)\bbigserial\b`), "INTEGER AUTO_INCREMENT"},
	{regexp.MustCompile(`(?i)\bserial\b`), "INTEGER AUTO_INCREMENT"},
	{regexp.MustCompile(`(?i)\bsmallint\b`), "INTEGER"},
	{regexp.MustCompile(`(?i)\bbigint\b`), "INTEGER"},
	{regexp.MustCompile(`(?i)\breal\b`), "FLOAT"},
	{regexp.MustCompile(`(?i)\bnumeric\s*\([^)]*\)`), "FLOAT"},
	{regexp.MustCompile(`(?i)\bnumeric\b`), "FLOAT"},
	{regexp.MustCompile(`(?i)\bdecimal\s*\([^)]*\)`), "FLOAT"},
	// PG array types → just use VARCHAR (must come BEFORE text→VARCHAR[4096])
	// Handles: "text[]", "jsonb[]", "uuid[]", etc.
	{regexp.MustCompile(`(?i)\w+\[\]`), "VARCHAR[4096]"},
	// Array-of-sized-VARCHAR, e.g. "character varying(255)[]" already got
	// rewritten by an earlier rule to "VARCHAR[255][]"; collapse to one
	// VARCHAR[4096] so the trailing [] doesn't confuse immudb's grammar.
	{regexp.MustCompile(`(?i)VARCHAR\[\d+\]\s*\[\s*\]`), "VARCHAR[4096]"},
	{regexp.MustCompile(`(?i)\btext\b`), "VARCHAR[4096]"},
	{regexp.MustCompile(`(?i)\bbytea\b`), "BLOB"},
	{regexp.MustCompile(`(?i)\btsvector\b`), "VARCHAR[4096]"},
	{regexp.MustCompile(`(?i)\bmpaa_rating\b`), "VARCHAR[10]"},
	// PG custom domain types from dvdrental
	{regexp.MustCompile(`(?i)\byear\b`), "INTEGER"},

	// (DEFAULT nextval and ::casts already handled above)

	// Rename bare reserved words used as column names (detected by following type keyword)
	{regexp.MustCompile(`(?i)\bpassword\s+(VARCHAR|INTEGER|BOOLEAN|TIMESTAMP|BLOB|FLOAT)`), "_password $1"},
	{regexp.MustCompile(`(?i)\bdatabase\s+(VARCHAR|INTEGER|BOOLEAN|TIMESTAMP|BLOB|FLOAT)`), "_database $1"},
	{regexp.MustCompile(`(?i)\btransaction\s+(VARCHAR|INTEGER|BOOLEAN|TIMESTAMP|BLOB|FLOAT)`), "_transaction $1"},

	// immudb doesn't support DEFAULT expr NOT NULL together — strip NOT NULL after DEFAULT
	{regexp.MustCompile(`(?i)(DEFAULT\s+\S+(?:\([^)]*\))?)\s+NOT\s+NULL`), "$1"},

	// Rails emits `SELECT "table_name".* FROM "table_name" WHERE ...` as
	// its standard all-columns projection on ActiveRecord models. immudb
	// grammar has no `identifier.*` production (only bare `*`), so strip
	// the table prefix. Works for single-table queries (~99 % of AR);
	// JOIN queries with multiple `t1.*, t2.*` become `*, *` which is
	// technically different but returns the same union of columns.
	{regexp.MustCompile(`(?i)\b[A-Za-z_][A-Za-z0-9_]*\s*\.\s*\*`), "*"},

	// Rails emits Postgres-style `ON CONFLICT (col_list) DO ...` for
	// `create_or_find_by!`, `upsert_all`, etc. immudb's grammar accepts
	// only the column-less form (`ON CONFLICT DO NOTHING` / `ON CONFLICT
	// DO UPDATE SET`), so drop the column list. The resulting statement
	// has the same intent: skip / update on any conflict.
	{regexp.MustCompile(`(?i)\bON\s+CONFLICT\s*\([^)]*\)\s*(DO)\b`), "ON CONFLICT $1"},

	// Rails decides whether a table already exists by probing pg_class;
	// with immudb's canned emulation that probe always reports "absent",
	// so Rails emits bare `CREATE TABLE` rather than the idempotent
	// `CREATE TABLE IF NOT EXISTS`. On the first run the plain form
	// succeeds. On a restart after a successful schema load the plain
	// form fails with "table already exists" and aborts db:prepare.
	// Make every `CREATE TABLE` implicitly idempotent by inserting the
	// `IF NOT EXISTS` clause when it is missing — Postgres and immudb
	// both accept the form, and Rails's control flow is unchanged.
	{regexp.MustCompile(`(?i)^(\s*CREATE\s+TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>"[^"]+"|\w+)`), "$1 IF NOT EXISTS $2"},

	// (INSERT-into-schema_migrations idempotency is applied later in a
	//  Go-side pass because regex alone cannot express "only add
	//  ON CONFLICT if one is not already present".)

	// Strip CHECK constraints (may be nested parens)
	{regexp.MustCompile(`(?i)\bCHECK\s*\([^)]*\)`), ""},

	// Strip PostgreSQL stored generated (virtual) column clauses.
	// Rails' `t.virtual ..., stored: true` emits
	//   "col" type GENERATED ALWAYS AS (expression) STORED
	// immudb has no generated-column support, so drop the clause and
	// leave the column as a regular nullable column. The expression
	// may contain arbitrary nested parens; we rely on STORED as the
	// (non-greedy) terminator. This works provided the expression
	// body does not itself contain the literal identifier "STORED",
	// which is safe in practice for ActiveRecord-emitted DDL.
	{regexp.MustCompile(`(?is)\s+GENERATED\s+ALWAYS\s+AS\s+\(.*?\)\s+STORED\b`), ""},

	// Strip CONSTRAINT keyword with name
	{regexp.MustCompile(`(?i)\bCONSTRAINT\s+\w+\s+`), ""},

	// Strip REFERENCES (foreign keys) with optional ON DELETE/UPDATE
	{regexp.MustCompile(`(?i)\bREFERENCES\s+\S+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION))*`), ""},

	// Strip UNIQUE keyword on columns (immudb handles unique via index)
	{regexp.MustCompile(`(?i)\bUNIQUE\b`), ""},

	// date type — must come after timestamp replacements
	// Only match standalone 'date' as a type (after a column name, not in other contexts)
	{regexp.MustCompile(`(?i)\bdate\b`), "TIMESTAMP"},
}

var createTableRe = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+`)
var primaryKeyInlineRe = regexp.MustCompile(`(?i)PRIMARY\s+KEY`)

var insertSchemaMigrationsRe = regexp.MustCompile(`(?is)\A(\s*INSERT\s+INTO\s+(?:"schema_migrations"|schema_migrations)\s+\([^)]*\)\s+VALUES\s+.+?)(\s*;?\s*)\z`)

func removePGCatalogReferences(sqlStr string) string {
	s := strings.ReplaceAll(sqlStr, "pg_catalog.", "")
	s = strings.ReplaceAll(s, "information_schema.", "information_schema_")
	s = strings.ReplaceAll(s, "public.", "")

	// Apply PG type translations
	for _, r := range pgTypeReplacements {
		s = r.re.ReplaceAllString(s, r.repl)
	}

	// Idempotency for Rails's schema_migrations bulk insert. See
	// pgTypeReplacements for the long explanation. Using a Go pass here
	// so we can check "ON CONFLICT already present" safely — the
	// standard library regex has no lookahead.
	if m := insertSchemaMigrationsRe.FindStringSubmatchIndex(s); m != nil {
		if !strings.Contains(strings.ToUpper(s), "ON CONFLICT") {
			head := s[m[2]:m[3]]
			tail := s[m[4]:m[5]]
			s = head + " ON CONFLICT DO NOTHING" + tail
		}
	}

	// Auto-add PRIMARY KEY for CREATE TABLE without one
	if createTableRe.MatchString(s) && !primaryKeyInlineRe.MatchString(s) {
		s = addPrimaryKeyToCreateTable(s)
	}

	// Clean up double spaces and empty lines
	s = regexp.MustCompile(`  +`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?m)^\s*,\s*,`).ReplaceAllString(s, ",")
	// Remove trailing commas before closing paren
	s = regexp.MustCompile(`,\s*\)`).ReplaceAllString(s, "\n)")

	return s
}

// addPrimaryKeyToCreateTable adds a PRIMARY KEY clause to a CREATE TABLE
// that doesn't have one. Picks the first NOT NULL column, or an id/ID column,
// or the first column.
func addPrimaryKeyToCreateTable(sql string) string {
	// Find the closing paren of the CREATE TABLE
	lastParen := strings.LastIndex(sql, ")")
	if lastParen < 0 {
		return sql
	}

	// Extract column definitions between first ( and last )
	firstParen := strings.Index(sql, "(")
	if firstParen < 0 || firstParen >= lastParen {
		return sql
	}

	colSection := sql[firstParen+1 : lastParen]
	lines := strings.Split(colSection, ",")

	var firstCol, notNullCol, idCol string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 2 {
			continue
		}
		colName := words[0]
		// Skip if it looks like a constraint, not a column
		upper := strings.ToUpper(colName)
		if upper == "CONSTRAINT" || upper == "PRIMARY" || upper == "FOREIGN" || upper == "CHECK" || upper == "UNIQUE" {
			continue
		}

		if firstCol == "" {
			firstCol = colName
		}

		if strings.Contains(strings.ToUpper(line), "NOT NULL") && notNullCol == "" {
			notNullCol = colName
		}

		lowerCol := strings.ToLower(colName)
		if (lowerCol == "id" || strings.HasSuffix(lowerCol, "_id") || strings.HasSuffix(lowerCol, "id")) && idCol == "" {
			idCol = colName
		}
	}

	// Pick the best PK column
	pkCol := notNullCol
	if pkCol == "" {
		pkCol = idCol
	}
	if pkCol == "" {
		pkCol = firstCol
	}
	if pkCol == "" {
		return sql
	}

	// Insert PRIMARY KEY before the closing paren
	before := sql[:lastParen]
	after := sql[lastParen:]

	// Remove trailing comma/whitespace from before
	before = strings.TrimRight(before, " \t\n,")

	return before + ",\n    PRIMARY KEY (" + pkCol + ")\n" + after
}

func (s *session) query(st sql.DataSource, parameters []*schema.NamedParam, resultColumnFormatCodes []int16, skipRowDesc bool) error {
	tx, err := s.sqlTx()
	if err != nil {
		return err
	}

	reader, err := s.db.SQLQueryPrepared(s.ctx, tx, st, schema.NamedParamsFromProto(parameters))
	if err != nil {
		return err
	}
	defer reader.Close()

	cols, err := reader.Columns(s.ctx)
	if err != nil {
		return err
	}

	if !skipRowDesc {
		if _, err = s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
			return err
		}
	}

	return sql.ReadRowsBatch(s.ctx, reader, maxRowsPerMessage, func(rowBatch []*sql.Row) error {
		_, err := s.writeMessage(bm.DataRow(rowBatch, len(cols), resultColumnFormatCodes))
		return err
	})
}

func (s *session) exec(st sql.SQLStmt, namedParams []*schema.NamedParam, resultColumnFormatCodes []int16, skipRowDesc bool) error {
	params := make(map[string]interface{}, len(namedParams))

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	tx, err := s.sqlTx()
	if err != nil {
		return err
	}

	ntx, _, err := s.db.SQLExecPrepared(s.ctx, tx, []sql.SQLStmt{st}, params)
	s.tx = ntx

	return err
}

type portal struct {
	Name                    string
	Statement               *statement
	Parameters              []*schema.NamedParam
	ResultColumnFormatCodes []int16
}

type statement struct {
	Name         string
	SQLStatement string
	PreparedStmt sql.SQLStmt
	Params       []sql.ColDescriptor
	Results      []sql.ColDescriptor
}

func (s *session) inferParamAndResultCols(stmt sql.SQLStmt) ([]sql.ColDescriptor, []sql.ColDescriptor, error) {
	var resCols []sql.ColDescriptor

	ds, ok := stmt.(sql.DataSource)
	if ok {
		rr, err := s.db.SQLQueryPrepared(s.ctx, s.tx, ds, nil)
		if err != nil {
			return nil, nil, err
		}

		resCols, err = rr.Columns(s.ctx)
		if err != nil {
			return nil, nil, err
		}

		rr.Close()
	}

	r, err := s.db.InferParametersPrepared(s.ctx, s.tx, stmt)
	if err != nil {
		return nil, nil, err
	}

	if len(r) > math.MaxInt16 {
		return nil, nil, pserr.ErrMaxParamsNumberExceeded
	}

	var paramsNameList []string
	for n := range r {
		paramsNameList = append(paramsNameList, n)
	}
	sort.Strings(paramsNameList)

	paramCols := make([]sql.ColDescriptor, 0)
	for _, n := range paramsNameList {
		paramCols = append(paramCols, sql.ColDescriptor{Column: n, Type: r[n]})
	}
	return paramCols, resCols, nil
}
