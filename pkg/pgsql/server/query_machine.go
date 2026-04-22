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
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
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

	_, err := s.writeMessage(bm.ReadyForQuery(s.txStatus))
	if err != nil {
		return err
	}

	for {
		msg, extQueryMode, err := s.nextMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Normal client disconnect — pq driver pools rotate
				// connections aggressively. Drop to Debug so production
				// logs aren't drowned in this benign event.
				s.log.Debugf("connection is closed")
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
				if _, err = s.writeMessage(bm.ReadyForQuery(s.txStatus)); err != nil {
					waitForSync = extQueryMode
				}
				continue
			}

			err := s.fetchAndWriteResults(statements, nil, nil, extQueryMode)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
			}

			if _, err = s.writeMessage(bm.ReadyForQuery(s.txStatus)); err != nil {
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
				if probe, ok := emulableCmd.(*xormColumnsCmd); ok {
					resCols = extractResultCols(probe.sql)
				}
				if _, ok := emulableCmd.(*pgAttributeForTableCmd); ok {
					resCols = pgAttributeResultCols()
				}
				// Emulated queries don't go through inferParametersPrepared,
				// so paramCols would default to empty. ParameterDescription
				// would then advertise 0 params, and the client's Bind with
				// N>0 values triggers
				//   "got N parameters but the statement requires 0".
				// Scan the SQL text for the highest $N marker and emit
				// AnyType placeholders so the count matches what the client
				// will Bind.
				paramCols = inferParamColsFromSQL(v.Statements)
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
			s.writeMessage(bm.ReadyForQuery(s.txStatus))
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
	tag := commandTagFor(statements)
	// Track explicit transaction state so the next ReadyForQuery message
	// reports the correct transaction-status byte. Clients (pq, JDBC)
	// gate commit/rollback handling on this byte; staying at 'I' after a
	// BEGIN gives the pq driver "unexpected transaction status idle".
	switch tag {
	case "BEGIN":
		s.txStatus = bm.TxStatusInTx
	case "COMMIT", "ROLLBACK":
		s.txStatus = bm.TxStatusIdle
	}
	if s.isInBlackList(statements) {
		_, err := s.writeMessage(bm.CommandComplete([]byte(tag)))
		return err
	}

	if i := s.isEmulableInternally(statements); i != nil {
		s.log.Infof("pgcompat: emulating query internally (extQueryMode=%v) sql=%.300s", extQueryMode, statements)
		if probe, ok := i.(*pgAdminProbe); ok && extQueryMode {
			// Extended protocol: Describe already sent RowDescription, only send DataRow
			if err := s.handlePgSystemQueryDataOnly(probe.sql); err != nil {
				return err
			}
			_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
			return err
		}
		if cmd, ok := i.(*pgAttributeForTableCmd); ok {
			// Same pattern as pgAdminProbe above: in Extended Query mode
			// Describe has already sent the RowDescription, so pass the
			// flag through to skip the second one.
			tableName := cmd.tableName
			if tableName == "" && len(parameters) > 0 {
				// Parameterised form — the table name is bound to $1
				// (value carries the Postgres regclass literal like
				// `"users"` — strip the double quotes).
				if raw, ok := schema.RawValue(parameters[0].Value).(string); ok {
					tableName = strings.Trim(raw, `"`)
				}
			}
			if err := s.handlePgAttributeForTable(tableName, extQueryMode); err != nil {
				return err
			}
			_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
			return err
		}
		if cmd, ok := i.(*xormColumnsCmd); ok {
			// XORM column-introspection emulation. Bind values carry the
			// table name (`c.relname = $1`) and schema (`s.table_schema = $2`).
			if err := s.handleXormColumnsQuery(cmd.sql, parameters, extQueryMode); err != nil {
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

	s.log.Infof("pgcompat: executing query via SQL engine: %.300s", statements)
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

	if len(stmts) == 0 {
		// PostgreSQL contract: a Simple Query that is empty or contains
		// only comments returns EmptyQueryResponse, not CommandComplete.
		// k3s/kine issues `-- ping` as a liveness check and expects this.
		_, err := s.writeMessage(bm.EmptyQueryResponse())
		return err
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

	_, err = s.writeMessage(bm.CommandComplete([]byte(tag)))
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
	// Quoted-identifier keyword escape. Whenever a SQL token is wrapped
	// in double quotes AND its content matches an immudb-reserved
	// keyword, prefix the identifier with an underscore so it parses as
	// a regular IDENTIFIER instead of triggering a grammar conflict.
	// The list is kept in sync with the keyword map in
	// embedded/sql/parser.go (see scripts/dump_immudb_keywords.sh — or
	// regenerate by grepping `^\s+"[A-Z_]+":` out of parser.go and
	// lowercasing).
	{regexp.MustCompile(`"((?i:add|admin|after|all|alter|and|as|asc|auto_increment|avg|before|begin|between|bigint|bigserial|blob|boolean|by|bytea|cascade|case|cast|check|column|commit|conflict|constraint|count|create|cross|database|databases|date|day|decimal|default|delete|desc|diff|distinct|do|double|drop|else|end|except|exists|explain|extract|false|fetch|first|float|for|foreign|from|full|grant|grants|group|having|history|hour|if|ilike|in|index|inner|insert|int|integer|intersect|into|is|join|json|jsonb|key|last|lateral|left|like|limit|max|min|minute|month|natural|not|nothing|null|nulls|numeric|of|offset|on|only|or|order|over|partition|password|primary|privileges|read|readwrite|real|recursive|references|release|rename|returning|revoke|right|rollback|rows|savepoint|second|select|sequence|serial|set|show|since|smallint|snapshot|sum|table|tables|then|timestamp|timestamptz|to|transaction|true|truncate|tx|union|unique|until|update|upsert|use|user|users|using|uuid|values|varchar|view|when|where|with|year))"`), "_$1"},
	{regexp.MustCompile(`"(\w+)"`), "$1"},

	// Strip PG-only ::TYPE casts before the type-name translation below.
	// Types natively recognised by immudb's SQL parser via the SCAST
	// production (boundexp :: sql_type) — INTEGER/INT/BIGINT, BOOLEAN,
	// FLOAT/REAL/NUMERIC/DECIMAL, VARCHAR, BLOB/BYTEA, UUID, JSON/JSONB,
	// TIMESTAMP/TIMESTAMPTZ/DATE — are intentionally left intact so the
	// engine can evaluate them (e.g. '42'::INTEGER). PG-specific or
	// string types are removed: ::text would otherwise become
	// ::VARCHAR[4096] after the text rewrite below; ::regclass, ::name,
	// ::oid etc. have no immudb equivalent.
	{regexp.MustCompile(`(?i)::(?:text|character(?:\s+varying)?|name|oid|regclass|regtype|regproc|regoper|regnamespace|anyarray|anyelement|anynonarray|void|cstring|internal|opaque|unknown|pg_[a-z_]+)(?:\[\])?`), ""},

	// DEFAULT nextval('...'::regclass) → strip (AUTO_INCREMENT handles it)
	{regexp.MustCompile(`(?i)\s*DEFAULT\s+nextval\s*\([^)]+\)`), ""},

	// Strip `COLLATE <identifier>` column/type modifiers. immudb has no
	// collation support — the default byte-wise comparison is always in
	// effect. k3s/kine emits `name text COLLATE "C"` to pin Postgres's
	// index behavior; without stripping, the unquote pass would leave us
	// with `COLLATE C` which still fails to parse. Run before the
	// double-quote unquote rule so the quoted form is handled in one go.
	{regexp.MustCompile(`(?i)\s+COLLATE\s+(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)`), ""},
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
	// PG's `CHAR(N)` is fixed-length string; immudb has only VARCHAR[N].
	// Map `CHAR(N)` (and the bare `CHAR` short form Postgres also accepts)
	// to the variable-length equivalent. Storage cost differs marginally
	// but the wire-visible behaviour matches.
	{regexp.MustCompile(`(?i)\bchar\s*\(\s*(\d+)\s*\)`), "VARCHAR[$1]"},
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
	// PG array types → just use a generous VARCHAR (must come BEFORE text
	// mapping). Handles: "text[]", "jsonb[]", "uuid[]", etc.
	// Size bumped from 4096 to 1 MB so Gitea's PushCommits JSON payload
	// and other unbounded-TEXT columns (workflow dispatch inputs, action
	// logs, etc.) fit. Per-db MaxValueLen is 32 MB (pkg/server/db_options.go:135).
	{regexp.MustCompile(`(?i)\w+\[\]`), "VARCHAR[1048576]"},
	// Array-of-sized-VARCHAR, e.g. "character varying(255)[]" already got
	// rewritten by an earlier rule to "VARCHAR[255][]"; collapse so the
	// trailing [] doesn't confuse immudb's grammar.
	{regexp.MustCompile(`(?i)VARCHAR\[\d+\]\s*\[\s*\]`), "VARCHAR[1048576]"},
	// PG's unbounded TEXT → 1 MB VARCHAR. Critical for Gitea's action.content
	// (commit JSON for a push of N commits grows O(N) and exceeds 4 KB for
	// even modest pushes), issue bodies, workflow YAML blobs, etc.
	{regexp.MustCompile(`(?i)\btext\b`), "VARCHAR[1048576]"},
	{regexp.MustCompile(`(?i)\bbytea\b`), "BLOB"},
	// PG accepts both `BOOL` and `BOOLEAN`; immudb's grammar only knows
	// the long form. XORM and several other ORMs emit the short form.
	{regexp.MustCompile(`(?i)\bbool\b`), "BOOLEAN"},

	// (ALTER TABLE … ADD <coldef> → ADD COLUMN <coldef>: handled in
	// a Go post-pass below since Go RE2 has no negative lookahead and a
	// blanket regex would also rewrite ADD CONSTRAINT / FOREIGN / etc.)

	// Strip trailing `; COMMENT ON … IS '…'` clauses that XORM and
	// JDBC ORMs append to DDL. The standalone `COMMENT ON` form is
	// already in pgUnsupportedDDL but the trailing-clause form survives
	// the per-statement blacklist check (it's part of a multi-statement
	// SQL string sent in one Parse). Drop it here so the leading DDL
	// reaches the engine cleanly.
	{regexp.MustCompile(`(?is);\s*COMMENT\s+ON\s+[^;]+(?:;|$)`), ""},

	// `BEGIN READ WRITE`, `BEGIN READ ONLY`, `START TRANSACTION READ
	// WRITE` etc. — PG transaction-mode suffixes that immudb's BEGIN
	// grammar doesn't accept. Strip the mode; immudb's transactions
	// are read/write by default and the only client-visible difference
	// from a SELECT-only block is performance, which doesn't matter
	// for correctness probing.
	{regexp.MustCompile(`(?i)\b(BEGIN|START\s+TRANSACTION)\s+(READ\s+(?:WRITE|ONLY)|ISOLATION\s+LEVEL\s+\S+(?:\s+\S+)?|DEFERRABLE|NOT\s+DEFERRABLE)`), "BEGIN"},
	// Explicit `NULL` after a column type is a no-op nullability marker
	// in PG (it's the default). immudb's grammar has no such production
	// and rejects the bare keyword. Strip it; the column stays nullable
	// because nothing else marked it NOT NULL.
	{regexp.MustCompile(`(?i)((?:VARCHAR(?:\[\d+\])?|INTEGER|BOOLEAN|BLOB|FLOAT|TIMESTAMP|UUID|JSON|VARCHAR_PAREN_FIXME\([^)]*\)))\s+NULL\b`), "$1"},
	// XORM-style `… DEFAULT <value> NULL` — same issue: the trailing
	// NULL is a redundant nullability marker after a default. Strip it
	// regardless of the default token (TRUE/FALSE/0/numeric/'string'/…).
	{regexp.MustCompile(`(?i)(DEFAULT\s+(?:'[^']*'|[^\s,()]+))\s+NULL\b`), "$1"},
	{regexp.MustCompile(`(?i)\btsvector\b`), "VARCHAR[1048576]"},
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

	// PRIMARY KEY columns are already implicitly NOT NULL in immudb's
	// grammar, and the parser rejects an explicit `NOT NULL` after the
	// PRIMARY KEY keywords with
	//   "syntax error: unexpected NOT, expecting ',' or ')'".
	// XORM, Hibernate, and several JDBC ORMs emit the redundant pair
	// (`PRIMARY KEY NOT NULL`) for PK columns, so strip the trailing
	// NOT NULL when it follows PRIMARY KEY.
	{regexp.MustCompile(`(?i)(PRIMARY\s+KEY)\s+NOT\s+NULL\b`), "$1"},
	// And the symmetric form (`NOT NULL PRIMARY KEY`) is fine, but a
	// duplicated NOT NULL on either side of PRIMARY KEY isn't:
	{regexp.MustCompile(`(?i)NOT\s+NULL\s+(PRIMARY\s+KEY)\s+NOT\s+NULL\b`), "$1"},

	// Rails emits `SELECT "table_name".* FROM "table_name" WHERE ...` as
	// its standard all-columns projection on ActiveRecord models. immudb
	// grammar has no `identifier.*` production (only bare `*`), so strip
	// the table prefix. Works for single-table queries (~99 % of AR);
	// JOIN queries with multiple `t1.*, t2.*` become `*, *` which is
	// technically different but returns the same union of columns.
	{regexp.MustCompile(`(?i)\b[A-Za-z_][A-Za-z0-9_]*\s*\.\s*\*`), "*"},

	// After the `table.* -> *` reduction above, XORM's
	//   SELECT project.*, project_issue.issue_id FROM project ...
	// becomes `SELECT *, project_issue.issue_id FROM ...`, which
	// immudb's grammar rejects (the `opt_targets` production is either
	// bare `*` or a comma-separated expression list, never a mix).
	// `SELECT *` over a JOIN in immudb already yields all columns from
	// all joined tables, so the trailing comma-separated list is
	// redundant — collapse it back to a bare `*`.
	{regexp.MustCompile(`(?is)(SELECT\s+\*)(?:\s*,[^,]+?)+(\s+FROM\b)`), "$1$2"},

	// XORM / Gitea's QueryIssueContentHistoryEditedCountMap emits
	//   SELECT comment_id, COUNT(1) as history_count ...
	//   ... HAVING count(1) > 1
	// immudb's aggregate-func grammar accepts COUNT(*) and COUNT(col)
	// but not an integer-literal arg, producing "unexpected INTEGER_LIT
	// at position 26" at parse time. Postgres/MySQL treat `COUNT(1)`
	// and `COUNT(*)` identically (row count), so rewrite.
	{regexp.MustCompile(`(?i)\bCOUNT\s*\(\s*1\s*\)`), "COUNT(*)"},

	// Gitea's eventsource UIDcounts (GetUIDsAndNotificationCounts) emits
	//   SELECT user_id, sum(case when status = $1 then 1 else 0 end) AS count
	//     FROM notification WHERE <cond> GROUP BY user_id
	// immudb's grammar has no expression-based aggregate (AggExp); the
	// CASE WHEN inside SUM(…) parses as "unexpected CASE at position 24".
	// For this specific shape the 0/1 indicator is equivalent to a
	// row count filtered by the CASE predicate, so hoist the predicate
	// into the WHERE clause and swap SUM(...) for COUNT(*). Users with
	// zero matching rows drop out of the result, which is the correct
	// semantics for an unread-notification counter (the Gitea frontend
	// treats absence as zero).
	{
		regexp.MustCompile(`(?is)SELECT\s+(\w+)\s*,\s*sum\s*\(\s*case\s+when\s+(\w+)\s*=\s*\$(\d+)\s+then\s+1\s+else\s+0\s+end\s*\)\s+AS\s+(\w+)\s+FROM\s+(\w+)\s+WHERE\s+`),
		`SELECT $1, COUNT(*) AS $4 FROM $5 WHERE $2 = $$$3 AND `,
	},

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

	// Strip table-level FOREIGN KEY … REFERENCES … constraints BEFORE the
	// column-level REFERENCES strip below. Without this, the column-level
	// strip removes only the REFERENCES clause, leaving a dangling
	// "FOREIGN KEY (col)" that causes "unexpected ')', expecting REFERENCES".
	// immudb's SQL engine does accept FOREIGN KEY in CREATE TABLE (parsed
	// but not enforced), but stripping here is equivalent and avoids the
	// interaction with the REFERENCES rule.
	{regexp.MustCompile(`(?i),?\s*\bFOREIGN\s+KEY\s*\([^)]*\)\s*REFERENCES\s+\S+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION))*`), ""},
	// Strip column-level REFERENCES (inline FK) with optional ON DELETE/UPDATE
	{regexp.MustCompile(`(?i)\bREFERENCES\s+\S+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION))*`), ""},

	// Strip the optional index name from `CREATE [UNIQUE] INDEX [IF NOT
	// EXISTS] <name> ON …`. PostgreSQL requires (and pg_dump emits) a
	// name; immudb's grammar rejects one (sql_grammar.y:390-408 has
	// `CREATE INDEX … ON …` with no name). The alternation matches both
	// the double-quoted and bare-identifier forms. When the name is
	// absent (`CREATE INDEX ON …`, which immudb accepts natively) the
	// `<name> ON` suffix can't be satisfied and the pattern does not
	// match, so no-op rewrite.
	{regexp.MustCompile(`(?i)\bCREATE\s+(UNIQUE\s+)?INDEX\s+(IF\s+NOT\s+EXISTS\s+)?(?:"[^"]+"|[A-Za-z_]\w*)\s+ON\b`), "CREATE ${1}INDEX ${2}ON"},

	// Strip the optional PostgreSQL column-alias list from
	// `CREATE VIEW [IF NOT EXISTS] <name>(col1, col2, …) AS SELECT …`.
	// immudb's grammar (sql_grammar.y:355-360) only accepts
	// `CREATE VIEW <name> AS dqlstmt`. PG's outer column list renames
	// the SELECT's output columns; pg_dump emits both the list AND
	// matching `AS` aliases in the SELECT, so dropping the outer list
	// preserves names for the common case. Multi-line matching (?is)
	// because dumps put each column on its own line. If a list ever
	// contains nested parens the pattern will not match and the user
	// gets the original pre-fix "unexpected '(', expecting AS" error,
	// which is acceptable — PG lists are identifier-only in practice.
	{regexp.MustCompile(`(?is)\bCREATE\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[A-Za-z_]\w*)\s*\([^)]*\)\s+AS\b`), "CREATE VIEW ${1}${2} AS"},

	// NOTE: `UNIQUE` inline-on-column / table-level constraints are NOT
	// stripped here. They are translated into a trailing
	// `CREATE UNIQUE INDEX ON tbl(col)` statement by the Go pass in
	// extractUniqueConstraints (unique_ddl.go) which runs before this
	// regex pipeline. Stripping the keyword outright would silently drop
	// uniqueness guarantees — which is exactly what the buggy prior
	// behavior did.

	// date type — must come after timestamp replacements
	// Only match standalone 'date' as a type (after a column name, not in other contexts)
	{regexp.MustCompile(`(?i)\bdate\b`), "TIMESTAMP"},
}

var createTableRe = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+`)
var primaryKeyInlineRe = regexp.MustCompile(`(?i)PRIMARY\s+KEY`)

var insertSchemaMigrationsRe = regexp.MustCompile(`(?is)\A(\s*INSERT\s+INTO\s+(?:"schema_migrations"|schema_migrations)\s+\([^)]*\)\s+VALUES\s+.+?)(\s*;?\s*)\z`)

// maskStringLiterals replaces every single-quoted SQL string literal in s
// with a sentinel placeholder so the downstream regex rewrites in
// pgTypeReplacements — which are not SQL-aware — cannot touch the
// characters inside. Without this, e.g. the `"(\w+)"` identifier-unquoter
// would strip double quotes out of a JSON value passed as a SQL literal
// (`INSERT … VALUES('{"k":1}')` → `INSERT … VALUES('{k:1}')`, which fails
// validation as invalid JSON).
//
// The returned restore function substitutes the originals back after all
// rewrites have run. SQL string grammar recognised: `''` inside a literal
// is an escaped single quote. Anything else (including `"`, backslashes,
// newlines) is preserved verbatim.
func maskStringLiterals(s string) (string, func(string) string) {
	var literals []string
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		if c != '\'' {
			b.WriteByte(c)
			i++
			continue
		}
		j := i + 1
		for j < len(s) {
			if s[j] != '\'' {
				j++
				continue
			}
			if j+1 < len(s) && s[j+1] == '\'' {
				j += 2
				continue
			}
			break
		}
		if j >= len(s) {
			// Unterminated string literal — emit the tail verbatim
			// and stop; downstream will fail on its own if needed.
			b.WriteString(s[i:])
			break
		}
		idx := len(literals)
		literals = append(literals, s[i:j+1])
		fmt.Fprintf(&b, "\x01%d\x01", idx)
		i = j + 1
	}
	masked := b.String()

	restore := func(m string) string {
		if len(literals) == 0 {
			return m
		}
		return stringLiteralTokenRe.ReplaceAllStringFunc(m, func(tok string) string {
			idxStr := tok[1 : len(tok)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 0 || idx >= len(literals) {
				return tok
			}
			return literals[idx]
		})
	}
	return masked, restore
}

var stringLiteralTokenRe = regexp.MustCompile("\x01(\\d+)\x01")

// commandTagRe extracts the leading SQL verb from a statement so we can
// emit the standard Postgres CommandComplete tag (`BEGIN`, `COMMIT`,
// `INSERT 0 0`, …) instead of the catch-all `ok`. Some clients (the pq
// Go driver, XORM's transaction state machine, several JDBC drivers)
// inspect the tag to decide whether a transaction actually committed,
// and `ok` confuses them with "unexpected command tag ok".
var commandTagRe = regexp.MustCompile(`(?i)^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE\s+TABLE|CREATE\s+INDEX|CREATE\s+UNIQUE\s+INDEX|CREATE\s+DATABASE|CREATE\s+VIEW|DROP\s+TABLE|DROP\s+INDEX|DROP\s+VIEW|DROP\s+DATABASE|ALTER\s+TABLE|TRUNCATE|BEGIN|START\s+TRANSACTION|COMMIT|END|ROLLBACK|ABORT|SAVEPOINT|RELEASE|SET|SHOW|GRANT|REVOKE|EXPLAIN|USE|VACUUM|ANALYZE|COPY|LOCK|FETCH|MOVE|CLOSE|DECLARE|LISTEN|NOTIFY|UNLISTEN|PREPARE|EXECUTE|DEALLOCATE|RESET|CHECKPOINT|REINDEX|DISCARD)\b`)

// commandTagFor returns the PG-canonical CommandComplete tag for a SQL
// statement. Defaults to "ok" if the verb isn't recognised, so existing
// behaviour is preserved for unusual statements.
func commandTagFor(sqlText string) string {
	m := commandTagRe.FindStringSubmatch(sqlText)
	if m == nil {
		return "ok"
	}
	verb := strings.ToUpper(strings.Join(strings.Fields(m[1]), " "))
	switch verb {
	case "BEGIN", "START TRANSACTION":
		return "BEGIN"
	case "COMMIT", "END":
		return "COMMIT"
	case "ROLLBACK", "ABORT":
		return "ROLLBACK"
	case "INSERT":
		// PG: "INSERT <oid> <count>". Most clients only check the verb;
		// emit a plausible 0-row tag.
		return "INSERT 0 0"
	case "UPDATE", "DELETE", "SELECT", "FETCH", "MOVE", "COPY":
		return verb + " 0"
	}
	return verb
}

// quotedSchemaPrefixRe matches a quoted schema qualifier directly
// followed by a dot and a (quoted or unquoted) identifier. The match
// covers known schemas only (public, pg_catalog, information_schema)
// so it can't accidentally rewrite a quoted column-list element like
// `"public", "private"` outside the schema-prefix context.
var quotedSchemaPrefixRe = regexp.MustCompile(`"(public|pg_catalog|information_schema)"\s*\.`)

// stripQuotedSchemaPrefix removes `"public".`, `"pg_catalog".`,
// `"information_schema".` prefixes that ORMs (XORM, Hibernate, JDBC)
// emit in DDL and DML. immudb's grammar has no schema.table production,
// so leaving the dot in place trips the parser with
//   "syntax error: unexpected DOT, expecting '('".
func stripQuotedSchemaPrefix(s string) string {
	return quotedSchemaPrefixRe.ReplaceAllString(s, "")
}

// alterAddRe matches `ALTER TABLE <name> ADD <next-token>` so a Go-side
// pass can decide whether to inject `COLUMN` before <next-token>.
// Captures: 1=tableName, 2=whitespace-after-ADD, 3=next-token (raw).
var alterAddRe = regexp.MustCompile(`(?i)\bALTER\s+TABLE\s+(\S+)\s+ADD(\s+)(\S+)`)

// addClauseSkipKeywords is the set of tokens that follow ADD in
// constraint/index forms (ADD CONSTRAINT, ADD FOREIGN KEY, …) — we
// must NOT inject COLUMN ahead of these because immudb's grammar parses
// them as a separate ALTER variant (or the rule is blacklisted).
var addClauseSkipKeywords = map[string]bool{
	"COLUMN": true, "CONSTRAINT": true, "FOREIGN": true,
	"PRIMARY": true, "UNIQUE": true, "CHECK": true, "INDEX": true,
}

// injectAddColumnKeyword rewrites `ALTER TABLE x ADD <colspec>` to
// `ALTER TABLE x ADD COLUMN <colspec>` when <colspec> begins with a
// (quoted or unquoted) identifier rather than a constraint keyword.
// PG accepts both forms; immudb's grammar requires the explicit COLUMN.
func injectAddColumnKeyword(s string) string {
	return alterAddRe.ReplaceAllStringFunc(s, func(match string) string {
		m := alterAddRe.FindStringSubmatch(match)
		if m == nil {
			return match
		}
		next := strings.ToUpper(strings.Trim(m[3], `"`))
		if addClauseSkipKeywords[next] {
			return match
		}
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN%s%s", m[1], m[2], m[3])
	})
}

// paramMarkerRe matches `$N` placeholder references in a SQL statement,
// excluding any that fall inside a single-quoted literal (those are
// pre-masked by maskStringLiterals, so the leading `$` is preserved
// verbatim and we don't accidentally count them here).
var paramMarkerRe = regexp.MustCompile(`\$(\d+)`)

// cleanup regexes used in removePGCatalogReferences — compiled once at init.
var (
	doubleSpaceRe   = regexp.MustCompile(`  +`)
	doubleCommaRe   = regexp.MustCompile(`(?m)^\s*,\s*,`)
	trailingCommaRe = regexp.MustCompile(`,\s*\)`)
)

// inferParamColsFromSQL scans a SQL statement for the highest `$N`
// placeholder and returns N AnyType ColDescriptors named param1..paramN.
// Used in the Extended Query Parse path for queries that the wire layer
// emulates (pg_catalog, pg_tables, …) and so never reach the engine's
// real inferParameters. Without this, ParameterDescription advertises
// zero parameters and the client's subsequent Bind with N>0 values
// trips the standard "got N parameters but the statement requires 0"
// pq driver error.
func inferParamColsFromSQL(stmtSQL string) []sql.ColDescriptor {
	masked, _ := maskStringLiterals(stmtSQL)
	max := 0
	for _, m := range paramMarkerRe.FindAllStringSubmatch(masked, -1) {
		n, err := strconv.Atoi(m[1])
		if err == nil && n > max {
			max = n
		}
	}
	if max == 0 {
		return nil
	}
	cols := make([]sql.ColDescriptor, max)
	for i := 0; i < max; i++ {
		cols[i] = sql.ColDescriptor{Column: fmt.Sprintf("param%d", i+1), Type: sql.AnyType}
	}
	return cols
}

// psqlOperatorRegexEqRe turns psql's anchored single-name regex match
// into equality. psql's \d-family meta-commands emit queries like
//   WHERE c.relname OPERATOR(pg_catalog.~) '^(mytable)$'
// and immudb's SQL grammar has no `~` regex operator. The anchored
// regex body, which psql uses to pin to exactly one name, is
// semantically identical to `= 'mytable'`.
//
// The rule requires a `|`-free body — alternations (`^(a|b|c)$`) are
// structural and deferred to the future AST rewriter (Part B).
var psqlOperatorRegexEqRe = regexp.MustCompile(
	`(?i)\s+OPERATOR\s*\(\s*(?:pg_catalog\.)?~\s*\)\s*'\^\(([^)|]+)\)\$'`)

// psqlOperatorRegexEqNoParenRe covers the rarer paren-less form some
// clients emit: `c.relname ~ '^mytable$'`.
var psqlOperatorRegexEqNoParenRe = regexp.MustCompile(
	`(?i)\s+OPERATOR\s*\(\s*(?:pg_catalog\.)?~\s*\)\s*'\^([^$|']+)\$'`)

// psqlOidStringLiteralRe coerces `'N'` back to `N` when compared to
// an integer oid column. PostgreSQL accepts both forms via implicit
// coercion; immudb's SQL engine does not, so psql's second round-trip
// `WHERE c.oid = '16384'` would fail type-check against pg_class.oid
// (IntegerType) without this rewrite.
//
// The column allowlist is bounded to known integer-oid columns across
// pg_class / pg_attribute / pg_index so the rewrite can't over-match
// into a genuinely string-typed column.
var psqlOidStringLiteralRe = regexp.MustCompile(
	`(?i)(\.\s*(?:oid|relnamespace|relfilenode|reltoastrelid|reltype|reloftype|relam|relowner|attrelid|atttypid|attcollation|indrelid|indexrelid|indcollation)\s*=\s*)'(\d+)'`)

// normalizePsqlPatterns performs the subset of query rewriting that
// must see string literals intact. Everything else goes through
// pgTypeReplacements, which runs after maskStringLiterals hides
// literal contents. See removePGCatalogReferences for the flow.
//
// Each rule is narrowly scoped to a psql meta-command pattern — over-
// matching would corrupt non-psql queries, so we prefer leaving
// edge cases to the canned-handler fallback.
func normalizePsqlPatterns(sql string) string {
	sql = psqlOperatorRegexEqRe.ReplaceAllString(sql, " = '$1'")
	sql = psqlOperatorRegexEqNoParenRe.ReplaceAllString(sql, " = '$1'")
	sql = psqlOidStringLiteralRe.ReplaceAllString(sql, "${1}${2}")
	return sql
}

func removePGCatalogReferences(sqlStr string) string {
	// Normalise PG-specific patterns that operate on string literals
	// *before* masking (maskStringLiterals hides literal contents so
	// the downstream regex chain can't touch them, but these rules
	// specifically need to see the literal). See normalizePsqlPatterns
	// for the rules and why they're kept separate.
	sqlStr = normalizePsqlPatterns(sqlStr)

	// Mask string literals first so regex-based rewrites can't mutate
	// their contents (see maskStringLiterals for rationale).
	s, restore := maskStringLiterals(sqlStr)

	s = strings.ReplaceAll(s, "pg_catalog.", "")
	s = strings.ReplaceAll(s, "information_schema.", "information_schema_")
	s = strings.ReplaceAll(s, "public.", "")

	// Strip QUOTED schema-qualified prefixes too — XORM, Hibernate, JDBC
	// and friends emit `"public"."tablename"` (and `"pg_catalog"."pg_x"`)
	// in DDL. The plain string ReplaceAll's above don't catch the quoted
	// form, and the immudb grammar has no schema.table production so the
	// stray DOT shows up at parse time as
	//   "syntax error: unexpected DOT, expecting '('".
	// Run this BEFORE the identifier-unquote pass in pgTypeReplacements
	// so we eliminate the dot in the same step that drops the prefix.
	s = stripQuotedSchemaPrefix(s)

	// Apply PG type translations
	for _, r := range pgTypeReplacements {
		s = r.re.ReplaceAllString(s, r.repl)
	}

	// PG `ALTER TABLE … ADD <coldef>` → immudb `ADD COLUMN <coldef>`.
	// Done as a Go pass so we can skip the constraint forms (ADD
	// CONSTRAINT / FOREIGN / PRIMARY / UNIQUE / CHECK / INDEX) which
	// take a different syntax in immudb (or are blacklisted).
	s = injectAddColumnKeyword(s)

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

	// Translate inline / table-level UNIQUE constraints into trailing
	// `CREATE UNIQUE INDEX` statements. Must run AFTER
	// addPrimaryKeyToCreateTable — that pass scans for the last ")" in
	// the string, and splitting the CREATE TABLE off from the new
	// CREATE UNIQUE INDEX statements would put that ")" inside the
	// INDEX parens (wrong target).
	s = extractUniqueConstraints(s)

	// Clean up double spaces and empty lines
	s = doubleSpaceRe.ReplaceAllString(s, " ")
	s = doubleCommaRe.ReplaceAllString(s, ",")
	// Remove trailing commas before closing paren
	s = trailingCommaRe.ReplaceAllString(s, "\n)")

	return restore(s)
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
	// Sort by the numeric suffix of "paramN" rather than lexicographically.
	// Lexical sort gives param1, param10, param11, …, param2, param3 —
	// which makes positional Bind values land on the wrong $N in
	// statements with 10+ parameters and silently corrupts inserts.
	sort.Slice(paramsNameList, func(i, j int) bool {
		ai, aok := paramNumericSuffix(paramsNameList[i])
		bi, bok := paramNumericSuffix(paramsNameList[j])
		if aok && bok {
			return ai < bi
		}
		return paramsNameList[i] < paramsNameList[j]
	})

	paramCols := make([]sql.ColDescriptor, 0)
	for _, n := range paramsNameList {
		paramCols = append(paramCols, sql.ColDescriptor{Column: n, Type: r[n]})
	}
	return paramCols, resCols, nil
}

// paramNumericSuffix returns the integer suffix of names like "param12".
// Used to sort parameter names by position rather than lexicographically.
func paramNumericSuffix(name string) (int, bool) {
	if !strings.HasPrefix(name, "param") {
		return 0, false
	}
	n, err := strconv.Atoi(name[len("param"):])
	if err != nil {
		return 0, false
	}
	return n, true
}
