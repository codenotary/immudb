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
	"regexp"

	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
)

var (
	// Match only top-level PG session SET commands (SET timezone=..., SET
	// client_encoding=..., etc.), NOT the SET clause inside `UPDATE ... SET
	// col = val WHERE ...`. Without the start-of-statement anchor every
	// UPDATE silently matched this regex and was dropped by the blacklist,
	// so UPDATEs "succeeded" but never wrote anything.
	set           = regexp.MustCompile(`(?is)^\s*set\s+\S+`)
	selectVersion = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)
	dealloc       = regexp.MustCompile(`(?i)deallocate\s+\"([^\"]+)\"`)

	// immudb verification functions exposed via PG wire protocol
	immudbStateRe     = regexp.MustCompile(`(?i)select\s+immudb_state\(\s*\)`)
	immudbVerifyRowRe = regexp.MustCompile(`(?i)select\s+immudb_verify_row\(\s*(.+)\s*\)`)
	immudbVerifyTxRe  = regexp.MustCompile(`(?i)select\s+immudb_verify_tx\(\s*(.+)\s*\)`)
	immudbHistoryRe   = regexp.MustCompile(`(?i)select\s+immudb_history\(\s*(.+)\s*\)`)
	immudbTxRe        = regexp.MustCompile(`(?i)select\s+immudb_tx\(\s*(.+)\s*\)`)

	// SHOW statement patterns for ORM compatibility
	showRe = regexp.MustCompile(`(?i)^\s*show\s+(\w+)\s*;?\s*$`)

	// regtype OID lookup: Rails (and other ORMs) resolve custom type OIDs with
	// queries like:   SELECT 'jsonb'::regtype::oid
	//                 SELECT 'decimal(19,4)'::regtype::oid
	// immudb's SQL engine does not implement regtype; without interception it
	// silently returns the literal string. Rails then stores the string into
	// its OID map and blows up later ("can't quote Hash") when it tries to
	// bind a Hash parameter against a column typed by that non-integer OID.
	regtypeOidRe = regexp.MustCompile(`(?i)^\s*select\s+'([^']+)'::regtype::oid\s*;?\s*$`)

	// Rails column introspection: drives ActiveRecord's knowledge of each
	// table's columns. Without real data here, models that declare
	// `enum :role, ...` fail with "Undeclared attribute type for enum 'role'".
	// Match the canonical Rails 7 form of this query (multi-line, joins on
	// pg_attrdef / pg_type / pg_collation, filter on attrelid = '"t"'::regclass).
	// Literal form — simple query protocol.
	pgAttributeForTableRe = regexp.MustCompile(`(?is)SELECT\s+a\.attname\s*,\s*format_type\s*\([^)]*\).*?FROM\s+pg_attribute\s+a.*?WHERE\s+a\.attrelid\s*=\s*'"?([^"']+)"?'::regclass`)

	// Parameterised form — Extended Query protocol (Rails's default).
	// Table name arrives as the bound value of $1 at Execute time; the
	// caller is responsible for substituting the parameter value for
	// the empty tableName slot in pgAttributeForTableCmd. Note the
	// `::regclass` cast is stripped by removePGCatalogReferences before
	// this regex runs, so we match on bare `$1` here.
	pgAttributeForTableParamRe = regexp.MustCompile(`(?is)SELECT\s+a\.attname\s*,\s*format_type\s*\([^)]*\).*?FROM\s+pg_attribute\s+a.*?WHERE\s+a\.attrelid\s*=\s*\$1\b`)

	// Rails's db:migrate takes a Postgres advisory lock to serialise
	// concurrent migrations. immudb has no advisory-lock subsystem; return
	// true unconditionally to let the single Rails container proceed.
	// Matches: SELECT pg_try_advisory_lock(...), pg_advisory_unlock(...)
	pgAdvisoryLockRe = regexp.MustCompile(`(?i)^\s*select\s+pg_(?:try_)?advisory_(?:lock|unlock)\s*\(`)

	// Blanket intercept for all PostgreSQL system catalog queries.
	// pgAdmin, DBeaver, ORMs etc. send dozens of these after connecting.
	// immudb can't execute them, so we return canned responses.
	pgSystemQueryRe = regexp.MustCompile(`(?i)pg_catalog\.|information_schema\.|pg_roles\b|pg_database\b|pg_settings\b|pg_extension\b|pg_tablespace\b|pg_replication_slots\b|pg_stat_activity\b|pg_authid\b|pg_shdescription\b|pg_description\b|pg_am\b|pg_stat_replication\b|pg_auth_members\b|pg_namespace\b|pg_class\b|pg_attribute\b|pg_type\b|pg_proc\b|pg_constraint\b|pg_index\b|pg_depend\b|pg_stat_user_tables\b|pg_statio_user_tables\b|pg_locks\b|pg_shadow\b|pg_user\b|pg_tables\b|pg_indexes\b|pg_views\b|pg_matviews\b|pg_sequences\b|current_setting\s*\(|has_database_privilege\s*\(|has_table_privilege\s*\(|has_schema_privilege\s*\(|pg_encoding_to_char\s*\(|pg_get_userbyid\s*\(`)

	// pgTablesRe singles out queries against the standard `pg_tables` view
	// (and its sister catalog views) so the handler can enumerate immudb's
	// actual tables and apply a WHERE filter, rather than returning the
	// generic "one canned row" response — XORM and other ORMs probe these
	// views to detect table existence, and a blanket 1-row response makes
	// every IsTableExist check return true.
	pgTablesRe = regexp.MustCompile(`(?is)\bfrom\s+(?:pg_catalog\.)?pg_tables\b`)

	// xormColumnsRe matches XORM's distinctive column-introspection query
	// (SELECT column_name, column_default, is_nullable, … FROM pg_attribute
	// JOIN pg_class …). XORM scans the result expecting non-NULL string
	// values for column_name; the generic 1-row pgAdminProbe response
	// (which fills NULL for unknown columns) trips the pq Scan check
	//   "converting NULL to string is unsupported".
	// The handler walks immudb's catalog and emits one row per column.
	xormColumnsRe = regexp.MustCompile(`(?is)\bcolumn_name\s*,\s*column_default\b.*\bfrom\s+pg_attribute\b`)

	// pgIndexesRe matches the standard `pg_indexes` view query that ORMs
	// (XORM, GORM, Hibernate) use to enumerate indexes for a given table.
	// The handler walks immudb's catalog and emits one row per index.
	pgIndexesRe = regexp.MustCompile(`(?is)\bfrom\s+(?:pg_catalog\.)?pg_indexes\b`)

	// infoSchemaColumnsRe matches the standard
	// `information_schema.columns` view query (psql `\d`, JDBC schema
	// browsers, alembic, flyway, …). The wire normaliser rewrites
	// `information_schema.` to `information_schema_`, so the regex
	// matches both forms. Without dedicated handling the canned
	// 1-row pgAdminProbe response returns NULL for column_name and
	// crashes any client that scans the result into a string column.
	infoSchemaColumnsRe = regexp.MustCompile(`(?is)\bfrom\s+information_schema[._]columns\b`)
)

var pgUnsupportedDDL = regexp.MustCompile(`(?i)^\s*(CREATE\s+TYPE|CREATE\s+FUNCTION|CREATE\s+OR\s+REPLACE\s+FUNCTION|CREATE\s+TRIGGER|CREATE\s+RULE|CREATE\s+EXTENSION|CREATE\s+CAST|CREATE\s+OPERATOR|CREATE\s+AGGREGATE|CREATE\s+SEQUENCE|CREATE\s+DOMAIN|CREATE\s+VIEW|CREATE\s+OR\s+REPLACE\s+VIEW|ALTER\s+TABLE\s+\S+\s+OWNER\s+TO|ALTER\s+TABLE\s+\S+\s+ALTER\s+COLUMN|ALTER\s+TABLE\s+ONLY|ALTER\s+TABLE\s+\S+\s+DISABLE|ALTER\s+TABLE\s+\S+\s+ENABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+(?:CONSTRAINT\s+\S+\s+)?FOREIGN\s+KEY|ALTER\s+TABLE\s+\S+\s+ADD\b|ALTER\s+SEQUENCE|ALTER\s+FUNCTION|ALTER\s+TYPE|GRANT\s|REVOKE\s|COMMENT\s+ON|CREATE\s+INDEX|CREATE\s+UNIQUE\s+INDEX|SELECT\s+pg_catalog\.|SELECT\s+setval|SET\s+default_tablespace|SET\s+default_table_access_method|SET\s+transaction_timeout|DROP\s+INDEX)`)

func (s *session) isInBlackList(statement string) bool {
	if set.MatchString(statement) {
		return true
	}

	if statement == ";" {
		return true
	}

	// Silently ignore unsupported PostgreSQL DDL statements
	if pgUnsupportedDDL.MatchString(statement) {
		return true
	}

	return false
}

func (s *session) isEmulableInternally(statement string) interface{} {
	if selectVersion.MatchString(statement) {
		return &version{}
	}

	if dealloc.MatchString(statement) {
		matches := dealloc.FindStringSubmatch(statement)
		if len(matches) == 2 {
			return &deallocate{plan: matches[1]}
		}
	}

	if immudbStateRe.MatchString(statement) {
		return &immudbStateCmd{}
	}

	if m := immudbVerifyRowRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbVerifyRowCmd{args: m[1]}
	}

	if m := immudbVerifyTxRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbVerifyTxCmd{args: m[1]}
	}

	if m := immudbHistoryRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbHistoryCmd{args: m[1]}
	}

	if m := immudbTxRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbTxCmd{args: m[1]}
	}

	if m := showRe.FindStringSubmatch(statement); len(m) == 2 {
		return &showCmd{param: m[1]}
	}

	if m := regtypeOidRe.FindStringSubmatch(statement); len(m) == 2 {
		return &regtypeOidCmd{typeName: m[1]}
	}

	if m := pgAttributeForTableRe.FindStringSubmatch(statement); len(m) == 2 {
		s.log.Infof("pgcompat: pg_attribute intercept for table %q", m[1])
		return &pgAttributeForTableCmd{tableName: m[1]}
	}

	if pgAttributeForTableParamRe.MatchString(statement) {
		// Parameterised form: table name is bound via $1 at Execute time.
		// Signal with an empty tableName — the caller pulls the value out
		// of the bind parameters and passes it in.
		s.log.Infof("pgcompat: pg_attribute intercept (parameterised, $1)")
		return &pgAttributeForTableCmd{tableName: ""}
	}

	if pgAdvisoryLockRe.MatchString(statement) {
		return &pgAdvisoryLockCmd{}
	}

	// pg_tables view: enumerate immudb's catalog rather than emit canned
	// rows. XORM / GORM / SQLAlchemy / Hibernate all probe this to decide
	// whether to CREATE TABLE; a one-row canned response would say "every
	// table exists" and skip every CREATE.
	if pgTablesRe.MatchString(statement) {
		return &pgTablesCmd{sql: statement}
	}

	// XORM column-introspection query: distinct column-list signature
	// SELECT column_name, column_default, is_nullable, …
	// Goes ahead of the generic pg_catalog catch because the canned
	// 1-row response would have NULL column_name and crash XORM.
	if xormColumnsRe.MatchString(statement) {
		return &xormColumnsCmd{sql: statement}
	}

	// pg_indexes view: enumerate indexes from immudb's catalog rather
	// than emit a canned NULL-filled row that crashes ORM Scan calls.
	if pgIndexesRe.MatchString(statement) {
		return &pgIndexesCmd{sql: statement}
	}

	// information_schema.columns: standard SQL view used by psql,
	// JDBC, alembic, flyway. We synthesise rows from the catalog with
	// the canonical column shape (table_catalog, table_schema,
	// table_name, column_name, ordinal_position, column_default,
	// is_nullable, data_type, character_maximum_length, numeric_*,
	// datetime_precision, …).
	if infoSchemaColumnsRe.MatchString(statement) {
		return &infoSchemaColumnsCmd{sql: statement}
	}

	// Blanket catch for ALL pg_catalog/information_schema/system queries.
	// Returns canned responses with column names extracted from the query.
	if pgSystemQueryRe.MatchString(statement) {
		return &pgAdminProbe{sql: statement}
	}

	return nil
}

func (s *session) tryToHandleInternally(command interface{}) error {
	switch cmd := command.(type) {
	case *version:
		if err := s.writeVersionInfo(); err != nil {
			return err
		}
	case *deallocate:
		delete(s.statements, cmd.plan)
		return nil
	case *immudbStateCmd:
		return s.immudbState()
	case *immudbVerifyRowCmd:
		return s.immudbVerifyRow(cmd.args)
	case *immudbVerifyTxCmd:
		return s.immudbVerifyTx(cmd.args)
	case *immudbHistoryCmd:
		return s.immudbHistory(cmd.args)
	case *immudbTxCmd:
		return s.immudbTxByID(cmd.args)
	case *showCmd:
		return s.handleShow(cmd.param)
	case *regtypeOidCmd:
		return s.handleRegtypeOid(cmd.typeName)
	case *pgAttributeForTableCmd:
		// Simple query path — no Describe beforehand, so always emit RowDescription.
		return s.handlePgAttributeForTable(cmd.tableName, false)
	case *pgAdvisoryLockCmd:
		return s.handlePgAdvisoryLock()
	case *pgAdminProbe:
		return s.handlePgSystemQuery(cmd.sql)
	case *pgTablesCmd:
		// Simple-query path — no Bind so no parameters available.
		return s.handlePgTablesQuery(cmd.sql, nil, false)
	case *xormColumnsCmd:
		return s.handleXormColumnsQuery(cmd.sql, nil, false)
	case *pgIndexesCmd:
		return s.handlePgIndexesQuery(cmd.sql, nil, false)
	case *infoSchemaColumnsCmd:
		return s.handleInfoSchemaColumnsQuery(cmd.sql, nil, false)
	default:
		return pserr.ErrMessageCannotBeHandledInternally
	}
	return nil
}

type version struct{}

type deallocate struct {
	plan string
}

type immudbStateCmd struct{}

type immudbVerifyRowCmd struct {
	args string
}

type immudbVerifyTxCmd struct {
	args string
}

type immudbHistoryCmd struct {
	args string
}

type immudbTxCmd struct {
	args string
}

type regtypeOidCmd struct {
	typeName string
}

type pgAttributeForTableCmd struct {
	tableName string
}

type pgAdvisoryLockCmd struct{}

type showCmd struct {
	param string
}

type pgAdminProbe struct {
	sql string
}

// pgTablesCmd is a query against the standard `pg_tables` view. The handler
// enumerates immudb's catalog (with optional WHERE filter) instead of
// emitting a canned single-row response that would make every IsTableExist
// probe return true.
type pgTablesCmd struct {
	sql string
}

// xormColumnsCmd is XORM's column-introspection query (a multi-table JOIN
// of pg_attribute / pg_class / pg_type / information_schema.columns). The
// handler synthesises one row per column from immudb's catalog with the
// fields XORM expects (column_name, column_default, is_nullable, data_type,
// character_maximum_length, description, primarykey, uniquekey).
type xormColumnsCmd struct {
	sql string
}

// pgIndexesCmd is a query against the standard `pg_indexes` view. The
// handler enumerates immudb's indexes for the given table and emits the
// canonical (schemaname, tablename, indexname, tablespace, indexdef) row
// shape so ORM Scan calls don't trip on NULLs.
type pgIndexesCmd struct {
	sql string
}

// infoSchemaColumnsCmd is a query against the standard SQL view
// `information_schema.columns`. The handler enumerates immudb's
// catalog columns and emits one row per column with the canonical
// information_schema.columns column shape.
type infoSchemaColumnsCmd struct {
	sql string
}
