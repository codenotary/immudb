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
	"strings"

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

	// pgVirtualTableFromRe matches a query whose FROM clause names one of the
	// virtual catalog tables the SQL engine implements natively. Covers
	// everything registered by pkg/pgsql/sys/: the relation catalog
	// (pg_class, pg_attribute, pg_index, pg_namespace, pg_am, pg_type),
	// auxiliary catalogs (pg_database, pg_roles, pg_settings,
	// pg_constraint, pg_description, pg_proc), and the A5 compat views
	// (pg_tables, pg_indexes, pg_views, pg_sequences). A match flags
	// the query as a candidate for engine passthrough;
	// allPgRefsRegistered decides whether it actually qualifies.
	pgVirtualTableFromRe = regexp.MustCompile(`(?i)\bfrom\s+(?:pg_catalog\.)?(?:pg_class|pg_attribute|pg_index|pg_namespace|pg_am|pg_type|pg_settings|pg_constraint|pg_database|pg_roles|pg_description|pg_proc|pg_tables|pg_indexes|pg_views|pg_sequences)\b`)

	// pgAnyTableRe finds every reference to a pg_ table in the statement.
	// Used by allPgRefsRegistered to decide whether a JOIN-containing
	// query only touches tables the engine can actually serve.
	pgAnyTableRe = regexp.MustCompile(`(?i)\b(pg_[a-z_]+)\b`)

	// registeredPgTables is the set of pg_catalog objects the SQL engine
	// can serve via the catalog-level system-table registry
	// (pkg/pgsql/sys/ — installed at engine-init time). Keep in sync
	// with pgVirtualTableFromRe and the init()s in pkg/pgsql/sys/.
	registeredPgTables = map[string]bool{
		// pkg/pgsql/sys/ — core relation catalog (A2)
		"pg_class":     true,
		"pg_attribute": true,
		"pg_index":     true,
		"pg_namespace": true,
		"pg_am":        true,
		"pg_type":      true,
		// pkg/pgsql/sys/ — auxiliary catalogs (A3)
		"pg_database":    true,
		"pg_roles":       true,
		"pg_settings":    true,
		"pg_constraint":  true,
		"pg_description": true,
		"pg_proc":        true,
		// pkg/pgsql/sys/ — compat views (A5)
		"pg_tables":    true,
		"pg_indexes":   true,
		"pg_views":     true,
		"pg_sequences": true,
	}

	// pgBuiltinFunctions are known pg_catalog-qualified functions built
	// into embedded/sql/functions.go. A reference to one of these does
	// not disqualify a query from engine passthrough even though it
	// matches pgAnyTableRe's identifier shape. Keep in sync with
	// embedded/sql/functions.go's builtinFunctions map.
	pgBuiltinFunctions = map[string]bool{
		"pg_table_is_visible":     true,
		"pg_get_userbyid":         true,
		"pg_get_expr":             true,
		"pg_get_constraintdef":    true,
		"pg_get_indexdef":         true,
		"pg_get_serial_sequence":  true,
		"pg_encoding_to_char":     true,
		"pg_total_relation_size":  true,
		"pg_relation_size":        true,
	}

	// xormColumnsRe matches XORM's distinctive column-introspection query
	// (SELECT column_name, column_default, is_nullable, … FROM pg_attribute
	// JOIN pg_class …). XORM selects info_schema-flavoured column names
	// against pg_attribute, which the SQL engine can't serve directly
	// (pg_attribute's own columns are attname/attnotnull/…). The handler
	// walks immudb's catalog and emits one row per column.
	xormColumnsRe = regexp.MustCompile(`(?is)\bcolumn_name\s*,\s*column_default\b.*\bfrom\s+pg_attribute\b`)

	// infoSchemaVirtualTableFromRe flags queries whose FROM clause
	// names one of the information_schema tables the SQL engine
	// implements natively via pkg/pgsql/sys/ (A4). When the dispatcher
	// spots one, the query is forwarded to the engine so JOINs and
	// complex WHEREs work correctly — the legacy infoSchemaColumnsRe
	// canned handler stays wired as a fallback for the tiny number of
	// information_schema views we haven't registered yet.
	infoSchemaVirtualTableFromRe = regexp.MustCompile(`(?is)\bfrom\s+information_schema[._](tables|columns|schemata|key_column_usage|table_constraints)\b`)
)

// CREATE INDEX / CREATE UNIQUE INDEX are SUPPORTED by immudb's engine
// (see embedded/sql/sql_grammar.y:390-408). PG-style statements arrive
// with an index name (`CREATE INDEX idx_foo ON t(c)`) which immudb's
// grammar rejects; the name-strip regex in query_machine.go's
// pgTypeReplacements handles that before the statement reaches the
// engine. Do NOT re-add them to this blacklist — blacklisting turns
// them into silent no-ops, which destroys uniqueness guarantees.
var pgUnsupportedDDL = regexp.MustCompile(`(?i)^\s*(CREATE\s+TYPE|CREATE\s+FUNCTION|CREATE\s+OR\s+REPLACE\s+FUNCTION|CREATE\s+TRIGGER|CREATE\s+RULE|CREATE\s+EXTENSION|CREATE\s+CAST|CREATE\s+OPERATOR|CREATE\s+AGGREGATE|CREATE\s+DOMAIN|ALTER\s+TABLE\s+\S+\s+OWNER\s+TO|ALTER\s+TABLE\s+\S+\s+ALTER\s+COLUMN|ALTER\s+TABLE\s+ONLY|ALTER\s+TABLE\s+\S+\s+DISABLE|ALTER\s+TABLE\s+\S+\s+ENABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+(?:CONSTRAINT\s+\S+\s+)?FOREIGN\s+KEY|ALTER\s+TABLE\s+\S+\s+ADD\b|ALTER\s+SEQUENCE|ALTER\s+FUNCTION|ALTER\s+TYPE|GRANT\s|REVOKE\s|COMMENT\s+ON|SELECT\s+pg_catalog\.|SELECT\s+setval|SET\s+default_tablespace|SET\s+default_table_access_method|SET\s+transaction_timeout|DROP\s+INDEX)`)

// allPgRefsRegistered returns true when every pg_* identifier in
// statement is either a registered system table (registeredPgTables)
// or a registered built-in function (pgBuiltinFunctions). Used by the
// dispatcher to decide whether a query can safely execute against the
// SQL engine instead of being shunted to a canned handler.
//
// The scan is deliberately permissive: pg_* substrings that happen to
// appear inside string literals are rare in real client traffic and
// a false "unregistered" classification only means we fall through to
// the canned handler (no correctness loss, just the old behaviour).
func allPgRefsRegistered(statement string) bool {
	matches := pgAnyTableRe.FindAllStringSubmatch(statement, -1)
	for _, m := range matches {
		name := strings.ToLower(m[1])
		// pg_catalog is a namespace qualifier, not a table — it gets
		// stripped by removePGCatalogReferences before the engine
		// sees the statement. Skip it here so queries that reference
		// `pg_catalog.pg_class` aren't wrongly disqualified.
		if name == "pg_catalog" {
			continue
		}
		if registeredPgTables[name] || pgBuiltinFunctions[name] {
			continue
		}
		return false
	}
	return true
}

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

	// XORM column-introspection: the one ORM shape where the query
	// selects info_schema-flavoured column names (column_name,
	// column_default, is_nullable, …) but FROMs pg_attribute. The
	// engine would reject this because pg_attribute doesn't expose
	// those column names — the canned handler synthesises a result set
	// that matches what XORM expects. Must run BEFORE the engine
	// passthrough below or the dispatcher sends it to the engine and
	// the client gets "column does not exist".
	if xormColumnsRe.MatchString(statement) {
		return &xormColumnsCmd{sql: statement}
	}

	// Engine passthrough for queries targeting registered pg_catalog
	// or information_schema system tables. The A2-A5 sys/ tables cover
	// every common ORM / psql introspection shape; JOINs, ORDER BY,
	// complex WHERE clauses, and sub-queries all work because the SQL
	// engine handles them against the live catalog.
	if pgVirtualTableFromRe.MatchString(statement) && allPgRefsRegistered(statement) {
		return nil
	}
	if infoSchemaVirtualTableFromRe.MatchString(statement) && allPgRefsRegistered(statement) {
		return nil
	}

	// Safety net for pg_catalog / information_schema probes that
	// reference tables we haven't registered (pg_extension,
	// pg_tablespace, pg_replication_slots, pg_locks, …). Returns
	// a canned one-row response with column names extracted from the
	// query's SELECT list — avoids "table does not exist" errors on
	// clients that probe every possible system view at connect time.
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
	case *xormColumnsCmd:
		return s.handleXormColumnsQuery(cmd.sql, nil, false)
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

// xormColumnsCmd is XORM's column-introspection query: a multi-table
// JOIN of pg_attribute / pg_class / pg_type / information_schema.columns
// where the SELECT list uses info_schema-flavoured column names
// (column_name, column_default, is_nullable, …) against pg_attribute.
// The SQL engine can't serve this shape directly — pg_attribute's own
// columns are attname / attnotnull / … — so the handler synthesises
// one row per column with the fields XORM expects (column_name,
// column_default, is_nullable, data_type, character_maximum_length,
// description, primarykey, uniquekey).
type xormColumnsCmd struct {
	sql string
}
