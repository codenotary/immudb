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
	"regexp"

	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
)

var (
	set           = regexp.MustCompile(`(?i)set\s+.+`)
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
	pgSystemQueryRe = regexp.MustCompile(`(?i)pg_catalog\.|information_schema\.|pg_roles\b|pg_database\b|pg_settings\b|pg_extension\b|pg_tablespace\b|pg_replication_slots\b|pg_stat_activity\b|pg_authid\b|pg_shdescription\b|pg_description\b|pg_am\b|pg_stat_replication\b|pg_auth_members\b|pg_namespace\b|pg_class\b|pg_attribute\b|pg_type\b|pg_proc\b|pg_constraint\b|pg_index\b|pg_depend\b|pg_stat_user_tables\b|pg_statio_user_tables\b|pg_locks\b|pg_shadow\b|pg_user\b|current_setting\s*\(|has_database_privilege\s*\(|has_table_privilege\s*\(|has_schema_privilege\s*\(|pg_encoding_to_char\s*\(|pg_get_userbyid\s*\(`)
)

var pgUnsupportedDDL = regexp.MustCompile(`(?i)^\s*(CREATE\s+TYPE|CREATE\s+FUNCTION|CREATE\s+OR\s+REPLACE\s+FUNCTION|CREATE\s+TRIGGER|CREATE\s+RULE|CREATE\s+EXTENSION|CREATE\s+CAST|CREATE\s+OPERATOR|CREATE\s+AGGREGATE|CREATE\s+SEQUENCE|CREATE\s+DOMAIN|CREATE\s+VIEW|CREATE\s+OR\s+REPLACE\s+VIEW|ALTER\s+TABLE\s+\S+\s+OWNER\s+TO|ALTER\s+TABLE\s+\S+\s+ALTER\s+COLUMN|ALTER\s+TABLE\s+ONLY|ALTER\s+TABLE\s+\S+\s+DISABLE|ALTER\s+TABLE\s+\S+\s+ENABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+(?:CONSTRAINT\s+\S+\s+)?FOREIGN\s+KEY|ALTER\s+SEQUENCE|ALTER\s+FUNCTION|ALTER\s+TYPE|GRANT\s|REVOKE\s|COMMENT\s+ON|CREATE\s+INDEX|CREATE\s+UNIQUE\s+INDEX|SELECT\s+pg_catalog\.|SELECT\s+setval|SET\s+default_tablespace|SET\s+default_table_access_method|SET\s+transaction_timeout)`)

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
