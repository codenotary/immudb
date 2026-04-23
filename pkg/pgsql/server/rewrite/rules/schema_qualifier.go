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

package rules

import (
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

// StripSchemaQualifier drops the schema prefix from table references
// whose schema is one of the well-known PG catalog schemas (or the
// default `public` schema). Matches the three ReplaceAll calls in
// query_machine.go:979-981:
//
//	s = strings.ReplaceAll(s, "pg_catalog.", "")
//	s = strings.ReplaceAll(s, "information_schema.", "information_schema_")
//	s = strings.ReplaceAll(s, "public.", "")
//
// Plus the `stripQuotedSchemaPrefix` pass at query_machine.go:847,
// which handles the `"public"."table"` XORM / Hibernate / JDBC form.
//
// Semantics:
//   - `pg_catalog.pg_class` → `pg_class`                   (drop)
//   - `public.users`        → `users`                      (drop)
//   - `information_schema.columns` → `information_schema_columns`
//     (rewrite: underscore-join, matching the sys/ system-table
//     naming convention where info_schema views live under the
//     public namespace with underscored names)
//
// Any other schema qualifier is left alone — immudb is single-
// schema so those queries will error at the engine layer, but that
// failure mode is more useful than silently stripping them.
type StripSchemaQualifier struct{}

// Name implements rewrite.Rule.
func (StripSchemaQualifier) Name() string { return "StripSchemaQualifier" }

// Apply implements rewrite.Rule.
func (s StripSchemaQualifier) Apply(stmt tree.Statement) tree.Statement {
	stripTableNamesInStmt(stmt)
	return stmt
}

// stripTableNamesInStmt walks every TableName inside the statement
// and applies stripOneTableName. The type-switch covers the
// Statement shapes our clients actually emit; unhandled forms pass
// through unchanged (and thus fall back to the regex chain).
func stripTableNamesInStmt(stmt tree.Statement) {
	switch s := stmt.(type) {
	case *tree.Select:
		stripTableNamesInSelect(s)
	case *tree.Insert:
		stripOneTableName(s.Table)
		if s.Rows != nil {
			stripTableNamesInSelect(s.Rows)
		}
	case *tree.Update:
		stripOneTableName(s.Table)
	case *tree.Delete:
		stripOneTableName(s.Table)
	case *tree.CreateTable:
		stripOneTableName(&s.Table)
	}
}

func stripTableNamesInSelect(s *tree.Select) {
	if s == nil {
		return
	}
	switch inner := s.Select.(type) {
	case *tree.SelectClause:
		stripTableNamesInSelectClause(inner)
	case *tree.UnionClause:
		if inner.Left != nil {
			stripTableNamesInSelect(inner.Left)
		}
		if inner.Right != nil {
			stripTableNamesInSelect(inner.Right)
		}
	}
}

func stripTableNamesInSelectClause(s *tree.SelectClause) {
	if s == nil {
		return
	}
	for _, te := range s.From.Tables {
		stripTableNamesInFromExpr(te)
	}
}

// stripTableNamesInFromExpr walks one entry in a FROM list. FROM
// entries are wrapped in several layers of indirection (AliasedTableExpr,
// JoinTableExpr, ParenTableExpr) — recurse through each.
func stripTableNamesInFromExpr(te tree.TableExpr) {
	switch t := te.(type) {
	case *tree.AliasedTableExpr:
		if t.Expr != nil {
			stripOneTableName(t.Expr)
		}
	case *tree.JoinTableExpr:
		stripTableNamesInFromExpr(t.Left)
		stripTableNamesInFromExpr(t.Right)
	case *tree.ParenTableExpr:
		stripTableNamesInFromExpr(t.Expr)
	case *tree.TableName:
		stripOneTableName(t)
	}
}

// stripOneTableName is the only node in this rule that actually
// mutates AST state. Accepts anything that might carry a TableName;
// a generic Expr input is required because AliasedTableExpr.Expr is
// a tree.TableExpr interface, not *TableName directly.
func stripOneTableName(node interface{}) {
	tn, ok := node.(*tree.TableName)
	if !ok {
		return
	}
	if !tn.ExplicitSchema {
		return
	}
	schema := strings.ToLower(string(tn.SchemaName))
	switch schema {
	case "pg_catalog", "public":
		// Drop the schema qualifier entirely. Matches the
		// "pg_catalog." and "public." ReplaceAll calls.
		tn.ExplicitSchema = false
		tn.ExplicitCatalog = false
		tn.SchemaName = ""
		tn.CatalogName = ""
	case "information_schema":
		// Rewrite to the underscore form. The sys/ registry installs
		// these as `information_schema_columns`, not
		// `information_schema.columns` — because immudb has no
		// dotted-schema catalog.
		tn.ExplicitSchema = false
		tn.ExplicitCatalog = false
		tn.SchemaName = ""
		tn.CatalogName = ""
		tn.TableName = tree.Name("information_schema_" + string(tn.TableName))
	}
}
