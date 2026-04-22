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

// Package rules holds the AST transformations the rewriter applies.
// Each file is one rule, with its own table-driven test. B1 ships
// three rules as proof-of-concept; B2 will port the remaining ~55
// regex rules in query_machine.go the same way.
package rules

import (
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

// StripPGCasts removes CAST expressions whose target is one of the
// Postgres-specific pseudo-types that psql and ORMs emit as
// no-op-for-immudb casts:
//
//	c.oid::regclass
//	'foo'::text
//	t.typname::name
//	x::regtype
//
// Replaces the compound regex at query_machine.go line 477:
//
//	`\s*::\s*(regclass|oid|text|name|regtype|regproc|regoper|regnamespace|anyarray)(\s*,|\s*\))?`
//
// Rationale: these casts were only ever needed to coerce pgwire text
// columns into typed values PG planners could optimise around; the
// immudb engine handles the raw column types fine, so stripping the
// cast leaves the underlying expression unchanged semantically.
//
// Implementation: walks every Expr in a Statement and replaces
// CastExpr nodes whose Type family or name matches the allowlist
// with the CastExpr's inner Expr.
type StripPGCasts struct{}

// Name implements rewrite.Rule.
func (StripPGCasts) Name() string { return "StripPGCasts" }

// Apply implements rewrite.Rule.
func (s StripPGCasts) Apply(stmt tree.Statement) tree.Statement {
	// Drive the rewrite through walkStatementExprs — we're a
	// single-Expr transform, no Statement-level surgery needed.
	return walkStatementExprs(stmt, stripCastVisitFn)
}

// pgStripCastTypeNames is the allowlist of type names that, when
// used as the target of a CAST, indicate a PG-compat cast we should
// unwrap. Matched case-insensitively against CastExpr.Type.SQLString.
//
// Matches the regex's alternation exactly:
//
//	regclass|oid|text|name|regtype|regproc|regoper|regnamespace|anyarray
//
// Intentionally does NOT include sized-text forms like CHAR(n) —
// those are real type conversions the user asked for.
var pgStripCastTypeNames = map[string]struct{}{
	"regclass":      {},
	"oid":           {},
	"text":          {},
	"name":          {},
	"regtype":       {},
	"regproc":       {},
	"regoper":       {},
	"regnamespace":  {},
	"anyarray":      {},
	// auxten normalises ::text to ::STRING, so accept both spellings.
	// The regex path matches "text" in the raw SQL; after parsing,
	// the AST Type is STRING. Including both makes the rule survive
	// the parse+re-deparse round trip that the regex rule doesn't
	// have to deal with.
	"string": {},
}

// stripCastVisitFn implements rewrite.SimpleVisitFn semantics for
// the CAST unwrap. Returns the inner expression in place of any
// CastExpr whose target type is in the allowlist.
func stripCastVisitFn(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
	cast, ok := expr.(*tree.CastExpr)
	if !ok {
		return true, expr, nil
	}
	if cast.Type == nil {
		return true, expr, nil
	}
	typeName := strings.ToLower(cast.Type.SQLString())
	// CockroachDB's SQLString() can include array / precision
	// suffixes — strip at the first non-identifier character so the
	// allowlist comparison works for bare type names.
	if idx := strings.IndexAny(typeName, "( []"); idx > 0 {
		typeName = typeName[:idx]
	}
	if _, stripMe := pgStripCastTypeNames[typeName]; !stripMe {
		return true, expr, nil
	}
	// Replace this CastExpr with its inner expression. Recurse into
	// the replacement so nested casts (rare but possible:
	// `c.oid::regclass::text`) collapse fully.
	return true, cast.Expr, nil
}
