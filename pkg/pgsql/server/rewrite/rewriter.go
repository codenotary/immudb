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

// Package rewrite is the AST-based SQL rewriter that replaces the
// regex chain in pkg/pgsql/server/query_machine.go (removePGCatalogReferences +
// pgTypeReplacements). See docs/pg-compat-roadmap.md Part B for the
// programme; this is the B1 scaffold with 3 proof-of-concept rules.
//
// The rewriter is currently additive: Rewrite() parses SQL with
// github.com/auxten/postgresql-parser, walks the AST applying the
// registered Rules in order, then deparses back to SQL. On any parse
// failure it returns the input string unchanged along with the parse
// error so callers can fall back to the legacy regex chain.
package rewrite

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

// Rule is a single AST-level transformation. Name() is used for
// logging and for the (future) B2 dependency graph; Apply rewrites the
// statement in place OR returns a copy — semantics follow the auxten
// Visitor contract, which means "may mutate" in practice.
type Rule interface {
	Name() string
	// Apply transforms a single parsed statement. The statement may
	// be mutated in place; returning it is purely a convenience for
	// chaining. Implementations should use tree.Walk / tree.WalkExpr
	// to traverse; building Statement-level visitors from scratch is
	// error-prone.
	Apply(stmt tree.Statement) tree.Statement
}

// Rewriter applies an ordered slice of Rules to each input statement.
// Construction is via New(); callers register Rules with WithRule().
// The Rewrite() method is safe for concurrent use: it holds no
// mutable state across calls.
type Rewriter struct {
	rules []Rule
}

// New returns a Rewriter with no rules installed. Use WithRule(s) to
// populate it. The zero-rule Rewriter is a valid parse-and-redeparse
// pass, which is occasionally useful for normalising whitespace.
func New() *Rewriter {
	return &Rewriter{}
}

// WithRule appends a rule to the pipeline. Rules run in the order
// they were added. B2 may add topological-sort logic if rule
// dependencies become explicit, but B1 relies on fixed ordering from
// the call site.
func (r *Rewriter) WithRule(rule Rule) *Rewriter {
	r.rules = append(r.rules, rule)
	return r
}

// Rules returns the ordered rule list. Primarily for tests and
// diagnostic logging.
func (r *Rewriter) Rules() []Rule {
	out := make([]Rule, len(r.rules))
	copy(out, r.rules)
	return out
}

// Rewrite parses sql, applies every registered rule to every parsed
// statement, and emits a canonical PG-dialect string. When parsing
// fails, returns the input unchanged along with the parse error —
// callers (currently query_machine.go) use that signal to fall back
// to the legacy regex chain.
//
// Rewriting is deliberately a pure function of (sql, rules): no
// session state, no catalog lookups. Rules that need catalog
// information take it via constructor arguments, not via the
// Rewriter.
func (r *Rewriter) Rewrite(sql string) (string, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return sql, err
	}
	for i := range stmts {
		for _, rule := range r.rules {
			stmts[i].AST = rule.Apply(stmts[i].AST)
		}
	}
	return stmts.String(), nil
}
