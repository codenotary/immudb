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

package rewrite

import (
	"strings"
	"testing"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

// TestRewriter_NoRulesRoundtrips pins the baseline contract: a
// Rewriter with no rules parses the input and deparses it, yielding
// a semantically identical statement. This is the canary that
// catches parser regressions — if auxten ever starts rejecting a
// previously-accepted input we'll see it here first.
func TestRewriter_NoRulesRoundtrips(t *testing.T) {
	cases := []string{
		`SELECT 1`,
		`SELECT * FROM t WHERE a = 1`,
		`SELECT c.oid, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`,
		`CREATE TABLE t (id INTEGER, name TEXT)`,
	}

	r := New()
	for _, sql := range cases {
		out, err := r.Rewrite(sql)
		require.NoError(t, err, "parse should succeed for %q", sql)
		require.NotEmpty(t, out)
	}
}

// TestRewriter_ParseFailureReturnsInput documents the fallback
// contract: when auxten can't parse a statement (typically because
// the input uses an immudb-specific grammar extension like
// AUTO_INCREMENT or VARCHAR[N] that CockroachDB's parser doesn't
// know), Rewrite returns the input unchanged alongside the error.
// The caller (query_machine.go dispatch) uses this signal to fall
// back to the legacy regex rewriter.
func TestRewriter_ParseFailureReturnsInput(t *testing.T) {
	// Intentionally malformed SQL — we want to confirm the error
	// pathway, not pin a specific parser diagnostic.
	sql := `SELECT FROM WHERE`

	r := New()
	out, err := r.Rewrite(sql)
	require.Error(t, err, "parser should reject malformed SQL")
	require.Equal(t, sql, out, "on parse error the input must be returned unchanged")
}

// TestRewriter_AppliesRulesInOrder exercises the rule-pipeline
// contract: rules run in registration order, each sees the AST as
// the previous rule left it. Uses a pair of trivial counting rules
// to avoid coupling to any specific transform's implementation.
func TestRewriter_AppliesRulesInOrder(t *testing.T) {
	var trace []string
	tracker := func(name string) Rule {
		return &traceRule{name: name, onApply: func() { trace = append(trace, name) }}
	}

	r := New().
		WithRule(tracker("first")).
		WithRule(tracker("second"))

	_, err := r.Rewrite(`SELECT 1`)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, trace)
}

// TestRewriter_RulesReadonlySnapshot guards against rule-list
// corruption from tests or callers that iterate Rules() and mutate
// the slice. A defensive copy at the Rules() boundary means the
// public API is safe even if call sites behave badly.
func TestRewriter_RulesReadonlySnapshot(t *testing.T) {
	r := New().WithRule(&traceRule{name: "r1"})
	snap := r.Rules()
	require.Len(t, snap, 1)

	// Zero out the returned slice — must not affect the rewriter.
	snap[0] = nil
	require.NotNil(t, r.Rules()[0], "Rules() must return a defensive copy")
}

// TestCanParse asserts the parser wrapper correctly reports
// parse-ability for representative inputs. Used by the fidelity
// corpus test and the feature-flag fallback guard.
func TestCanParse(t *testing.T) {
	require.True(t, CanParse(`SELECT 1`))
	require.True(t, CanParse(`SELECT * FROM pg_catalog.pg_class`))
	require.False(t, CanParse(`this is not sql`))
	// Empty string is a zero-statement parse — not an error. That's
	// the auxten contract; callers should check for empty rather
	// than relying on CanParse to reject it.
	require.True(t, CanParse(``))
}

// TestRewriter_RoundtripPreservesFromClause guards against a subtle
// deparse bug: auxten formats identifiers with or without quotes
// based on context. A FROM target must survive the round trip or
// every SQL rewrite is silently buggy.
func TestRewriter_RoundtripPreservesFromClause(t *testing.T) {
	r := New()
	out, err := r.Rewrite(`SELECT * FROM users`)
	require.NoError(t, err)
	require.True(t, strings.Contains(strings.ToLower(out), "from users"),
		"expected FROM users in deparsed output, got %q", out)
}

// traceRule is a test helper that records its Apply invocation.
type traceRule struct {
	name    string
	onApply func()
}

func (r *traceRule) Name() string { return r.name }
func (r *traceRule) Apply(stmt tree.Statement) tree.Statement {
	if r.onApply != nil {
		r.onApply()
	}
	return stmt
}
