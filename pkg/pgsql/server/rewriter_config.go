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
	"strings"
	"sync/atomic"

	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite"
	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite/rules"
)

// The SQL rewriter mode selects between the legacy regex chain and
// the Part B AST-based rewriter. Kept as a package-level atomic so
// every caller of removePGCatalogReferences (tests, wire handlers,
// COPY subflow) reads a consistent value without having to thread
// the option through every signature.
//
// Values:
//   - "regex" (default): use the legacy pgTypeReplacements chain.
//   - "ast":             try the AST path first, fall back to regex
//                        on parse failure.
//
// Other values are treated as "regex". The feature flag is
// reversible at runtime — flipping it between requests is safe
// because the rewriter holds no per-session state.
var sqlRewriterModeValue atomic.Value

// DefaultSQLRewriterMode is the compiled-in default. Can be
// overridden via WithSQLRewriter(...) at server construction.
const DefaultSQLRewriterMode = "regex"

// SQLRewriter is a server option that selects the rewriter mode.
// Passing an unknown value is a no-op (default regex stays in
// effect); we don't panic so the server keeps starting if a config
// file carries a typo'd value.
func SQLRewriter(mode string) Option {
	return func(_ *pgsrv) {
		setSQLRewriterMode(mode)
	}
}

// setSQLRewriterMode writes the mode to the atomic cell. Exposed
// as lowercase so tests in the same package can flip it without
// constructing a full server.
func setSQLRewriterMode(mode string) {
	m := strings.ToLower(strings.TrimSpace(mode))
	switch m {
	case "ast", "regex":
		sqlRewriterModeValue.Store(m)
	default:
		sqlRewriterModeValue.Store(DefaultSQLRewriterMode)
	}
}

// sqlRewriterMode reads the current mode. Returns the compiled-in
// default if the atomic hasn't been populated yet (which is the
// case for any binary that hasn't constructed a server).
func sqlRewriterMode() string {
	v := sqlRewriterModeValue.Load()
	if v == nil {
		return DefaultSQLRewriterMode
	}
	s, _ := v.(string)
	if s == "" {
		return DefaultSQLRewriterMode
	}
	return s
}

// astRewriterSingleton is the pre-constructed pipeline used by the
// "ast" mode. Building it once at package init rather than per-call
// avoids O(n) rule-struct allocation on every query.
var astRewriterSingleton = rewrite.New().
	WithRule(rules.StripSchemaQualifier{}).
	WithRule(rules.StripPGCasts{}).
	WithRule(rules.StripCollate{}).
	WithRule(rules.NormalizeCountOne{})

// astRewrite runs the B1 AST rewriter on a single SQL string.
// Returns (output, true) on success; (input, false) when the parser
// fails or when the output is empty (e.g. comment-only input). The
// caller uses the bool to decide whether to fall through to the
// regex chain.
//
// Why not merge the AST rules into the regex chain? Two reasons:
// (1) the AST path is a pure function of (rules, parser) and can
// be swapped out wholesale in B3 without touching callers; (2) the
// boolean return makes the A/B comparison possible in integration
// tests.
func astRewrite(sql string) (string, bool) {
	out, err := astRewriterSingleton.Rewrite(sql)
	if err != nil {
		return sql, false
	}
	if out == "" {
		return sql, false
	}
	return out, true
}
