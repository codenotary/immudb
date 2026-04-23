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

// NormalizeCountOne rewrites `COUNT(1)` to `COUNT(*)`. Postgres and
// MySQL treat the two forms as identical (row-count aggregation),
// but immudb's aggregate-func grammar rejects an integer literal as
// the argument with "unexpected INTEGER_LIT at position N".
//
// XORM / Gitea (QueryIssueContentHistoryEditedCountMap) emits this
// shape routinely:
//
//	SELECT comment_id, COUNT(1) as history_count … HAVING count(1) > 1
//
// Replaces the regex rule at query_machine.go line 625:
//
//	{regexp.MustCompile(`(?i)\bCOUNT\s*\(\s*1\s*\)`), "COUNT(*)"}
//
// The AST version is safer than the regex: it only rewrites
// FuncExpr nodes whose function is COUNT, never touching an
// identifier or string that happens to contain "count(1)".
type NormalizeCountOne struct{}

// Name implements rewrite.Rule.
func (NormalizeCountOne) Name() string { return "NormalizeCountOne" }

// Apply implements rewrite.Rule.
func (NormalizeCountOne) Apply(stmt tree.Statement) tree.Statement {
	return walkStatementExprs(stmt, normalizeCountOneVisitFn)
}

func normalizeCountOneVisitFn(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
	fn, ok := expr.(*tree.FuncExpr)
	if !ok {
		return true, expr, nil
	}
	// FunctionReference names survive parsing unlowered for quoted
	// forms; match case-insensitively against the canonical name.
	if !strings.EqualFold(fn.Func.String(), "count") {
		return true, expr, nil
	}
	if len(fn.Exprs) != 1 {
		return true, expr, nil
	}
	// Match a bare integer literal equal to 1. Both NumVal (parser
	// output) and DInt (constant-folded) can appear depending on the
	// call site.
	if !isLiteralOne(fn.Exprs[0]) {
		return true, expr, nil
	}
	// Clone the FuncExpr so we don't mutate a shared node, then
	// replace the argument list with a bare `*`.
	fnCopy := *fn
	fnCopy.Exprs = tree.Exprs{tree.UnqualifiedStar{}}
	return true, &fnCopy, nil
}

// isLiteralOne returns true if expr is a numeric literal equal to 1.
// Kept narrow on purpose — COUNT('1'), COUNT(1.0), COUNT(TRUE) are
// semantically distinct or already handled elsewhere, and we only
// want to rewrite the exact XORM/Gitea shape.
func isLiteralOne(e tree.Expr) bool {
	switch v := e.(type) {
	case *tree.NumVal:
		s := v.String()
		return s == "1"
	case *tree.DInt:
		return int64(*v) == 1
	}
	return false
}
