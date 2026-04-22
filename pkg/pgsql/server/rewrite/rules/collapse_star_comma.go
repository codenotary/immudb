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
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

// CollapseStarCommaList reduces a SELECT target list that contains
// a bare `*` together with other comma-separated items to just the
// bare `*`. XORM's query builder emits shapes like
//
//	SELECT project.*, project_issue.issue_id FROM project …
//
// which the StripTableStarPrefix rule above already folds to
//
//	SELECT *, project_issue.issue_id FROM project …
//
// immudb's grammar rejects the mixed list (the `opt_targets`
// production is either bare `*` or a comma-separated expression
// list, never a mix). Because `SELECT *` over a JOIN already
// yields every column, the trailing list is redundant.
//
// Replaces the regex at query_machine.go:616:
//
//	{regexp.MustCompile(`(?is)(SELECT\s+\*)(?:\s*,[^,]+?)+(\s+FROM\b)`), "$1$2"}
//
// The AST version is safer: it identifies UnqualifiedStar nodes by
// type (not string-matching on `*`) and filters the target list
// in place, preserving any other top-level clauses.
type CollapseStarCommaList struct{}

// Name implements rewrite.Rule.
func (CollapseStarCommaList) Name() string { return "CollapseStarCommaList" }

// Apply implements rewrite.Rule.
func (CollapseStarCommaList) Apply(stmt tree.Statement) tree.Statement {
	sel, ok := stmt.(*tree.Select)
	if !ok {
		return stmt
	}
	collapseStarInSelect(sel)
	return sel
}

func collapseStarInSelect(s *tree.Select) {
	if s == nil {
		return
	}
	switch inner := s.Select.(type) {
	case *tree.SelectClause:
		collapseStarInSelectClause(inner)
	case *tree.UnionClause:
		collapseStarInSelect(inner.Left)
		collapseStarInSelect(inner.Right)
	}
}

func collapseStarInSelectClause(s *tree.SelectClause) {
	hasStar := false
	for _, e := range s.Exprs {
		if _, ok := e.Expr.(tree.UnqualifiedStar); ok {
			hasStar = true
			break
		}
	}
	if !hasStar || len(s.Exprs) < 2 {
		return
	}
	// Keep only the first UnqualifiedStar, drop the rest.
	s.Exprs = tree.SelectExprs{{Expr: tree.UnqualifiedStar{}}}
}
