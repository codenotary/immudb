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

// StripTableStarPrefix rewrites `table.*` and `schema.table.*` to a
// bare `*`. Rails ActiveRecord's default all-columns projection is
// `SELECT "users".* FROM "users" …`; immudb's grammar has no
// `identifier.*` production (only bare `*`), so stripping the
// prefix keeps AR-generated SQL working.
//
// Replaces this regex at query_machine.go:606:
//
//	{regexp.MustCompile(`(?i)\b[A-Za-z_][A-Za-z0-9_]*\s*\.\s*\*`), "*"}
//
// Trade-off mirrors the regex: for multi-table JOINs, `t1.*, t2.*`
// becomes `*, *` which is technically different but returns the
// same union of columns. Real clients don't hit this (the
// comma-collapse rule below cleans it up), so we accept it.
type StripTableStarPrefix struct{}

// Name implements rewrite.Rule.
func (StripTableStarPrefix) Name() string { return "StripTableStarPrefix" }

// Apply implements rewrite.Rule.
func (StripTableStarPrefix) Apply(stmt tree.Statement) tree.Statement {
	switch s := stmt.(type) {
	case *tree.Select:
		stripTableStarInSelect(s)
	}
	return stmt
}

func stripTableStarInSelect(s *tree.Select) {
	if s == nil {
		return
	}
	switch inner := s.Select.(type) {
	case *tree.SelectClause:
		stripTableStarInSelectClause(inner)
	case *tree.UnionClause:
		stripTableStarInSelect(inner.Left)
		stripTableStarInSelect(inner.Right)
	}
}

func stripTableStarInSelectClause(s *tree.SelectClause) {
	for i := range s.Exprs {
		if isTableQualifiedStar(s.Exprs[i].Expr) {
			s.Exprs[i].Expr = tree.UnqualifiedStar{}
		}
	}
}

// isTableQualifiedStar returns true when expr is an UnresolvedName
// that ends in `.*` AND has at least one name part before the star.
// Bare `*` (already unqualified) and `a.b` (no star) return false.
func isTableQualifiedStar(expr tree.Expr) bool {
	name, ok := expr.(*tree.UnresolvedName)
	if !ok {
		return false
	}
	if !name.Star {
		return false
	}
	// NumParts counts the star too; a qualified star like `t.*`
	// has NumParts=2 (the star + one qualifier). `*` alone is
	// represented by tree.UnqualifiedStar, not UnresolvedName, so
	// we never see NumParts==1 here — but guard just in case.
	return name.NumParts >= 2
}
