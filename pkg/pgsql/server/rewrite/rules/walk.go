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

// walkStatementExprs applies preFn to every Expr reachable through
// the common Statement shapes — SELECT target lists, WHERE clauses,
// ORDER BY / GROUP BY keys, INSERT values, UPDATE SET clauses, and
// DELETE WHERE clauses.
//
// The auxten parser exposes a private walkStmt that would do this
// for us, but its WalkableStmt interface is unexported. Rather than
// fork the walker, we spell out the shapes we care about for B1 —
// SELECT / INSERT / UPDATE / DELETE / CREATE TABLE column defaults.
// Unhandled Statement types pass through unchanged; that's safe
// because the rewriter's fallback path (legacy regex chain) catches
// anything the AST path can't rewrite.
func walkStatementExprs(stmt tree.Statement, preFn tree.SimpleVisitFn) tree.Statement {
	visit := func(e tree.Expr) tree.Expr {
		if e == nil {
			return nil
		}
		out, err := tree.SimpleVisit(e, preFn)
		if err != nil || out == nil {
			return e
		}
		return out
	}

	switch s := stmt.(type) {
	case *tree.Select:
		walkSelect(s, visit)
	case *tree.Insert:
		walkInsert(s, visit)
	case *tree.Update:
		walkUpdate(s, visit)
	case *tree.Delete:
		walkDelete(s, visit)
	case *tree.CreateTable:
		walkCreateTable(s, visit)
	}
	return stmt
}

func walkSelect(s *tree.Select, visit func(tree.Expr) tree.Expr) {
	switch inner := s.Select.(type) {
	case *tree.SelectClause:
		walkSelectClause(inner, visit)
	case *tree.UnionClause:
		if l, ok := inner.Left.Select.(*tree.SelectClause); ok {
			walkSelectClause(l, visit)
		}
		if r, ok := inner.Right.Select.(*tree.SelectClause); ok {
			walkSelectClause(r, visit)
		}
	case *tree.ValuesClause:
		for i, row := range inner.Rows {
			inner.Rows[i] = walkExprSlice(row, visit)
		}
	}
	// ORDER BY / LIMIT aren't inspected for Expr rewrites in the B1
	// rule set — none of the three POC rules rewrite them. Left
	// here as a TODO marker for B2.
}

func walkSelectClause(s *tree.SelectClause, visit func(tree.Expr) tree.Expr) {
	for i := range s.Exprs {
		s.Exprs[i].Expr = visit(s.Exprs[i].Expr)
	}
	if s.Where != nil {
		s.Where.Expr = visit(s.Where.Expr)
	}
	if s.Having != nil {
		s.Having.Expr = visit(s.Having.Expr)
	}
	// FROM sub-exprs (table references, JOIN ON predicates): the
	// POC rules don't rewrite these directly (StripPGCasts /
	// StripCollate only matter in projections and predicates;
	// StripSchemaQualifier uses its own type switch). Skipping
	// FROM-expr recursion keeps this helper simple; B2 rules that
	// need it (e.g. correlated subqueries in WHERE) will extend
	// this function.
}

func walkInsert(s *tree.Insert, visit func(tree.Expr) tree.Expr) {
	if s.Rows != nil {
		// Rows is itself a *Select; recurse.
		walkSelect(s.Rows, visit)
	}
}

func walkUpdate(s *tree.Update, visit func(tree.Expr) tree.Expr) {
	for i := range s.Exprs {
		if s.Exprs[i].Expr != nil {
			s.Exprs[i].Expr = visit(s.Exprs[i].Expr)
		}
	}
	if s.Where != nil {
		s.Where.Expr = visit(s.Where.Expr)
	}
}

func walkDelete(s *tree.Delete, visit func(tree.Expr) tree.Expr) {
	if s.Where != nil {
		s.Where.Expr = visit(s.Where.Expr)
	}
}

func walkCreateTable(s *tree.CreateTable, visit func(tree.Expr) tree.Expr) {
	// CREATE TABLE has column DEFAULT expressions that may contain
	// PG casts or COLLATE. Walk them.
	for _, def := range s.Defs {
		cd, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		if cd.DefaultExpr.Expr != nil {
			cd.DefaultExpr.Expr = visit(cd.DefaultExpr.Expr)
		}
	}
}

func walkExprSlice(exprs []tree.Expr, visit func(tree.Expr) tree.Expr) []tree.Expr {
	for i, e := range exprs {
		exprs[i] = visit(e)
	}
	return exprs
}
