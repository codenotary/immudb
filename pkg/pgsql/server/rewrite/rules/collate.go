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

// StripCollate unwraps every COLLATE expression, replacing it with
// its inner expression. immudb has no locale / collation concept;
// every COLLATE clause is a no-op for our engine, and psql / pgAdmin
// emit plenty of them in catalog queries (`relname COLLATE "default"`
// in `\d`).
//
// Replaces this regex at query_machine.go line 488:
//
//	`\s+COLLATE\s+(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)`
//
// Limitation: the Collate-qualifier form `COLLATE pg_catalog.default`
// doesn't parse (auxten rejects the dotted identifier). When that
// input appears, the Rewriter.Rewrite parse step fails and the
// caller falls back to the legacy regex chain, which does handle
// the dotted form. That's acceptable for B1 — a B2 enhancement
// could pre-strip the `pg_catalog.` prefix before parsing.
type StripCollate struct{}

// Name implements rewrite.Rule.
func (StripCollate) Name() string { return "StripCollate" }

// Apply implements rewrite.Rule.
func (s StripCollate) Apply(stmt tree.Statement) tree.Statement {
	return walkStatementExprs(stmt, stripCollateVisitFn)
}

func stripCollateVisitFn(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
	coll, ok := expr.(*tree.CollateExpr)
	if !ok {
		return true, expr, nil
	}
	// Recurse into the inner expression so `('x' COLLATE "C") COLLATE
	// "default"` collapses in one pass.
	return true, coll.Expr, nil
}
