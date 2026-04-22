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

// StripCheckConstraints removes all CHECK constraints from a CREATE
// TABLE statement — both column-inline forms (`col INT CHECK (col > 0)`)
// and table-level forms (`CONSTRAINT name CHECK (…)`). immudb's SQL
// engine doesn't enforce CHECK constraints, so clients that emit
// them (pg_dump, Rails, XORM, pgAdmin) would fail at parse time
// without this rule.
//
// Replaces the regex at query_machine.go:666:
//
//	{regexp.MustCompile(`(?i)\bCHECK\s*\([^)]*\)`), ""}
//
// The regex is greedy-naive — it only matches single-paren CHECK
// expressions. The AST version correctly handles nested parens in
// the check expression because the parser has already resolved the
// grouping for us.
type StripCheckConstraints struct{}

// Name implements rewrite.Rule.
func (StripCheckConstraints) Name() string { return "StripCheckConstraints" }

// Apply implements rewrite.Rule.
func (StripCheckConstraints) Apply(stmt tree.Statement) tree.Statement {
	ct, ok := stmt.(*tree.CreateTable)
	if !ok {
		return stmt
	}
	kept := ct.Defs[:0]
	for _, def := range ct.Defs {
		// Table-level CHECK: drop the entire definition.
		if _, isCheck := def.(*tree.CheckConstraintTableDef); isCheck {
			continue
		}
		// Column-level CHECK: clear the CheckExprs slice but keep
		// the column itself.
		if col, ok := def.(*tree.ColumnTableDef); ok && len(col.CheckExprs) > 0 {
			col.CheckExprs = nil
		}
		kept = append(kept, def)
	}
	ct.Defs = kept
	return ct
}
