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

// StripForeignKeys drops FOREIGN KEY and REFERENCES clauses from
// CREATE TABLE statements. immudb's SQL engine accepts FK syntax
// today but doesn't enforce it; real clients (pg_dump, Rails
// schema.rb, ActiveRecord migrations) emit both forms, and some
// constructions — particularly `FOREIGN KEY (…) REFERENCES …
// ON DELETE CASCADE` — break the grammar when combined with the
// other rewrites.
//
// Replaces two regex rules at query_machine.go:689-691:
//
//	{regexp.MustCompile(`(?i),?\s*\bFOREIGN\s+KEY\s*\([^)]*\)\s*REFERENCES\s+\S+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION))*`), ""}
//	{regexp.MustCompile(`(?i)\bREFERENCES\s+\S+\s*\([^)]*\)(\s+ON\s+(DELETE|UPDATE)\s+(CASCADE|RESTRICT|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION))*`), ""}
//
// The AST version drops:
//   - table-level ForeignKeyConstraintTableDef entries (filter out
//     of Defs)
//   - column-level References (clear the References field on
//     ColumnTableDef)
//
// Column and table survive; only the FK relationship vanishes.
type StripForeignKeys struct{}

// Name implements rewrite.Rule.
func (StripForeignKeys) Name() string { return "StripForeignKeys" }

// Apply implements rewrite.Rule.
func (StripForeignKeys) Apply(stmt tree.Statement) tree.Statement {
	ct, ok := stmt.(*tree.CreateTable)
	if !ok {
		return stmt
	}
	kept := ct.Defs[:0]
	for _, def := range ct.Defs {
		if _, isFK := def.(*tree.ForeignKeyConstraintTableDef); isFK {
			continue
		}
		if col, ok := def.(*tree.ColumnTableDef); ok && col.References.Table != nil {
			// Clear the column-level References block. Zeroing the
			// whole struct keeps the ColumnTableDef otherwise valid
			// and avoids stale ConstraintName / Actions leaking
			// into the deparse.
			col.References.Table = nil
			col.References.Col = ""
			col.References.ConstraintName = ""
			col.References.Actions = tree.ReferenceActions{}
			col.References.Match = tree.MatchSimple
		}
		kept = append(kept, def)
	}
	ct.Defs = kept
	return ct
}
