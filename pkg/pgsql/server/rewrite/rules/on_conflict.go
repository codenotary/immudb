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

// StripOnConflictColumns drops the column-list qualifier from
// `ON CONFLICT (col_list) DO …` clauses in INSERT statements. Rails'
// `create_or_find_by!` and `upsert_all` emit the PG form; immudb's
// grammar accepts only the column-less form
// (`ON CONFLICT DO NOTHING` / `ON CONFLICT DO UPDATE SET`), so the
// list has to go.
//
// The resulting statement has the same intent — skip or update on
// any conflict against an available index. Rails call sites never
// depend on conflict detection being scoped to the listed columns
// because immudb uses the primary key as the conflict arbiter.
//
// Replaces the regex at query_machine.go:648:
//
//	{regexp.MustCompile(`(?i)\bON\s+CONFLICT\s*\([^)]*\)\s*(DO)\b`),
//	    "ON CONFLICT $1"}
type StripOnConflictColumns struct{}

// Name implements rewrite.Rule.
func (StripOnConflictColumns) Name() string { return "StripOnConflictColumns" }

// Apply implements rewrite.Rule.
func (StripOnConflictColumns) Apply(stmt tree.Statement) tree.Statement {
	ins, ok := stmt.(*tree.Insert)
	if !ok {
		return stmt
	}
	if ins.OnConflict == nil {
		return stmt
	}
	// Zero the column list. The rest of OnConflict (DoNothing /
	// Exprs / Where) is preserved — we only drop the qualifier, not
	// the clause.
	ins.OnConflict.Columns = nil
	return ins
}
