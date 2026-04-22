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

// StripCreateViewColList drops the PG-optional column-alias list
// from `CREATE VIEW name(col1, col2, …) AS SELECT …`. immudb's
// grammar (sql_grammar.y:355-360) accepts only
// `CREATE VIEW <name> AS dqlstmt` without the outer rename list.
//
// Replaces the regex at query_machine.go:714:
//
//	{regexp.MustCompile(`(?is)\bCREATE\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[A-Za-z_]\w*)\s*\([^)]*\)\s+AS\b`),
//	    "CREATE VIEW ${1}${2} AS"}
//
// Semantics: the outer list renames the SELECT's output columns.
// pg_dump emits both the list AND matching AS aliases in the
// SELECT, so dropping the list preserves names for the common
// case. Clients that depend on the rename-only behaviour (rare in
// practice) will see the inner SELECT's column names instead.
type StripCreateViewColList struct{}

// Name implements rewrite.Rule.
func (StripCreateViewColList) Name() string { return "StripCreateViewColList" }

// Apply implements rewrite.Rule.
func (StripCreateViewColList) Apply(stmt tree.Statement) tree.Statement {
	view, ok := stmt.(*tree.CreateView)
	if !ok {
		return stmt
	}
	view.ColumnNames = nil
	return view
}
