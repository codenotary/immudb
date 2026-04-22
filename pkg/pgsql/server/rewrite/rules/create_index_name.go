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

// StripCreateIndexName drops the optional index name from
// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON …`. PostgreSQL
// both requires and pg_dump emits a name; immudb's grammar at
// sql_grammar.y:390-408 is `CREATE INDEX ON …` without a name, so
// the name has to go.
//
// Replaces the regex at query_machine.go:701:
//
//	{regexp.MustCompile(`(?i)\bCREATE\s+(UNIQUE\s+)?INDEX\s+(IF\s+NOT\s+EXISTS\s+)?(?:"[^"]+"|[A-Za-z_]\w*)\s+ON\b`),
//	    "CREATE ${1}INDEX ${2}ON"}
//
// The AST version simply zeros CreateIndex.Name. `CREATE INDEX ON …`
// with no name was already legal in both parsers, so this is a
// safe transform in both directions — re-parsing our output never
// gains or loses semantic content.
type StripCreateIndexName struct{}

// Name implements rewrite.Rule.
func (StripCreateIndexName) Name() string { return "StripCreateIndexName" }

// Apply implements rewrite.Rule.
func (StripCreateIndexName) Apply(stmt tree.Statement) tree.Statement {
	idx, ok := stmt.(*tree.CreateIndex)
	if !ok {
		return stmt
	}
	idx.Name = ""
	return idx
}
