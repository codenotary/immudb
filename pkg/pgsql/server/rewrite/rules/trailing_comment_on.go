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

// StripTrailingCommentOn drops `COMMENT ON …` statements from a
// multi-statement SQL string. pg_dump and JDBC ORMs append
// `; COMMENT ON TABLE t IS 'foo'` after DDL; immudb's engine has no
// object-comment support, and the standalone COMMENT ON form is
// already in the DDL blacklist — but the trailing form survives
// the per-statement blacklist check because the whole string is
// parsed in one Parse call.
//
// The rule returns nil for any CommentOn* statement, which the
// Rewriter uses to drop the statement from the emitted SQL.
//
// Replaces the regex at query_machine.go:555:
//
//	{regexp.MustCompile(`(?is);\s*COMMENT\s+ON\s+[^;]+(?:;|$)`), ""}
type StripTrailingCommentOn struct{}

// Name implements rewrite.Rule.
func (StripTrailingCommentOn) Name() string { return "StripTrailingCommentOn" }

// Apply implements rewrite.Rule. Returning nil signals the Rewriter
// to drop this Statement from the emitted SQL.
func (StripTrailingCommentOn) Apply(stmt tree.Statement) tree.Statement {
	switch stmt.(type) {
	case *tree.CommentOnTable,
		*tree.CommentOnColumn,
		*tree.CommentOnDatabase,
		*tree.CommentOnIndex:
		return nil
	}
	return stmt
}
