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

package rewrite

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
)

// Parse is a thin wrapper over auxten's parser kept in this package
// so call sites (tests, rules, the fidelity corpus) can import a
// single entry point. Using the upstream directly works too; the
// indirection exists to make swapping the parser in B3 (if coverage
// gaps force a move to pg_query_go cgo) a one-file change.
func Parse(sql string) (parser.Statements, error) {
	return parser.Parse(sql)
}

// CanParse returns true when auxten can parse the given SQL string
// without error. Intended for the B1 fidelity corpus test and for
// the feature-flag guard in Rewriter.Rewrite — a query that doesn't
// parse falls back to the legacy regex chain.
func CanParse(sql string) bool {
	_, err := parser.Parse(sql)
	return err == nil
}
