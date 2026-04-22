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

// EnsureCreateTableIfNotExists adds `IF NOT EXISTS` to any CREATE
// TABLE that doesn't already have it. Rails's `db:prepare`
// probes pg_class to decide whether to emit the idempotent form;
// with the earlier canned emulation the probe always reported
// "absent" and Rails emitted plain `CREATE TABLE`. On the first run
// that succeeded; on a restart after a successful schema load the
// plain form failed with "table already exists" and aborted the
// whole migration.
//
// Both PG and immudb accept the `IF NOT EXISTS` form, so setting
// the flag unconditionally is a zero-semantic-change operation for
// Rails's control flow.
//
// Replaces the regex at query_machine.go:659:
//
//	{regexp.MustCompile(`(?i)^(\s*CREATE\s+TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>"[^"]+"|\w+)`), "$1 IF NOT EXISTS $2"}
type EnsureCreateTableIfNotExists struct{}

// Name implements rewrite.Rule.
func (EnsureCreateTableIfNotExists) Name() string { return "EnsureCreateTableIfNotExists" }

// Apply implements rewrite.Rule.
func (EnsureCreateTableIfNotExists) Apply(stmt tree.Statement) tree.Statement {
	ct, ok := stmt.(*tree.CreateTable)
	if !ok {
		return stmt
	}
	ct.IfNotExists = true
	return ct
}
