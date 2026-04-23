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

// StripTransactionModes drops the PG-optional transaction-mode
// suffixes from BEGIN / START TRANSACTION statements. immudb's
// BEGIN grammar rejects READ WRITE / READ ONLY / ISOLATION LEVEL /
// DEFERRABLE qualifiers; its transactions are read/write by default
// with snapshot isolation, so stripping is semantically a no-op
// for correctness-probing clients (psql, pgAdmin, Rails).
//
// Replaces the regex at query_machine.go:563:
//
//	{regexp.MustCompile(`(?i)\b(BEGIN|START\s+TRANSACTION)\s+(READ\s+(?:WRITE|ONLY)|ISOLATION\s+LEVEL\s+\S+(?:\s+\S+)?|DEFERRABLE|NOT\s+DEFERRABLE)`), "BEGIN"}
type StripTransactionModes struct{}

// Name implements rewrite.Rule.
func (StripTransactionModes) Name() string { return "StripTransactionModes" }

// Apply implements rewrite.Rule.
func (StripTransactionModes) Apply(stmt tree.Statement) tree.Statement {
	begin, ok := stmt.(*tree.BeginTransaction)
	if !ok {
		return stmt
	}
	begin.Modes = tree.TransactionModes{}
	return begin
}
