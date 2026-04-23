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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExtractColumnNames_PreservesSelectArity guards against the
// psql-segfault regression: the RowDescription column count must equal
// the SELECT-list arity, otherwise libpq reads past the advertised
// column count and crashes the client (`column number N is out of
// range 0..M` followed by a segfault from a NULL deref).
func TestExtractColumnNames_PreservesSelectArity(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		wantLen    int
		wantExact  []string // if non-nil, exact match required
		wantSubset []string // if non-nil, each must appear somewhere in result
	}{
		{
			// The exact query psql's \d <table> sends on the second
			// round-trip. Has 15 SELECT-list items; two (bare '' and
			// CASE...END) previously got dropped, leaving 13 and
			// triggering the segfault.
			name: "psql_describe_table_second_roundtrip",
			query: `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, ` +
				`c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, ` +
				`c.relispartition, '', c.reltablespace, ` +
				`CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, ` +
				`c.relpersistence, c.relreplident, am.amname ` +
				`FROM pg_catalog.pg_class c ` +
				`LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) ` +
				`LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) ` +
				`WHERE c.oid = '16384';`,
			wantLen: 15,
			wantSubset: []string{
				"relchecks", "relkind", "relhasoids", "relispartition",
				"reltablespace", "relpersistence", "relreplident", "amname",
			},
		},
		{
			name:    "bare_string_literal_gets_placeholder",
			query:   `SELECT a, '', b FROM t;`,
			wantLen: 3,
			wantExact: []string{"a", "?column?", "b"},
		},
		{
			name:    "case_when_end_gets_placeholder_not_dropped",
			query:   `SELECT a, CASE WHEN x = 0 THEN 1 ELSE 2 END, b FROM t;`,
			wantLen: 3,
			wantExact: []string{"a", "?column?", "b"},
		},
		{
			// psql sometimes produces queries where the same inferred
			// name appears twice. The prior dedup silently collapsed
			// these, causing the same crash.
			name:    "duplicate_inferred_names_preserved",
			query:   `SELECT c.name, c.name FROM t c;`,
			wantLen: 2,
			wantExact: []string{"name", "name"},
		},
		{
			name:    "select_star_returns_nil",
			query:   `SELECT * FROM t;`,
			wantLen: 0,
		},
		{
			name:    "as_alias_resolved",
			query:   `SELECT foo AS bar, baz AS qux FROM t;`,
			wantLen: 2,
			wantExact: []string{"bar", "qux"},
		},
		{
			name:    "dotted_prefix_stripped",
			query:   `SELECT c.relname, n.nspname FROM pg_class c;`,
			wantLen: 2,
			wantExact: []string{"relname", "nspname"},
		},
		{
			// First round-trip for \d continent — three simple dotted columns.
			name: "psql_describe_table_first_roundtrip",
			query: `SELECT c.oid, n.nspname, c.relname ` +
				`FROM pg_catalog.pg_class c ` +
				`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
				`WHERE c.relname OPERATOR(pg_catalog.~) '^(continent)$' COLLATE pg_catalog.default ` +
				`AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3;`,
			wantLen: 3,
			wantExact: []string{"oid", "nspname", "relname"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractColumnNames(tc.query)
			require.Equal(t, tc.wantLen, len(got),
				"column count mismatch (libpq will crash psql if this diverges from the SELECT-list arity): got %v", got)
			if tc.wantExact != nil {
				require.Equal(t, tc.wantExact, got)
			}
			for _, want := range tc.wantSubset {
				require.Contains(t, got, want)
			}
		})
	}
}
