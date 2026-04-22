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

package rules_test

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite"
	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite/rules"
	"github.com/stretchr/testify/require"
)

// TestStripSchemaQualifier_PgCatalog pins the pg_catalog prefix
// removal — the single highest-traffic rewrite since every psql
// backslash command emits `pg_catalog.<table>` references.
func TestStripSchemaQualifier_PgCatalog(t *testing.T) {
	cases := []struct {
		in   string
		want string // substring that must appear
	}{
		{`SELECT * FROM pg_catalog.pg_class`, "FROM pg_class"},
		{`SELECT c.oid FROM pg_catalog.pg_class c`, "FROM pg_class"},
		{`SELECT * FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.oid = n.oid`,
			"pg_namespace"},
	}

	r := rewrite.New().WithRule(rules.StripSchemaQualifier{})
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := r.Rewrite(tc.in)
			require.NoError(t, err)
			require.Contains(t, out, tc.want, "out=%q", out)
			require.NotContains(t, out, "pg_catalog.",
				"pg_catalog. prefix must be stripped: %q", out)
		})
	}
}

// TestStripSchemaQualifier_Public pins the `public.` prefix drop,
// matching the third ReplaceAll at query_machine.go:981.
func TestStripSchemaQualifier_Public(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripSchemaQualifier{})
	out, err := r.Rewrite(`SELECT * FROM public.users`)
	require.NoError(t, err)
	require.Contains(t, out, "FROM users")
	require.NotContains(t, out, "public.")
}

// TestStripSchemaQualifier_InformationSchema pins the rewrite from
// `information_schema.tables` → `information_schema_tables`,
// matching the sys/ registry's naming convention and the
// ReplaceAll at query_machine.go:980.
func TestStripSchemaQualifier_InformationSchema(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripSchemaQualifier{})
	out, err := r.Rewrite(`SELECT * FROM information_schema.tables`)
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(out), "information_schema_tables",
		"out=%q", out)
	require.NotContains(t, strings.ToLower(out), "information_schema.",
		"dotted form must be rewritten to underscored: %q", out)
}

// TestStripSchemaQualifier_UnknownSchemaPreserved guards against
// over-stripping. An unknown schema name (immudb doesn't have user
// schemas but a client might emit one anyway) must pass through so
// the engine surfaces a clear "schema does not exist" error rather
// than silently collapsing the reference.
func TestStripSchemaQualifier_UnknownSchemaPreserved(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripSchemaQualifier{})
	out, err := r.Rewrite(`SELECT * FROM someschema.footable`)
	require.NoError(t, err)
	require.Contains(t, out, "someschema",
		"unknown schema qualifier must survive: %q", out)
}

// TestStripSchemaQualifier_JoinBothSides covers the JOIN-both-sides
// case — psql `\d` first-roundtrip JOINs pg_class on pg_namespace;
// both prefixes must strip.
func TestStripSchemaQualifier_JoinBothSides(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripSchemaQualifier{})
	sql := `SELECT c.oid, n.nspname, c.relname
	        FROM pg_catalog.pg_class c
	        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`

	out, err := r.Rewrite(sql)
	require.NoError(t, err)
	require.Contains(t, out, "pg_class")
	require.Contains(t, out, "pg_namespace")
	require.NotContains(t, out, "pg_catalog.")
}
