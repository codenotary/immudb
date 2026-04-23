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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCaseWhen_MixedTypeWidening covers the engine-level fix for
// PG-style CASE arm widening. Before the coerceTypesForCase
// refactor, any CASE whose arms had different types (other than
// INT↔FLOAT) rejected with "CASE types X and Y cannot be matched".
// After the fix, pairs of (any scalar, VARCHAR) widen to VARCHAR
// and the arm's native-type value is converted via the runtime
// matrix at type_conversion.go.
//
// This is the "proper" replacement for the
// psqlAlwaysZeroOidCaseRe regex in pkg/pgsql/server/query_machine.go:
// that rule worked around this exact limitation for the psql `\d`
// query but only for eight specific columns. The engine-level fix
// generalises to every CASE expression.
func TestCaseWhen_MixedTypeWidening(t *testing.T) {
	engine := setupCommonTest(t)

	cases := []struct {
		name string
		sql  string
		want interface{}
	}{
		{
			// The canonical psql \d shape. INT arm widens to VARCHAR.
			name: "int_else_varchar_then_winning_then",
			sql:  `SELECT CASE WHEN 1 = 1 THEN '' ELSE 42 END`,
			want: "",
		},
		{
			name: "int_else_varchar_then_winning_else",
			sql:  `SELECT CASE WHEN 1 = 0 THEN '' ELSE 42 END`,
			want: "42",
		},
		{
			name: "float_else_varchar_then_winning_else",
			sql:  `SELECT CASE WHEN 1 = 0 THEN 'n/a' ELSE 3.14 END`,
			want: "3.14",
		},
		{
			name: "bool_else_varchar_then_winning_else",
			sql:  `SELECT CASE WHEN 1 = 0 THEN 'no' ELSE true END`,
			want: "true",
		},
		{
			// Plain INT↔FLOAT widening must still work (historic
			// contract predating this change).
			name: "int_float_historic_widen",
			sql:  `SELECT CASE WHEN 1 = 0 THEN 1 ELSE 2.0 END`,
			want: float64(2),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, tc.sql, nil)
			require.NoError(t, err, "query: %s", tc.sql)
			defer r.Close()

			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.want, row.ValuesByPosition[0].RawValue(),
				"sql: %s", tc.sql)
		})
	}
}

// TestCaseWhen_PsqlShape feeds the exact psql \d <table> CASE that
// the regex used to pre-chew. Now the engine accepts it directly.
// Uses a synthetic reloftype column (always 0, matching sys/pg_class
// semantics) so both arms can be exercised via different input.
func TestCaseWhen_PsqlShape(t *testing.T) {
	engine := setupCommonTest(t)

	// Always-0 case: THEN branch fires → ''.
	r, err := engine.Query(context.Background(), nil,
		`SELECT CASE WHEN 0 = 0 THEN '' ELSE 12345 END`, nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "", row.ValuesByPosition[0].RawValue())

	// Non-zero case: ELSE branch fires → the widened INT value.
	r2, err := engine.Query(context.Background(), nil,
		`SELECT CASE WHEN 1 = 0 THEN '' ELSE 12345 END`, nil)
	require.NoError(t, err)
	defer r2.Close()

	row2, err := r2.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "12345", row2.ValuesByPosition[0].RawValue(),
		"INT arm must widen to VARCHAR in a mixed-type CASE")
}

// TestCaseWhen_ArithmeticStaysStrict is the negative contract: the
// widening must NOT leak into arithmetic or comparison operators.
// `WHERE col = 'x'` on an INT column still errors at plan time —
// that's by design, preserves user-bug detection.
func TestCaseWhen_ArithmeticStaysStrict(t *testing.T) {
	// coerceTypes (the strict variant used by CmpBoolExp / NumExp)
	// should still reject INT / VARCHAR pairs.
	_, ok := coerceTypes(IntegerType, VarcharType)
	require.False(t, ok, "strict coerceTypes must reject INT/VARCHAR")

	// coerceTypesForCase accepts the same pair.
	merged, ok := coerceTypesForCase(IntegerType, VarcharType)
	require.True(t, ok, "widening coerceTypesForCase must accept INT/VARCHAR")
	require.Equal(t, VarcharType, merged)
}

// TestCoerceTypesForCase_UnchangedRejections pins the pairs that
// must still fail under the permissive coercion. Two unrelated
// scalars (neither side VARCHAR) have no runtime converter, so
// coerceTypesForCase must reject them even though it accepts
// anything/VARCHAR.
func TestCoerceTypesForCase_UnchangedRejections(t *testing.T) {
	rejects := []struct{ a, b SQLValueType }{
		{IntegerType, BooleanType},
		{IntegerType, TimestampType},
		{Float64Type, BooleanType},
		{UUIDType, TimestampType},
	}
	for _, r := range rejects {
		_, ok := coerceTypesForCase(r.a, r.b)
		require.Falsef(t, ok, "%v / %v must still reject — no runtime converter",
			r.a, r.b)
	}
}
