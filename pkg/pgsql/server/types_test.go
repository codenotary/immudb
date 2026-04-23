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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func Test_getInt64(t *testing.T) {
	b64i := make([]byte, 8)
	binary.BigEndian.PutUint64(b64i, 1)
	i, err := getInt64(b64i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)
	b32i := make([]byte, 4)
	binary.BigEndian.PutUint32(b32i, 1)
	i, err = getInt64(b32i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)
	b16i := make([]byte, 2)
	binary.BigEndian.PutUint16(b16i, 1)
	i, err = getInt64(b16i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)

	bxxx := make([]byte, 64)
	_, err = getInt64(bxxx)
	require.ErrorContains(t, err, fmt.Sprintf("cannot convert a slice of %d byte in an INTEGER parameter", len(bxxx)))
}

func Test_buildNamedParams(t *testing.T) {
	// integer error
	cols := []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "INTEGER",
		},
	}
	pt := []interface{}{[]byte(`1`)}
	_, err := buildNamedParams(cols, pt)
	require.ErrorContains(t, err, fmt.Sprintf("cannot convert a slice of %d byte in an INTEGER parameter", len(cols)))

	// varchar error
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "VARCHAR",
		},
	}
	pt = []interface{}{[]byte(`1`)}
	_, err = buildNamedParams(cols, pt)
	require.NoError(t, err)

	// blob
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "BLOB",
		},
	}
	pt = []interface{}{[]byte(`1`)}
	_, err = buildNamedParams(cols, pt)
	require.NoError(t, err)

	// blob text error
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "BLOB",
		},
	}
	pt = []interface{}{"blob"}
	_, err = buildNamedParams(cols, pt)
	require.ErrorIs(t, err, hex.InvalidByteError(108))
}

// paramVal pulls the typed Go value out of a NamedParam list for inspection.
func paramVal(t *testing.T, params []*schema.NamedParam, name string) interface{} {
	t.Helper()
	for _, p := range params {
		if p.Name == name {
			switch v := p.Value.GetValue().(type) {
			case *schema.SQLValue_N:
				return v.N
			case *schema.SQLValue_S:
				return v.S
			case *schema.SQLValue_B:
				return v.B
			case *schema.SQLValue_F:
				return v.F
			case *schema.SQLValue_Ts:
				return v.Ts
			case *schema.SQLValue_Bs:
				return v.Bs
			case *schema.SQLValue_Null:
				return nil
			default:
				t.Fatalf("unexpected SQLValue type %T", v)
			}
		}
	}
	t.Fatalf("param %q not found in %+v", name, params)
	return nil
}

// Test_buildNamedParams_BooleanText covers the PG text-format boolean
// values that buildNamedParams must accept (regression for the
// 14f64047 fix where only "true" matched and every "t"/"f" bind from the
// pg gem silently wrote false).
func Test_buildNamedParams_BooleanText(t *testing.T) {
	cols := []sql.ColDescriptor{{Column: "b", Type: sql.BooleanType}}

	cases := map[string]bool{
		"t": true, "true": true, "T": true, "TRUE": true,
		"y": true, "yes": true, "on": true, "1": true,
		"f": false, "false": false, "F": false, "FALSE": false,
		"n": false, "no": false, "off": false, "0": false,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			params, err := buildNamedParams(cols, []interface{}{in})
			require.NoError(t, err)
			require.Equal(t, want, paramVal(t, params, "b"))
		})
	}

	// Anything else must error rather than silently coerce — silent
	// coercion to false is what hid the original bug.
	for _, bad := range []string{"", "maybe", "2", "tt"} {
		t.Run("invalid_"+bad, func(t *testing.T) {
			_, err := buildNamedParams(cols, []interface{}{bad})
			require.Error(t, err, "input %q should be rejected", bad)
		})
	}
}

// Test_buildNamedParams_TimestampText covers parsing of PG text-format
// timestamps. Previously the value fell through to the default branch
// as a raw string and the engine kept only the leading year.
func Test_buildNamedParams_TimestampText(t *testing.T) {
	cols := []sql.ColDescriptor{{Column: "ts", Type: sql.TimestampType}}

	want := time.Date(2026, 4, 14, 8, 28, 52, 218224000, time.UTC)

	for _, in := range []string{
		"2026-04-14 08:28:52.218224",
		"2026-04-14 08:28:52.218224000",
		"2026-04-14T08:28:52.218224Z",
	} {
		t.Run(in, func(t *testing.T) {
			params, err := buildNamedParams(cols, []interface{}{in})
			require.NoError(t, err)
			gotTs := paramVal(t, params, "ts").(int64)
			require.Equal(t, sql.TimeToInt64(want), gotTs)
		})
	}

	t.Run("date_only", func(t *testing.T) {
		params, err := buildNamedParams(cols, []interface{}{"2026-04-14"})
		require.NoError(t, err)
		require.NotZero(t, paramVal(t, params, "ts").(int64))
	})

	t.Run("garbage_rejected", func(t *testing.T) {
		_, err := buildNamedParams(cols, []interface{}{"not a date"})
		require.Error(t, err)
	})
}

// Test_buildNamedParams_FloatText covers the new float text-bind path.
// Without it, a textual "3.14" fell through to the default branch and
// the engine never received a float value.
func Test_buildNamedParams_FloatText(t *testing.T) {
	cols := []sql.ColDescriptor{{Column: "f", Type: sql.Float64Type}}

	for _, tc := range []struct {
		in   string
		want float64
	}{
		{"0", 0},
		{"3.14", 3.14},
		{"-1.5e2", -150},
		{"1e10", 1e10},
	} {
		t.Run(tc.in, func(t *testing.T) {
			params, err := buildNamedParams(cols, []interface{}{tc.in})
			require.NoError(t, err)
			require.Equal(t, tc.want, paramVal(t, params, "f"))
		})
	}

	_, err := buildNamedParams(cols, []interface{}{"not-a-number"})
	require.Error(t, err)
}

func Test_pgTextBool(t *testing.T) {
	for _, in := range []string{"t", "T", "true", "TRUE", "y", "yes", "on", "1"} {
		v, ok := pgTextBool(in)
		require.True(t, ok)
		require.True(t, v)
	}
	for _, in := range []string{"f", "F", "false", "FALSE", "n", "no", "off", "0"} {
		v, ok := pgTextBool(in)
		require.True(t, ok)
		require.False(t, v)
	}
	for _, in := range []string{"", "maybe", "2", " ", "tt"} {
		_, ok := pgTextBool(in)
		// whitespace is trimmed; "  " becomes "" which is rejected
		require.False(t, ok, "should reject %q", in)
	}
}

func Test_pgTextTimestamp(t *testing.T) {
	want := time.Date(2026, 4, 14, 8, 28, 52, 0, time.UTC)
	for _, in := range []string{
		"2026-04-14 08:28:52",
		"2026-04-14T08:28:52Z",
	} {
		got, ok := pgTextTimestamp(in)
		require.True(t, ok, "input %q", in)
		require.True(t, got.Equal(want), "got=%v want=%v", got, want)
	}
	_, ok := pgTextTimestamp("not a date")
	require.False(t, ok)
}
