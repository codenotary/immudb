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
	"strings"
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

	// Issue #1149: lib/pq and JDBC's setTimestamp emit space-separated
	// timestamps with a trailing tz suffix when the time.Time carries a
	// location. The original layouts only handled the `T`-separated
	// (RFC3339) form, so binds in the space-separated form were rejected
	// with "invalid timestamp bind value". Verify the variants we expect
	// to see on the wire:
	wantNanos := time.Date(2026, 4, 29, 14, 59, 19, 125014887, time.UTC)
	for _, in := range []string{
		"2026-04-29 14:59:19.125014887Z",   // lib/pq UTC, 9-digit fraction
		"2026-04-29 14:59:19.125014Z",      // 6-digit fraction (Rails-style + UTC)
		"2026-04-29 14:59:19Z",             // no fraction
	} {
		got, ok := pgTextTimestamp(in)
		require.Truef(t, ok, "input %q", in)
		require.Truef(t, got.Equal(time.Date(2026, 4, 29, 14, 59, 19, parseFracNs(in), time.UTC)),
			"got=%v want match for %q", got, in)
	}
	// Sanity check on the 9-digit fraction round-trip
	got, ok := pgTextTimestamp("2026-04-29 14:59:19.125014887Z")
	require.True(t, ok)
	require.True(t, got.Equal(wantNanos), "got=%v want=%v", got, wantNanos)

	// Non-UTC offsets must also round-trip on the space-separated form.
	gotPositive, ok := pgTextTimestamp("2026-04-29 16:59:19.125014887+02:00")
	require.True(t, ok)
	require.True(t, gotPositive.Equal(wantNanos),
		"+02:00 offset must equal the same instant as UTC")
}

func parseFracNs(s string) int {
	// pull the fractional-second portion out of "...:SS.fff[...]" and
	// pad to nanoseconds.
	idx := strings.Index(s, ".")
	if idx < 0 {
		return 0
	}
	end := idx + 1
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	frac := s[idx+1 : end]
	for len(frac) < 9 {
		frac += "0"
	}
	frac = frac[:9]
	n := 0
	for _, c := range frac {
		n = n*10 + int(c-'0')
	}
	return n
}
