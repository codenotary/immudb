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

// TestSystemTable_PgTypeRegistered asserts that pg_type — the one
// system table immudb has always shipped — is still installed with
// its historic three-column schema and still returns zero rows at
// the SQL engine layer (the PG wire layer synthesises rows separately).
func TestSystemTable_PgTypeRegistered(t *testing.T) {
	engine := setupCommonTest(t)

	// All three historic columns must resolve — a wrong schema would
	// surface here as "column does not exist".
	r, err := engine.Query(context.Background(), nil,
		"SELECT oid, typbasetype, typname FROM pg_type", nil)
	require.NoError(t, err)
	defer r.Close()

	// Zero rows at the SQL layer — historic contract preserved by the
	// pg_type registration in catalog.go having Scan == nil.
	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

// TestSystemTable_RegisterAndQuery registers a dummy system table with
// a live Scan func and verifies it's queryable end-to-end through the
// SQL engine. This is the contract future pg-compat phases (A2+) will
// rely on when they register pg_class, pg_attribute, etc.
func TestSystemTable_RegisterAndQuery(t *testing.T) {
	RegisterSystemTable(&SystemTableDef{
		Name: "test_sys_colors",
		Columns: []SystemTableColumn{
			{Name: "id", Type: IntegerType},
			{Name: "name", Type: VarcharType, MaxLen: 16},
		},
		PKColumn: "id",
		Scan: func(ctx context.Context, tx *SQLTx) ([]*Row, error) {
			return []*Row{
				{ValuesByPosition: []TypedValue{&Integer{val: 1}, &Varchar{val: "red"}}},
				{ValuesByPosition: []TypedValue{&Integer{val: 2}, &Varchar{val: "green"}}},
				{ValuesByPosition: []TypedValue{&Integer{val: 3}, &Varchar{val: "blue"}}},
			}, nil
		},
	})

	engine := setupCommonTest(t)

	r, err := engine.Query(context.Background(), nil,
		"SELECT id, name FROM test_sys_colors ORDER BY id", nil)
	require.NoError(t, err)
	defer r.Close()

	var got []string
	for {
		row, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		require.Len(t, row.ValuesByPosition, 2)
		got = append(got, row.ValuesByPosition[1].RawValue().(string))
	}
	require.Equal(t, []string{"red", "green", "blue"}, got)
}

// TestSystemTable_WhereFilter verifies the SQL engine's WHERE clause
// works over system-table rows — important for pg-compat views like
// `SELECT ... FROM pg_class WHERE c.oid = '16384'`.
func TestSystemTable_WhereFilter(t *testing.T) {
	RegisterSystemTable(&SystemTableDef{
		Name: "test_sys_fruits",
		Columns: []SystemTableColumn{
			{Name: "id", Type: IntegerType},
			{Name: "name", Type: VarcharType, MaxLen: 16},
		},
		PKColumn: "id",
		Scan: func(ctx context.Context, tx *SQLTx) ([]*Row, error) {
			return []*Row{
				{ValuesByPosition: []TypedValue{&Integer{val: 1}, &Varchar{val: "apple"}}},
				{ValuesByPosition: []TypedValue{&Integer{val: 2}, &Varchar{val: "banana"}}},
				{ValuesByPosition: []TypedValue{&Integer{val: 3}, &Varchar{val: "cherry"}}},
			}, nil
		},
	})

	engine := setupCommonTest(t)

	r, err := engine.Query(context.Background(), nil,
		"SELECT name FROM test_sys_fruits WHERE id = 2", nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "banana", row.ValuesByPosition[0].RawValue())

	_, err = r.Read(context.Background())
	require.ErrorIs(t, err, ErrNoMoreRows)
}

// TestSystemTable_RegisterDuplicatePanics guards the registry against
// silently overwriting a prior definition — a common footgun when two
// packages try to own the same pg_catalog object.
func TestSystemTable_RegisterDuplicatePanics(t *testing.T) {
	// pg_type is already registered by the init() in catalog.go, so
	// registering again must panic. No setupCommonTest needed — this
	// exercises only the registry itself.
	require.Panics(t, func() {
		RegisterSystemTable(&SystemTableDef{
			Name:     "pg_type",
			Columns:  []SystemTableColumn{{Name: "oid", Type: IntegerType}},
			PKColumn: "oid",
		})
	})
}

// TestSystemTable_RegisterValidation pins the defensive panics that
// protect call sites from malformed definitions.
func TestSystemTable_RegisterValidation(t *testing.T) {
	require.Panics(t, func() { RegisterSystemTable(nil) })

	require.Panics(t, func() {
		RegisterSystemTable(&SystemTableDef{
			Name:     "", // empty
			Columns:  []SystemTableColumn{{Name: "oid", Type: IntegerType}},
			PKColumn: "oid",
		})
	})

	require.Panics(t, func() {
		RegisterSystemTable(&SystemTableDef{
			Name:     "t_missing_cols",
			Columns:  nil,
			PKColumn: "oid",
		})
	})

	require.Panics(t, func() {
		RegisterSystemTable(&SystemTableDef{
			Name:     "t_missing_pk",
			Columns:  []SystemTableColumn{{Name: "oid", Type: IntegerType}},
			PKColumn: "", // empty
		})
	})

	require.Panics(t, func() {
		RegisterSystemTable(&SystemTableDef{
			Name:     "t_bad_pk",
			Columns:  []SystemTableColumn{{Name: "oid", Type: IntegerType}},
			PKColumn: "nonexistent",
		})
	})
}
