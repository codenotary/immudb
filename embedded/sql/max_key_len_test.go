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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMaxKeyLenAllows1024 pins the bump from 512 → 1024 for the
// SQL-engine-side variable-key cap. The value matches the underlying
// store/btree default (embedded/store/immustore.go:MaxKeyLen = 1024),
// so engine code is no longer artificially stricter than the storage
// layer it sits on. Existing tables with shorter keys remain valid;
// this test asserts that:
//
//   - VARCHAR[N] declarations for 513 ≤ N ≤ 1024 are now accepted by
//     the SQL engine where the old 512 cap rejected them with
//     ErrLimitedKeyType, and
//   - inserting and reading back a value of that wider length round-
//     trips through the indexer (as a non-PK indexed column, which is
//     the realistic case — composite PK keys still have to fit in the
//     same 1024B store cap, so a VARCHAR PK declaration above ~480B
//     can be created but cannot be inserted into).
func TestMaxKeyLenAllows1024(t *testing.T) {
	require.GreaterOrEqual(t, MaxKeyLen, 1024,
		"engine MaxKeyLen must match the store-layer cap (1024)")

	engine := setupCommonTest(t)

	// CREATE TABLE with a VARCHAR[800] non-PK indexed column would have
	// been rejected outright under the old MaxKeyLen=512 with
	// ErrLimitedKeyType. The bump now accepts it.
	_, _, err := engine.Exec(context.Background(), nil,
		"CREATE TABLE wide_idx (id INTEGER NOT NULL, name VARCHAR[800], PRIMARY KEY id)", nil)
	require.NoError(t, err)
	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE INDEX ON wide_idx(name)", nil)
	require.NoError(t, err)

	// A 700-byte value in the wide indexed column round-trips: the
	// secondary-index entry encodes the value once + the small INTEGER
	// PK suffix, staying well under the 1024B composite-key cap.
	value := strings.Repeat("v", 700)
	_, _, err = engine.Exec(context.Background(), nil,
		"INSERT INTO wide_idx (id, name) VALUES (1, @name)",
		map[string]interface{}{"name": value},
	)
	require.NoError(t, err)

	rows, err := engine.queryAll(context.Background(), nil,
		"SELECT id, name FROM wide_idx", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	gotName, ok := rows[0].ValuesByPosition[1].RawValue().(string)
	require.True(t, ok)
	require.Equal(t, value, gotName)

	// 1025-byte declaration is still rejected — the engine ceiling sits
	// at MaxKeyLen exactly.
	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE TABLE too_big (id INTEGER NOT NULL, name VARCHAR[1025], PRIMARY KEY id)", nil)
	// The DDL itself succeeds (only the indexed-column path enforces
	// the limit), but an attempt to index the column must fail.
	require.NoError(t, err)
	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE INDEX ON too_big(name)", nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)

	// Same goes for declaring the over-cap column as the PRIMARY KEY:
	// the engine rejects the table outright.
	_, _, err = engine.Exec(context.Background(), nil,
		"CREATE TABLE too_big_pk (name VARCHAR[1025], PRIMARY KEY name)", nil)
	require.ErrorIs(t, err, ErrLimitedKeyType)
}
