/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

// TestINLJCoversEqualityRanges verifies that coversEqualityRanges returns true
// only when the index leading column has a point-equality range.
func TestINLJCoversEqualityRanges(t *testing.T) {
	intVal := func(n int64) TypedValue { return &Integer{val: n} }

	makeRange := func(lo, hi TypedValue, loInc, hiInc bool) *typedValueRange {
		r := &typedValueRange{}
		if lo != nil {
			r.lRange = &typedValueSemiRange{val: lo, inclusive: loInc}
		}
		if hi != nil {
			r.hRange = &typedValueSemiRange{val: hi, inclusive: hiInc}
		}
		return r
	}

	col := &Column{id: 1}
	idx := &Index{cols: []*Column{col}}

	t.Run("empty rangesByColID", func(t *testing.T) {
		require.False(t, idx.coversEqualityRanges(nil))
		require.False(t, idx.coversEqualityRanges(map[uint32]*typedValueRange{}))
	})

	t.Run("unrelated column", func(t *testing.T) {
		ranges := map[uint32]*typedValueRange{
			99: makeRange(intVal(5), intVal(5), true, true),
		}
		require.False(t, idx.coversEqualityRanges(ranges))
	})

	t.Run("equality range (EQ)", func(t *testing.T) {
		ranges := map[uint32]*typedValueRange{
			1: makeRange(intVal(42), intVal(42), true, true),
		}
		require.True(t, idx.coversEqualityRanges(ranges))
	})

	t.Run("inequality range (GT)", func(t *testing.T) {
		ranges := map[uint32]*typedValueRange{
			1: makeRange(intVal(10), nil, false, false),
		}
		require.False(t, idx.coversEqualityRanges(ranges))
	})

	t.Run("half-open range", func(t *testing.T) {
		ranges := map[uint32]*typedValueRange{
			1: makeRange(intVal(1), intVal(10), true, false),
		}
		require.False(t, idx.coversEqualityRanges(ranges))
	})

	t.Run("only lRange set", func(t *testing.T) {
		ranges := map[uint32]*typedValueRange{
			1: makeRange(intVal(5), nil, true, false),
		}
		require.False(t, idx.coversEqualityRanges(ranges))
	})
}

// TestINLJUsesSecondaryIndexForJoinInnerScan verifies that a JOIN query against
// a table that has a secondary index on the join column actually uses that
// index (INLJ) rather than a full sequential scan.
//
// Schema: customer(id PK, name) joined to rental(id PK, customer_id, item)
// with a secondary index on rental(customer_id).
//
// The query aggregates rentals per customer; correct results must be returned
// regardless of which scan strategy the planner chose.
func TestINLJUsesSecondaryIndexForJoinInnerScan(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	ctx := context.Background()
	exec := func(sql string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, sql, nil)
		require.NoError(t, err)
	}

	// Schema
	exec(`CREATE TABLE customer (id INTEGER, name VARCHAR[64], PRIMARY KEY id)`)
	exec(`CREATE TABLE rental (id INTEGER, customer_id INTEGER, item VARCHAR[64], PRIMARY KEY id)`)
	exec(`CREATE INDEX ON rental(customer_id)`)

	// Seed: 5 customers, ~3 rentals each
	for i := 1; i <= 5; i++ {
		exec(`INSERT INTO customer(id, name) VALUES(` + itoa(i) + `, 'cust` + itoa(i) + `')`)
	}
	rentalID := 1
	for cust := 1; cust <= 5; cust++ {
		for j := 0; j < cust; j++ { // cust 1 → 1 rental, cust 5 → 5 rentals
			exec(`INSERT INTO rental(id, customer_id, item) VALUES(` +
				itoa(rentalID) + `, ` + itoa(cust) + `, 'item` + itoa(rentalID) + `')`)
			rentalID++
		}
	}

	// Verify planner picks the secondary index for the inner scan.
	// genScanSpecs is tested indirectly: if INLJ is NOT used the query would
	// still be correct but slower; we verify correctness + index selection.
	tx, err := engine.NewTx(ctx, DefaultTxOptions())
	require.NoError(t, err)
	defer tx.Cancel()

	// Confirm the secondary index exists on rental(customer_id).
	rentalTable, err := tx.catalog.GetTableByName("rental")
	require.NoError(t, err)
	var secIdx *Index
	for _, idx := range rentalTable.indexes {
		if !idx.IsPrimary() && len(idx.cols) > 0 && idx.cols[0].colName == "customer_id" {
			secIdx = idx
		}
	}
	require.NotNil(t, secIdx, "secondary index on rental(customer_id) not found")

	// Build the equality range that the join inner scan would produce for a
	// specific customer_id value and verify coversEqualityRanges returns true.
	ranges := map[uint32]*typedValueRange{
		secIdx.cols[0].id: {
			lRange: &typedValueSemiRange{val: &Integer{val: 3}, inclusive: true},
			hRange: &typedValueSemiRange{val: &Integer{val: 3}, inclusive: true},
		},
	}
	require.True(t, secIdx.coversEqualityRanges(ranges),
		"secondary index should cover the equality range")

	// Directly verify that genScanSpecs picks the secondary index for an
	// equality WHERE predicate — the core INLJ planner assertion.
	// This fails if the fallback is missing or gated incorrectly.
	innerStmt := &SelectStmt{
		ds: &tableRef{table: "rental"},
		where: &CmpBoolExp{
			op:    EQ,
			left:  &ColSelector{table: "rental", col: "customer_id"},
			right: &Integer{val: 3},
		},
	}
	scanSpecs, err := innerStmt.genScanSpecs(tx, nil)
	require.NoError(t, err)
	require.Equal(t, secIdx, scanSpecs.Index,
		"genScanSpecs must select the secondary index for equality WHERE (INLJ)")

	// Run the actual JOIN query and verify correct results.
	r, err := engine.Query(ctx, nil,
		`SELECT c.name, COUNT(*) AS cnt
		 FROM customer c
		 JOIN rental r ON c.id = r.customer_id
		 GROUP BY c.id, c.name
		 ORDER BY cnt DESC
		 LIMIT 3`, nil)
	require.NoError(t, err)
	defer r.Close()

	rows := 0
	for {
		row, err := r.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		rows++
		_ = row
	}
	require.Equal(t, 3, rows, "expected top-3 customers returned")
}

// itoa is a tiny helper so the test has no import for strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
