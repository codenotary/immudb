/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package sql

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

// TestCountRowReader_StubGetters constructs a keyFilterCountingRowReader
// directly and exercises the RowReader-interface methods that are otherwise
// not on the planner-driven hot path: OrderBy / ScanSpecs / Columns /
// InferParameters / colsBySelector / Tx / TableAlias / Parameters / onClose.
// The Read path is already covered by TestCountStarIndexedWhere — this is
// just to keep the interface surface tracked by coverage.
func TestCountRowReader_StubGetters(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer st.Close()

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	ctx := context.Background()
	tx, err := engine.NewTx(ctx, DefaultTxOptions())
	require.NoError(t, err)

	_, _, err = engine.Exec(ctx, tx, "CREATE TABLE t(id INTEGER, name VARCHAR[32], PRIMARY KEY id)", nil)
	require.NoError(t, err)

	tx, err = engine.NewTx(ctx, DefaultTxOptions())
	require.NoError(t, err)
	defer tx.Cancel()

	table := tx.catalog.tables[0]

	rawRdr, err := newRawRowReader(tx, nil, table, period{}, "", &ScanSpecs{Index: table.primaryIndex})
	require.NoError(t, err)

	agg := &AggColSelector{aggFn: COUNT, col: "*"}
	bare := newCountingRowReader(rawRdr, agg)
	require.NotNil(t, bare)

	// Bare COUNT reader: no ordering, single COUNT column, scanSpecs forwarded.
	require.Nil(t, bare.OrderBy())
	require.Equal(t, rawRdr.ScanSpecs(), bare.ScanSpecs())

	cols, err := bare.Columns(ctx)
	require.NoError(t, err)
	require.Len(t, cols, 1)
	require.Equal(t, IntegerType, cols[0].Type)
	require.Equal(t, string(COUNT), cols[0].AggFn)

	colsBySel, err := bare.colsBySelector(ctx)
	require.NoError(t, err)
	require.Contains(t, colsBySel, bare.encSel)

	require.Equal(t, rawRdr.Tx(), bare.Tx())
	require.Equal(t, rawRdr.TableAlias(), bare.TableAlias())
	require.Equal(t, rawRdr.Parameters(), bare.Parameters())

	params := map[string]SQLValueType{}
	require.NoError(t, bare.InferParameters(ctx, params))

	closed := false
	bare.onClose(func() { closed = true })

	// Exercise the keyFilter variant: same surface, plus a where expression
	// that the stubs don't touch.
	rawRdr2, err := newRawRowReader(tx, nil, table, period{}, "", &ScanSpecs{Index: table.primaryIndex})
	require.NoError(t, err)
	where := &CmpBoolExp{op: EQ, left: &ColSelector{col: "id"}, right: &Integer{val: 1}}
	kf := newKeyFilterCountingRowReader(rawRdr2, agg, where)
	require.NotNil(t, kf)
	require.Nil(t, kf.OrderBy())
	require.Equal(t, rawRdr2.ScanSpecs(), kf.ScanSpecs())

	kfCols, err := kf.Columns(ctx)
	require.NoError(t, err)
	require.Len(t, kfCols, 1)
	require.NoError(t, kf.InferParameters(ctx, params))

	require.NoError(t, bare.Close())
	require.True(t, closed)
	require.NoError(t, kf.Close())
}
