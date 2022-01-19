/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package sql

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestGroupedRowReader(t *testing.T) {
	st, err := store.Open("sqldata_grouped_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_grouped_reader")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, err = newGroupedRowReader(nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	tx, err := engine.NewTx(context.Background())
	require.NoError(t, err)

	db, err := tx.catalog.newDatabase(1, "db1")
	require.NoError(t, err)

	table, err := db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}})
	require.NoError(t, err)

	index, err := table.newIndex(true, []uint32{1})
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Equal(t, table.primaryIndex, index)

	r, err := newRawRowReader(tx, nil, table, period{}, "", &ScanSpecs{Index: table.primaryIndex})
	require.NoError(t, err)

	gr, err := newGroupedRowReader(r, []Selector{&ColSelector{col: "id"}}, []*ColSelector{{col: "id"}})
	require.NoError(t, err)

	orderBy := gr.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)

	cols, err := gr.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 1)

	scanSpecs := gr.ScanSpecs()
	require.NotNil(t, scanSpecs)
	require.NotNil(t, scanSpecs.Index)
	require.True(t, scanSpecs.Index.IsPrimary())
}
