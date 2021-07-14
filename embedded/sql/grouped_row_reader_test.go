/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestGroupedRowReader(t *testing.T) {
	catalogStore, err := store.Open("catalog_grouped_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_grouped_reader")

	dataStore, err := store.Open("sqldata_grouped_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_grouped_reader")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	_, err = engine.newGroupedRowReader(nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	db, err := engine.catalog.newDatabase(1, "db1")
	require.NoError(t, err)

	table, err := db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}}, "id")
	require.NoError(t, err)

	snap, err := engine.Snapshot()
	require.NoError(t, err)

	r, err := engine.newRawRowReader(db, snap, table, 0, "", "id", EqualTo, nil)
	require.NoError(t, err)

	gr, err := engine.newGroupedRowReader(r, []Selector{&ColSelector{col: "id"}}, []*ColSelector{{col: "id"}})
	require.NoError(t, err)

	cols, err := gr.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 1)
}
