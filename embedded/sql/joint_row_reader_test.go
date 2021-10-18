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
	"errors"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestJointRowReader(t *testing.T) {
	catalogStore, err := store.Open("catalog_joint_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_joint_reader")

	dataStore, err := store.Open("sqldata_joint_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_joint_reader")

	engine, err := NewEngine(catalogStore, dataStore, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	err = engine.EnsureCatalogReady(nil)
	require.NoError(t, err)

	_, err = engine.newJointRowReader(nil, nil, nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	db, err := engine.catalog.newDatabase(1, "db1")
	require.NoError(t, err)

	table, err := db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}})
	require.NoError(t, err)

	index, err := table.newIndex(true, []uint32{1})
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Equal(t, table.primaryIndex, index)

	snap, err := engine.getSnapshot()
	require.NoError(t, err)

	r, err := engine.newRawRowReader(snap, table, 0, "", &ScanSpecs{index: table.primaryIndex})
	require.NoError(t, err)

	_, err = engine.newJointRowReader(db, snap, nil, r, []*JoinSpec{{joinType: LeftJoin}})
	require.Equal(t, ErrUnsupportedJoinType, err)

	_, err = engine.newJointRowReader(db, snap, nil, r, []*JoinSpec{{joinType: InnerJoin, ds: &SelectStmt{}}})
	require.NoError(t, err)

	jr, err := engine.newJointRowReader(db, snap, nil, r, []*JoinSpec{{joinType: InnerJoin, ds: &tableRef{table: "table1", as: "table2"}}})
	require.NoError(t, err)

	orderBy := jr.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)

	cols, err := jr.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 2)

	scanSpecs := jr.ScanSpecs()
	require.NotNil(t, scanSpecs)
	require.NotNil(t, scanSpecs.index)
	require.True(t, scanSpecs.index.IsPrimary())

	t.Run("corner cases", func(t *testing.T) {

		t.Run("detect ambiguous selectors", func(t *testing.T) {
			jr, err = engine.newJointRowReader(db, snap, nil, r, []*JoinSpec{{joinType: InnerJoin, ds: &tableRef{table: "table1"}}})
			require.NoError(t, err)

			_, err = jr.colsBySelector()
			require.ErrorIs(t, err, ErrAmbiguousSelector)
		})

		t.Run("must propagate error from rowReader on colsBySelector", func(t *testing.T) {
			jr := jointRowReader{
				rowReader: &dummyRowReader{},
			}

			cols, err := jr.colsBySelector()
			require.ErrorIs(t, err, errDummy)
			require.Nil(t, cols)
		})

		t.Run("must propagate error from rowReader on InferParameters", func(t *testing.T) {
			jr := jointRowReader{
				rowReader: &dummyRowReader{
					failInferringParams: true,
				},
			}

			err := jr.InferParameters(make(map[string]SQLValueType))
			require.ErrorIs(t, err, errDummy)

			jr.rowReader.(*dummyRowReader).failInferringParams = false
			err = jr.InferParameters(make(map[string]SQLValueType))
			require.ErrorIs(t, err, errDummy)
		})

		t.Run("must propagate error from rowReader on Columns", func(t *testing.T) {

			jr := jointRowReader{
				rowReader: &dummyRowReader{
					failReturningColumns: true,
				},
			}

			cols, err := jr.Columns()
			require.ErrorIs(t, err, errDummy)
			require.Nil(t, cols)
		})

		t.Run("must propagate error from joined reader on colsBySelector", func(t *testing.T) {
			injectedErr := errors.New("err")

			jr, err := engine.newJointRowReader(db, snap, nil, r,
				[]*JoinSpec{{joinType: InnerJoin, ds: &dummyDataSource{
					ResolveFunc: func(e *Engine, snap *store.Snapshot, implicitDB *Database, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
						return nil, injectedErr
					},
				}}})
			require.NoError(t, err)

			cols, err := jr.colsBySelector()
			require.ErrorIs(t, err, injectedErr)
			require.Nil(t, cols)
		})

		t.Run("must propagate error from joined reader on colsBySelector from Resolve", func(t *testing.T) {

			jr, err := engine.newJointRowReader(db, snap, nil, r,
				[]*JoinSpec{{joinType: InnerJoin, ds: &dummyDataSource{
					ResolveFunc: func(e *Engine, snap *store.Snapshot, implicitDB *Database, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
						return &dummyRowReader{}, nil
					},
				}}})
			require.NoError(t, err)

			cols, err := jr.colsBySelector()
			require.ErrorIs(t, err, errDummy)
			require.Nil(t, cols)
		})
	})
}
