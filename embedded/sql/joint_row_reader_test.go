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
	"errors"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestJointRowReader(t *testing.T) {
	st, err := store.Open("sqldata_joint_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("sqldata_joint_reader")

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, err = newJointRowReader(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	tx, err := engine.NewTx(context.Background())
	require.NoError(t, err)

	db, err := tx.catalog.newDatabase(1, "db1")
	require.NoError(t, err)

	err = tx.useDatabase("db1")
	require.NoError(t, err)

	table, err := db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}, {colName: "number", colType: IntegerType}})
	require.NoError(t, err)

	index, err := table.newIndex(true, []uint32{1})
	require.NoError(t, err)
	require.NotNil(t, index)
	require.Equal(t, table.primaryIndex, index)

	r, err := newRawRowReader(tx, nil, table, period{}, "", &ScanSpecs{Index: table.primaryIndex})
	require.NoError(t, err)

	_, err = newJointRowReader(r, []*JoinSpec{{joinType: LeftJoin}})
	require.Equal(t, ErrUnsupportedJoinType, err)

	_, err = newJointRowReader(r, []*JoinSpec{{joinType: InnerJoin, ds: &SelectStmt{}}})
	require.NoError(t, err)

	jr, err := newJointRowReader(r, []*JoinSpec{{joinType: InnerJoin, ds: &tableRef{table: "table1", as: "table2"}}})
	require.NoError(t, err)

	orderBy := jr.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)

	cols, err := jr.Columns()
	require.NoError(t, err)
	require.Len(t, cols, 4)
	require.Equal(t, cols[0].Table, "table1")
	require.Equal(t, cols[1].Table, "table1")
	require.Equal(t, cols[2].Table, "table2")
	require.Equal(t, cols[3].Table, "table2")
	require.Equal(t, cols[0].Column, "id")
	require.Equal(t, cols[1].Column, "number")
	require.Equal(t, cols[2].Column, "id")
	require.Equal(t, cols[3].Column, "number")

	scanSpecs := jr.ScanSpecs()
	require.NotNil(t, scanSpecs)
	require.NotNil(t, scanSpecs.Index)
	require.True(t, scanSpecs.Index.IsPrimary())

	t.Run("corner cases", func(t *testing.T) {

		t.Run("detect ambiguous selectors", func(t *testing.T) {
			jr, err = newJointRowReader(r, []*JoinSpec{{joinType: InnerJoin, ds: &tableRef{table: "table1"}}})
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

			jr, err := newJointRowReader(r,
				[]*JoinSpec{{joinType: InnerJoin, ds: &dummyDataSource{
					ResolveFunc: func(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
						return nil, injectedErr
					},
				}}})
			require.NoError(t, err)

			cols, err := jr.colsBySelector()
			require.ErrorIs(t, err, injectedErr)
			require.Nil(t, cols)
		})

		t.Run("must propagate error from joined reader on colsBySelector from Resolve", func(t *testing.T) {

			jr, err := newJointRowReader(r,
				[]*JoinSpec{{joinType: InnerJoin, ds: &dummyDataSource{
					ResolveFunc: func(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
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
