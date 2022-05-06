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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequiresTypeColSelectorsValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(db1.mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.ts)"] = ColDescriptor{Type: TimestampType}
	cols["(db1.mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &ColSelector{db: "db1", table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &ColSelector{db: "db1", table: "mytable", col: "id1"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrColumnDoesNotExist,
		},
		{
			exp:           &ColSelector{db: "db1", table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &ColSelector{db: "db1", table: "mytable", col: "ts"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  TimestampType,
			expectedError: nil,
		},
		{
			exp:           &ColSelector{db: "db1", table: "mytable", col: "ts"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &AggColSelector{aggFn: "COUNT", db: "db1", table: "mytable", col: "*"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "COUNT", db: "db1", table: "mytable", col: "*"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &AggColSelector{aggFn: "MIN", db: "db1", table: "mytable", col: "title"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "MIN", db: "db1", table: "mytable", col: "title1"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrColumnDoesNotExist,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", db: "db1", table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", db: "db1", table: "mytable", col: "title"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeNumExpValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(db1.mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &NumExp{op: ADDOP, left: &Number{val: 0}, right: &Number{val: 0}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Number{val: 0}, right: &Number{val: 0}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Bool{val: true}, right: &Number{val: 0}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Number{val: 0}, right: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeSimpleValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(db1.mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &NullValue{t: AnyType},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &NullValue{t: VarcharType},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &NullValue{t: BooleanType},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Number{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &Number{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Varchar{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &Varchar{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Bool{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &Bool{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Blob{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BLOBType,
			expectedError: nil,
		},
		{
			exp:           &Blob{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NotBoolExp{exp: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &NotBoolExp{exp: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NotBoolExp{exp: &Varchar{val: "abc"}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Varchar{val: ""}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Varchar{val: ""}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &LikeBoolExp{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidCondition,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeSysFnValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(db1.mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &FnCall{fn: "NOW"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  TimestampType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: "NOW"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: "LOWER"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrIllegalArguments,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeBinValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(db1.mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &BinBoolExp{op: AND, left: &Bool{val: true}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &BinBoolExp{op: AND, left: &Bool{val: true}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &BinBoolExp{op: AND, left: &Number{val: 1}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &BinBoolExp{op: AND, left: &Bool{val: false}, right: &Number{val: 1}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Number{val: 1}, right: &Number{val: 1}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Number{val: 1}, right: &Number{val: 1}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Number{val: 1}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Bool{val: false}, right: &Number{val: 1}},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestYetUnsupportedExistsBoolExp(t *testing.T) {
	exp := &ExistsBoolExp{}

	_, err := exp.inferType(nil, nil, "", "")
	require.Error(t, err)

	err = exp.requiresType(BooleanType, nil, nil, "", "")
	require.Error(t, err)

	rexp, err := exp.substitute(nil)
	require.NoError(t, err)
	require.Equal(t, exp, rexp)

	_, err = exp.reduce(nil, nil, "", "")
	require.Error(t, err)

	require.Equal(t, exp, exp.reduceSelectors(nil, "", ""))

	require.False(t, exp.isConstant())

	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))
}

func TestYetUnsupportedInSubQueryExp(t *testing.T) {
	exp := &InSubQueryExp{}

	_, err := exp.inferType(nil, nil, "", "")
	require.ErrorIs(t, err, ErrNoSupported)

	err = exp.requiresType(BooleanType, nil, nil, "", "")
	require.ErrorIs(t, err, ErrNoSupported)

	rexp, err := exp.substitute(nil)
	require.NoError(t, err)
	require.Equal(t, exp, rexp)

	_, err = exp.reduce(nil, nil, "", "")
	require.ErrorIs(t, err, ErrNoSupported)

	require.Equal(t, exp, exp.reduceSelectors(nil, "", ""))

	require.False(t, exp.isConstant())

	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))
}

func TestLikeBoolExpEdgeCases(t *testing.T) {
	exp := &LikeBoolExp{}

	_, err := exp.inferType(nil, nil, "", "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = exp.requiresType(BooleanType, nil, nil, "", "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	_, err = exp.substitute(nil)
	require.ErrorIs(t, err, ErrInvalidCondition)

	_, err = exp.reduce(nil, nil, "", "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	require.Equal(t, exp, exp.reduceSelectors(nil, "", ""))
	require.False(t, exp.isConstant())
	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))

	t.Run("like expression with invalid types", func(t *testing.T) {
		exp := &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Number{}}

		_, err = exp.inferType(nil, nil, "", "")
		require.ErrorIs(t, err, ErrInvalidTypes)

		err = exp.requiresType(BooleanType, nil, nil, "", "")
		require.ErrorIs(t, err, ErrInvalidTypes)

		v := &NullValue{}

		row := &Row{
			ValuesByPosition: []TypedValue{v},
			ValuesBySelector: map[string]TypedValue{"(db1.table1.col1)": v},
		}

		_, err = exp.reduce(nil, row, "db1", "table1")
		require.ErrorIs(t, err, ErrInvalidTypes)
	})

}

func TestAliasing(t *testing.T) {
	stmt := &SelectStmt{ds: &tableRef{table: "table1"}}
	require.Equal(t, "table1", stmt.Alias())

	stmt.as = "t1"
	require.Equal(t, "t1", stmt.Alias())
}

func TestEdgeCases(t *testing.T) {
	stmt := &CreateIndexStmt{}
	_, err := stmt.execAt(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	stmt.cols = make([]string, MaxNumberOfColumnsInIndex+1)
	_, err = stmt.execAt(nil, nil)
	require.ErrorIs(t, err, ErrMaxNumberOfColumnsInIndexExceeded)
}

func TestIsConstant(t *testing.T) {
	require.True(t, (&NullValue{}).isConstant())
	require.True(t, (&Number{}).isConstant())
	require.True(t, (&Varchar{}).isConstant())
	require.True(t, (&Bool{}).isConstant())
	require.True(t, (&Blob{}).isConstant())
	require.True(t, (&Timestamp{}).isConstant())
	require.True(t, (&Param{}).isConstant())
	require.False(t, (&ColSelector{}).isConstant())
	require.False(t, (&AggColSelector{}).isConstant())

	require.True(t, (&NumExp{
		op:    AND,
		left:  &Number{val: 1},
		right: &Number{val: 2},
	}).isConstant())

	require.True(t, (&NotBoolExp{exp: &Bool{}}).isConstant())
	require.False(t, (&LikeBoolExp{}).isConstant())

	require.True(t, (&CmpBoolExp{
		op:    LE,
		left:  &Number{val: 1},
		right: &Number{val: 2},
	}).isConstant())

	require.True(t, (&BinBoolExp{
		op:    ADDOP,
		left:  &Number{val: 1},
		right: &Number{val: 2},
	}).isConstant())

	require.False(t, (&CmpBoolExp{
		op:    LE,
		left:  &Number{val: 1},
		right: &ColSelector{},
	}).isConstant())

	require.False(t, (&FnCall{}).isConstant())

	require.False(t, (&ExistsBoolExp{}).isConstant())
}

func TestTimestmapType(t *testing.T) {

	ts := &Timestamp{val: time.Date(2021, 12, 6, 11, 53, 0, 0, time.UTC)}

	t.Run("comparison functions", func(t *testing.T) {

		cmp, err := ts.Compare(&Timestamp{val: time.Date(2021, 12, 6, 11, 53, 0, 0, time.UTC)})
		require.NoError(t, err)
		require.Equal(t, 0, cmp)

		cmp, err = ts.Compare(&Timestamp{val: time.Date(2021, 12, 6, 11, 52, 0, 0, time.UTC)})
		require.NoError(t, err)
		require.Greater(t, cmp, 0)

		cmp, err = ts.Compare(&Timestamp{val: time.Date(2021, 12, 6, 11, 54, 0, 0, time.UTC)})
		require.NoError(t, err)
		require.Less(t, cmp, 0)

		cmp, err = ts.Compare(&NullValue{t: TimestampType})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		cmp, err = ts.Compare(&NullValue{t: AnyType})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		cmp, err = (&NullValue{t: TimestampType}).Compare(ts)
		require.NoError(t, err)
		require.Equal(t, -1, cmp)

		cmp, err = (&NullValue{t: AnyType}).Compare(ts)
		require.NoError(t, err)
		require.Equal(t, -1, cmp)
	})

	it, err := ts.inferType(map[string]ColDescriptor{}, map[string]string{}, "", "")
	require.NoError(t, err)
	require.Equal(t, TimestampType, it)

	err = ts.requiresType(TimestampType, map[string]ColDescriptor{}, map[string]string{}, "", "")
	require.NoError(t, err)

	err = ts.requiresType(IntegerType, map[string]ColDescriptor{}, map[string]string{}, "", "")
	require.ErrorIs(t, err, ErrInvalidTypes)

	v, err := ts.substitute(map[string]interface{}{})
	require.NoError(t, err)
	require.Equal(t, ts, v)

	v = ts.reduceSelectors(&Row{}, "", "")
	require.Equal(t, ts, v)

	err = ts.selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)
}
