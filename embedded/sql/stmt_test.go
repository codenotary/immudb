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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiresTypeColSelectorsValueExp(t *testing.T) {
	cols := make(map[string]*ColDescriptor)
	cols["(db1.mytable.id)"] = &ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = &ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = &ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = &ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = &ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]*ColDescriptor
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
			expectedError: ErrInvalidColumn,
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
			expectedError: ErrInvalidColumn,
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
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeNumExpValueExp(t *testing.T) {
	cols := make(map[string]*ColDescriptor)
	cols["(db1.mytable.id)"] = &ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = &ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = &ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = &ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = &ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]*ColDescriptor
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
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeSimpleValueExp(t *testing.T) {
	cols := make(map[string]*ColDescriptor)
	cols["(db1.mytable.id)"] = &ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = &ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = &ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = &ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = &ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]*ColDescriptor
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
			exp:           &LikeBoolExp{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &LikeBoolExp{},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitDB, tc.implicitTable)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeSysFnValueExp(t *testing.T) {
	cols := make(map[string]*ColDescriptor)
	cols["(db1.mytable.id)"] = &ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = &ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = &ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = &ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = &ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]*ColDescriptor
		params        map[string]SQLValueType
		implicitDB    string
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &SysFn{fn: "NOW"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &SysFn{fn: "NOW"},
			cols:          cols,
			params:        params,
			implicitDB:    "db1",
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &SysFn{fn: "LOWER"},
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
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitDB, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeBinValueExp(t *testing.T) {
	cols := make(map[string]*ColDescriptor)
	cols["(db1.mytable.id)"] = &ColDescriptor{Type: IntegerType}
	cols["(db1.mytable.title)"] = &ColDescriptor{Type: VarcharType}
	cols["(db1.mytable.active)"] = &ColDescriptor{Type: BooleanType}
	cols["(db1.mytable.payload)"] = &ColDescriptor{Type: BLOBType}
	cols["COUNT(db1.mytable.*)"] = &ColDescriptor{Type: IntegerType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]*ColDescriptor
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
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

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
}

func TestAliasing(t *testing.T) {
	stmt := &SelectStmt{ds: &tableRef{table: "table1"}}
	require.Equal(t, "table1", stmt.Alias())

	stmt.as = "t1"
	require.Equal(t, "t1", stmt.Alias())
}
