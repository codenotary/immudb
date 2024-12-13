/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRequiresTypeColSelectorsValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ts)"] = ColDescriptor{Type: TimestampType}
	cols["(mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(mytable.*)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ft)"] = ColDescriptor{Type: Float64Type}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &ColSelector{table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &ColSelector{table: "mytable", col: "id1"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrColumnDoesNotExist,
		},
		{
			exp:           &ColSelector{table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &ColSelector{table: "mytable", col: "ts"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  TimestampType,
			expectedError: nil,
		},
		{
			exp:           &ColSelector{table: "mytable", col: "ts"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &AggColSelector{aggFn: "COUNT", table: "mytable", col: "*"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "COUNT", table: "mytable", col: "*"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &AggColSelector{aggFn: "MIN", table: "mytable", col: "title"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "MIN", table: "mytable", col: "title1"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrColumnDoesNotExist,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", table: "mytable", col: "id"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", table: "mytable", col: "title"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", table: "mytable", col: "ft"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  Float64Type,
			expectedError: nil,
		},
		{
			exp:           &AggColSelector{aggFn: "SUM", table: "mytable", col: "ft"},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeNumExpValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(mytable.*)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ft)"] = ColDescriptor{Type: Float64Type}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &NumExp{op: ADDOP, left: &Integer{val: 0}, right: &Integer{val: 0}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Integer{val: 0}, right: &Integer{val: 0}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Bool{val: true}, right: &Integer{val: 0}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Integer{val: 0}, right: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NumExp{op: ADDOP, left: &Integer{val: 0}, right: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  Float64Type,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &UUID{val: uuid.New()},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  UUIDType,
			expectedError: nil,
		},
		{
			exp:           &UUID{val: uuid.New()},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  Float64Type,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeSimpleValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(mytable.*)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ft)"] = ColDescriptor{Type: Float64Type}
	cols["(mytable.data)"] = ColDescriptor{Type: JSONType}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp                  ValueExp
		cols                 map[string]ColDescriptor
		params               map[string]SQLValueType
		implicitTable        string
		requiredType         SQLValueType
		expectedInferredType SQLValueType
		expectedError        error
	}{
		{
			exp:           &NullValue{t: AnyType},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &NullValue{t: VarcharType},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &NullValue{t: BooleanType},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Integer{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &Integer{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:                  &Integer{},
			cols:                 cols,
			params:               params,
			implicitTable:        "mytable",
			requiredType:         JSONType,
			expectedInferredType: IntegerType,
			expectedError:        nil,
		},
		{
			exp:           &Varchar{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:                  &Varchar{},
			cols:                 cols,
			params:               params,
			implicitTable:        "mytable",
			requiredType:         JSONType,
			expectedInferredType: VarcharType,
			expectedError:        nil,
		},
		{
			exp:           &Varchar{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Bool{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:                  &Bool{},
			cols:                 cols,
			params:               params,
			implicitTable:        "mytable",
			requiredType:         JSONType,
			expectedInferredType: BooleanType,
			expectedError:        nil,
		},
		{
			exp:           &Bool{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &Blob{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BLOBType,
			expectedError: nil,
		},
		{
			exp:           &Blob{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &JSON{},
			cols:          cols,
			params:        params,
			requiredType:  JSONType,
			implicitTable: "mytable",
			expectedError: nil,
		},
		{
			exp:                  &JSON{val: "some-string"},
			cols:                 cols,
			params:               params,
			requiredType:         VarcharType,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        nil,
		},
		{
			exp:                  &JSON{val: int64(10)},
			cols:                 cols,
			params:               params,
			requiredType:         Float64Type,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        nil,
		},
		{
			exp:                  &JSON{val: float64(10.5)},
			cols:                 cols,
			params:               params,
			requiredType:         IntegerType,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        ErrInvalidTypes,
		},
		{
			exp:                  &JSON{val: true},
			cols:                 cols,
			params:               params,
			requiredType:         BooleanType,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        nil,
		},
		{
			exp:                  &JSON{val: nil},
			cols:                 cols,
			params:               params,
			requiredType:         AnyType,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        nil,
		},
		{
			exp:                  &JSON{val: int64(10)},
			cols:                 cols,
			params:               params,
			requiredType:         IntegerType,
			expectedInferredType: JSONType,
			implicitTable:        "mytable",
			expectedError:        nil,
		},
		{
			exp:           &NotBoolExp{exp: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &NotBoolExp{exp: &Bool{val: true}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &NotBoolExp{exp: &Varchar{val: "abc"}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Varchar{val: ""}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Varchar{val: ""}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &LikeBoolExp{},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidCondition,
		},
		{
			exp:           &LikeBoolExp{val: &ColSelector{col: "ft"}, pattern: &Varchar{val: ""}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  Float64Type,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			expectedInferredType := tc.expectedInferredType
			if expectedInferredType == "" {
				expectedInferredType = tc.requiredType
			}

			it, err := tc.exp.inferType(tc.cols, params, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, expectedInferredType, it)
		}
	}
}

func TestRequiresTypeSysFnValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(mytable.*)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ft)"] = ColDescriptor{Type: Float64Type}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &FnCall{fn: NowFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  TimestampType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: NowFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: LengthFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: LengthFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: SubstringFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: SubstringFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: ConcatFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: ConcatFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: TrimFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: TrimFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: UpperFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: LowerFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: LowerFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  Float64Type,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: JSONTypeOfFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: JSONTypeOfFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &FnCall{fn: UUIDFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  UUIDType,
			expectedError: nil,
		},
		{
			exp:           &FnCall{fn: UUIDFnCall},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  VarcharType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestRequiresTypeBinValueExp(t *testing.T) {
	cols := make(map[string]ColDescriptor)
	cols["(mytable.id)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.title)"] = ColDescriptor{Type: VarcharType}
	cols["(mytable.active)"] = ColDescriptor{Type: BooleanType}
	cols["(mytable.payload)"] = ColDescriptor{Type: BLOBType}
	cols["COUNT(mytable.*)"] = ColDescriptor{Type: IntegerType}
	cols["(mytable.ft)"] = ColDescriptor{Type: Float64Type}

	params := make(map[string]SQLValueType)

	testCases := []struct {
		exp           ValueExp
		cols          map[string]ColDescriptor
		params        map[string]SQLValueType
		implicitTable string
		requiredType  SQLValueType
		expectedError error
	}{
		{
			exp:           &BinBoolExp{op: And, left: &Bool{val: true}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &BinBoolExp{op: And, left: &Bool{val: true}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &BinBoolExp{op: And, left: &Integer{val: 1}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &BinBoolExp{op: And, left: &Bool{val: false}, right: &Integer{val: 1}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Integer{val: 1}, right: &Integer{val: 1}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: nil,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Integer{val: 1}, right: &Integer{val: 1}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  IntegerType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Integer{val: 1}, right: &Bool{val: false}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
		{
			exp:           &CmpBoolExp{op: LE, left: &Bool{val: false}, right: &Integer{val: 1}},
			cols:          cols,
			params:        params,
			implicitTable: "mytable",
			requiredType:  BooleanType,
			expectedError: ErrInvalidTypes,
		},
	}

	for i, tc := range testCases {
		err := tc.exp.requiresType(tc.requiredType, tc.cols, tc.params, tc.implicitTable)
		require.ErrorIs(t, err, tc.expectedError, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			it, err := tc.exp.inferType(tc.cols, params, tc.implicitTable)
			require.NoError(t, err)
			require.Equal(t, tc.requiredType, it)
		}
	}
}

func TestYetUnsupportedExistsBoolExp(t *testing.T) {
	exp := &ExistsBoolExp{}

	_, err := exp.inferType(nil, nil, "")
	require.Error(t, err)

	err = exp.requiresType(BooleanType, nil, nil, "")
	require.Error(t, err)

	rexp, err := exp.substitute(nil)
	require.NoError(t, err)
	require.Equal(t, exp, rexp)

	_, err = exp.reduce(nil, nil, "")
	require.Error(t, err)

	require.Equal(t, exp, exp.reduceSelectors(nil, ""))

	require.False(t, exp.isConstant())

	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))
}

func TestYetUnsupportedInSubQueryExp(t *testing.T) {
	exp := &InSubQueryExp{}

	_, err := exp.inferType(nil, nil, "")
	require.ErrorIs(t, err, ErrNoSupported)

	err = exp.requiresType(BooleanType, nil, nil, "")
	require.ErrorIs(t, err, ErrNoSupported)

	rexp, err := exp.substitute(nil)
	require.NoError(t, err)
	require.Equal(t, exp, rexp)

	_, err = exp.reduce(nil, nil, "")
	require.ErrorIs(t, err, ErrNoSupported)

	require.Equal(t, exp, exp.reduceSelectors(nil, ""))

	require.False(t, exp.isConstant())

	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))
}

func TestCaseWhenExp(t *testing.T) {
	t.Run("simple case", func(t *testing.T) {
		e, err := ParseExpFromString(
			"CASE job_title WHEN 1 THEN true ELSE false END",
		)
		require.NoError(t, err)

		err = e.requiresType(BooleanType, map[string]ColDescriptor{
			EncodeSelector("", "", "job_title"): {Type: VarcharType},
		}, nil, "")
		require.ErrorIs(t, err, ErrInvalidTypes)
		require.ErrorContains(t, err, "argument of CASE/WHEN must be of type VARCHAR, not type INTEGER")

		e, err = ParseExpFromString(
			"CASE concat(@prefix, job_title) WHEN 'job_engineer' THEN true ELSE false END",
		)
		require.NoError(t, err)

		e, err = e.substitute(map[string]interface{}{"prefix": "job_"})
		require.NoError(t, err)

		v, err := e.reduce(nil, &Row{
			ValuesBySelector: map[string]TypedValue{
				EncodeSelector("", "", "job_title"): &Varchar{"engineer"},
			},
		}, "")
		require.NoError(t, err)
		require.Equal(t, v, &Bool{true})
	})

	t.Run("searched case", func(t *testing.T) {
		e, err := ParseExpFromString(
			"CASE WHEN salary > 100000 THEN @p0 ELSE @p1 END",
		)
		require.NoError(t, err)

		e, err = e.substitute(map[string]interface{}{"p0": int64(0), "p1": int64(1)})
		require.NoError(t, err)

		err = e.requiresType(IntegerType, map[string]ColDescriptor{
			EncodeSelector("", "", "salary"): {Type: IntegerType},
		}, nil, "")
		require.NoError(t, err)

		require.False(t, e.isConstant())
		require.Nil(t, e.selectorRanges(nil, "", nil, nil))

		row := &Row{ValuesBySelector: map[string]TypedValue{EncodeSelector("", "", "salary"): &Integer{50000}}}
		require.Equal(t,
			&CaseWhenExp{
				whenThen: []whenThenClause{
					{
						when: NewCmpBoolExp(GT, &Integer{50000}, &Integer{100000}), then: &Integer{0},
					},
				},
				elseExp: &Integer{1},
			}, e.reduceSelectors(row, ""))

		v, err := e.reduce(nil, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(1), v.RawValue())
	})
}

func TestInferTypeCaseWhenExp(t *testing.T) {
	t.Run("simple case", func(t *testing.T) {
		e, err := ParseExpFromString(
			"CASE department WHEN 'engineering' THEN 0 ELSE 1 END",
		)
		require.NoError(t, err)

		_, err = e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "department"): {Type: IntegerType},
			},
			nil,
			"",
		)
		require.ErrorIs(t, err, ErrInvalidTypes)
		require.ErrorContains(t, err, "argument of CASE/WHEN must be of type INTEGER, not type VARCHAR")

		it, err := e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "department"): {Type: VarcharType},
			},
			nil,
			"",
		)
		require.NoError(t, err)
		require.Equal(t, IntegerType, it)
	})

	t.Run("searched case", func(t *testing.T) {
		e, err := ParseExpFromString(
			"CASE WHEN salary THEN 10 ELSE '0' END",
		)
		require.NoError(t, err)

		_, err = e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "salary"): {Type: IntegerType},
			},
			nil,
			"",
		)
		require.ErrorIs(t, err, ErrInvalidTypes)

		e, err = ParseExpFromString(
			"CASE WHEN salary > 0 THEN 10 ELSE '0' END",
		)
		require.NoError(t, err)

		_, err = e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "salary"): {Type: IntegerType},
			},
			nil,
			"",
		)
		require.ErrorIs(t, err, ErrInferredMultipleTypes)

		e, err = ParseExpFromString(
			"CASE WHEN salary > 0 THEN 10 ELSE 0 END",
		)
		require.NoError(t, err)

		it, err := e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "salary"): {Type: IntegerType},
			},
			nil,
			"",
		)
		require.NoError(t, err)
		require.Equal(t, IntegerType, it)

		it, err = e.inferType(
			map[string]ColDescriptor{
				EncodeSelector("", "", "salary"): {Type: Float64Type},
			},
			nil,
			"",
		)
		require.NoError(t, err)
		require.Equal(t, IntegerType, it)
	})
}

func TestLikeBoolExpEdgeCases(t *testing.T) {
	exp := &LikeBoolExp{}

	_, err := exp.inferType(nil, nil, "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	err = exp.requiresType(BooleanType, nil, nil, "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	_, err = exp.substitute(nil)
	require.ErrorIs(t, err, ErrInvalidCondition)

	_, err = exp.reduce(nil, nil, "")
	require.ErrorIs(t, err, ErrInvalidCondition)

	require.Equal(t, exp, exp.reduceSelectors(nil, ""))
	require.False(t, exp.isConstant())
	require.Nil(t, exp.selectorRanges(nil, "", nil, nil))

	t.Run("like expression with invalid types", func(t *testing.T) {
		exp := &LikeBoolExp{val: &ColSelector{col: "col1"}, pattern: &Integer{}}

		_, err = exp.inferType(nil, nil, "")
		require.ErrorIs(t, err, ErrInvalidTypes)

		err = exp.requiresType(BooleanType, nil, nil, "")
		require.ErrorIs(t, err, ErrInvalidTypes)

		v := &Integer{}

		row := &Row{
			ValuesByPosition: []TypedValue{v},
			ValuesBySelector: map[string]TypedValue{"(table1.col1)": v},
		}

		_, err = exp.reduce(nil, row, "table1")
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
	_, err := stmt.execAt(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	stmt.cols = make([]string, MaxNumberOfColumnsInIndex+1)
	_, err = stmt.execAt(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrMaxNumberOfColumnsInIndexExceeded)
}

func TestInferParameterEdgeCases(t *testing.T) {
	err := (&CreateUserStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&AlterUserStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&AlterPrivilegesStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&DropUserStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&RenameTableStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&DropTableStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&DropColumnStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&DropIndexStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)

	err = (&FnDataSourceStmt{}).inferParameters(context.Background(), nil, nil)
	require.Nil(t, err)
}

func TestIsConstant(t *testing.T) {
	require.True(t, (&NullValue{}).isConstant())
	require.True(t, (&Integer{}).isConstant())
	require.True(t, (&Varchar{}).isConstant())
	require.True(t, (&Bool{}).isConstant())
	require.True(t, (&Blob{}).isConstant())
	require.True(t, (&UUID{}).isConstant())
	require.True(t, (&Timestamp{}).isConstant())
	require.True(t, (&Param{}).isConstant())
	require.False(t, (&ColSelector{}).isConstant())
	require.False(t, (&AggColSelector{}).isConstant())

	require.True(t, (&NumExp{
		op:    And,
		left:  &Integer{val: 1},
		right: &Integer{val: 2},
	}).isConstant())

	require.True(t, (&NotBoolExp{exp: &Bool{}}).isConstant())
	require.False(t, (&LikeBoolExp{}).isConstant())

	require.True(t, (&CmpBoolExp{
		op:    LE,
		left:  &Integer{val: 1},
		right: &Integer{val: 2},
	}).isConstant())

	require.True(t, (&BinBoolExp{
		op:    ADDOP,
		left:  &Integer{val: 1},
		right: &Integer{val: 2},
	}).isConstant())

	require.False(t, (&CmpBoolExp{
		op:    LE,
		left:  &Integer{val: 1},
		right: &ColSelector{},
	}).isConstant())

	require.False(t, (&FnCall{}).isConstant())

	require.False(t, (&ExistsBoolExp{}).isConstant())
}

func TestTimestamapType(t *testing.T) {

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

	it, err := ts.inferType(map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)
	require.Equal(t, TimestampType, it)

	err = ts.requiresType(TimestampType, map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)

	err = ts.requiresType(IntegerType, map[string]ColDescriptor{}, map[string]string{}, "")
	require.ErrorIs(t, err, ErrInvalidTypes)

	v, err := ts.substitute(map[string]interface{}{})
	require.NoError(t, err)
	require.Equal(t, ts, v)

	v = ts.reduceSelectors(&Row{}, "")
	require.Equal(t, ts, v)

	err = ts.selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)
}

func TestJSONType(t *testing.T) {
	js := &JSON{val: float64(10)}

	require.True(t, js.isConstant())
	require.False(t, js.IsNull())

	it, err := js.inferType(map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)
	require.Equal(t, JSONType, it)

	v, err := js.substitute(map[string]interface{}{})
	require.NoError(t, err)
	require.Equal(t, js, v)

	v, err = js.reduce(nil, nil, "")
	require.NoError(t, err)
	require.Equal(t, js, v)

	v = js.reduceSelectors(&Row{}, "")
	require.Equal(t, js, v)

	err = js.selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)

	t.Run("test comparison functions", func(t *testing.T) {
		type test struct {
			a             TypedValue
			b             TypedValue
			res           int
			expectedError error
		}

		tests := []test{
			{
				a: NewJson(10.5),
				b: NewJson(10.5),
			},
			{
				a:             NewJson(map[string]interface{}{}),
				b:             NewJson(map[string]interface{}{}),
				expectedError: ErrNotComparableValues,
			},
			{
				a:   NewJson(10.5),
				b:   NewFloat64(9.5),
				res: 1,
			},
			{
				a:   NewJson(true),
				b:   NewBool(true),
				res: 0,
			},
			{
				a:   NewJson("test"),
				b:   NewVarchar("test"),
				res: 0,
			},
			{
				a:   NewJson(int64(2)),
				b:   NewInteger(8),
				res: -1,
			},
			{
				a:   NewJson(nil),
				b:   NewNull(JSONType),
				res: 0,
			},
			{
				a:   NewJson(nil),
				b:   NewNull(AnyType),
				res: 0,
			},
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("compare %s to %s", tc.a.Type(), tc.b.Type()), func(t *testing.T) {
				res, err := tc.a.Compare(tc.b)
				if tc.expectedError != nil {
					require.ErrorIs(t, err, ErrNotComparableValues)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.res, res)
				}

				res1, err := tc.b.Compare(tc.a)
				if tc.expectedError != nil {
					require.ErrorIs(t, err, ErrNotComparableValues)
				} else {
					require.NoError(t, err)
					require.Equal(t, res, -res1)
				}
			})
		}
	})

	t.Run("test casts", func(t *testing.T) {
		type test struct {
			src TypedValue
			dst TypedValue
		}

		cases := []test{
			{
				src: &NullValue{t: JSONType},
				dst: &JSON{val: nil},
			},
			{
				src: &NullValue{t: AnyType},
				dst: &JSON{val: nil},
			},
			{
				src: &JSON{val: nil},
				dst: &NullValue{t: AnyType},
			},
			{
				src: &JSON{val: 10.5},
				dst: &Float64{val: 10.5},
			},
			{
				src: &Float64{val: 10.5},
				dst: &JSON{val: 10.5},
			},
			{
				src: &JSON{val: 10.5},
				dst: &Integer{val: 10},
			},
			{
				src: &Integer{val: 10},
				dst: &JSON{val: int64(10)},
			},
			{
				src: &JSON{val: true},
				dst: &Bool{val: true},
			},
			{
				src: &Bool{val: true},
				dst: &JSON{val: true},
			},
			{
				src: &JSON{val: "test"},
				dst: &Varchar{val: `"test"`},
			},
			{
				src: &Varchar{val: `{"name": "John Doe"}`},
				dst: &JSON{val: map[string]interface{}{"name": "John Doe"}},
			},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("cast %s to %s", tc.src.Type(), tc.dst.Type()), func(t *testing.T) {
				conv, err := getConverter(tc.src.Type(), tc.dst.Type())
				require.NoError(t, err)

				converted, err := conv(tc.src)
				require.NoError(t, err)
				require.Equal(t, converted, tc.dst)
			})
		}
	})
}

func TestUnionSelectErrors(t *testing.T) {
	t.Run("fail on creating union reader", func(t *testing.T) {
		reader1 := &dummyRowReader{
			recordClose: true,
		}

		reader2 := &dummyRowReader{
			recordClose:          true,
			failReturningColumns: true,
		}

		stmt := &UnionStmt{
			left: &dummyDataSource{
				ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
					return reader1, nil
				},
			},
			right: &dummyDataSource{
				ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
					return reader2, nil
				},
			},
			distinct: true,
		}

		reader, err := stmt.Resolve(context.Background(), nil, nil, nil)
		require.ErrorIs(t, err, errDummy)
		require.Nil(t, reader)
		require.True(t, reader1.closed)
		require.True(t, reader2.closed)
	})

	t.Run("fail on creating distinct reader", func(t *testing.T) {
		reader1 := &dummyRowReader{
			recordClose:                true,
			failSecondReturningColumns: true,
		}

		reader2 := &dummyRowReader{
			recordClose: true,
		}

		stmt := &UnionStmt{
			left: &dummyDataSource{
				ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
					return reader1, nil
				},
			},
			right: &dummyDataSource{
				ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
					return reader2, nil
				},
			},
			distinct: true,
		}

		reader, err := stmt.Resolve(context.Background(), nil, nil, nil)
		require.ErrorIs(t, err, errDummy)
		require.Nil(t, reader)
		require.True(t, reader1.closed)
		require.True(t, reader2.closed)
	})
}

func TestJoinErrors(t *testing.T) {
	baseReader := &dummyRowReader{
		recordClose: true,
	}

	stmt := &SelectStmt{
		ds: &dummyDataSource{
			ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
				return baseReader, nil
			},
		},
		joins: []*JoinSpec{{
			joinType: JoinType(99999),
		}},
	}

	reader, err := stmt.Resolve(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, ErrUnsupportedJoinType)
	require.Nil(t, reader)
	require.True(t, baseReader.closed)
}

func TestProjectedRowReaderErrors(t *testing.T) {
	baseReader := &dummyRowReader{
		recordClose:          true,
		failReturningColumns: true,
	}

	stmt := &SelectStmt{
		ds: &dummyDataSource{
			ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
				return baseReader, nil
			},
		},
	}

	reader, err := stmt.Resolve(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, errDummy)
	require.Nil(t, reader)
	require.True(t, baseReader.closed)
}

func TestDistinctRowReaderErrors(t *testing.T) {
	baseReader := &dummyRowReader{
		recordClose:                true,
		failSecondReturningColumns: true,
	}

	stmt := &SelectStmt{
		ds: &dummyDataSource{
			ResolveFunc: func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
				return baseReader, nil
			},
		},
		distinct: true,
	}

	reader, err := stmt.Resolve(context.Background(), nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, reader)
	require.True(t, baseReader.closed)
}

func TestFloat64Type(t *testing.T) {

	ts := &Float64{val: 0.0}

	t.Run("comparison functions", func(t *testing.T) {

		cmp, err := ts.Compare(&Float64{val: 0.0})
		require.NoError(t, err)
		require.Equal(t, 0, cmp)

		cmp, err = ts.Compare(&Float64{val: 0.1})
		require.NoError(t, err)
		require.Equal(t, cmp, -1)

		cmp, err = ts.Compare(&Float64{val: -0.1})
		require.NoError(t, err)
		require.Equal(t, cmp, 1)

		cmp, err = ts.Compare(&NullValue{t: Float64Type})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		cmp, err = ts.Compare(&NullValue{t: AnyType})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		cmp, err = (&NullValue{t: Float64Type}).Compare(ts)
		require.NoError(t, err)
		require.Equal(t, -1, cmp)

		cmp, err = (&NullValue{t: AnyType}).Compare(ts)
		require.NoError(t, err)
		require.Equal(t, -1, cmp)
	})

	it, err := ts.inferType(map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)
	require.Equal(t, Float64Type, it)

	err = ts.requiresType(Float64Type, map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)

	err = ts.requiresType(IntegerType, map[string]ColDescriptor{}, map[string]string{}, "")
	require.ErrorIs(t, err, ErrInvalidTypes)

	v, err := ts.substitute(map[string]interface{}{})
	require.NoError(t, err)
	require.Equal(t, ts, v)

	v = ts.reduceSelectors(&Row{}, "")
	require.Equal(t, ts, v)

	err = ts.selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)
}

func TestUUIDType(t *testing.T) {

	id := &UUID{val: uuid.New()}

	t.Run("comparison functions", func(t *testing.T) {
		cmp, err := id.Compare(&UUID{val: id.val})
		require.NoError(t, err)
		require.Equal(t, 0, cmp)

		cmp, err = id.Compare(&UUID{val: uuid.New()})
		require.NoError(t, err)
		require.NotZero(t, cmp)

		cmp, err = id.Compare(&NullValue{t: UUIDType})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		cmp, err = id.Compare(&NullValue{t: AnyType})
		require.NoError(t, err)
		require.Equal(t, 1, cmp)

		_, err = id.Compare(&Float64{})
		require.ErrorIs(t, err, ErrNotComparableValues)
	})

	err := id.requiresType(UUIDType, map[string]ColDescriptor{}, map[string]string{}, "")
	require.NoError(t, err)

	err = id.requiresType(IntegerType, map[string]ColDescriptor{}, map[string]string{}, "")
	require.ErrorIs(t, err, ErrInvalidTypes)

	v, err := id.substitute(map[string]interface{}{})
	require.NoError(t, err)
	require.Equal(t, id, v)

	v = id.reduceSelectors(&Row{}, "")
	require.Equal(t, id, v)

	err = id.selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)

	err = (&NullValue{}).selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)

	err = (&Integer{}).selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)

	err = (&Varchar{}).selectorRanges(&Table{}, "", map[string]interface{}{}, map[uint32]*typedValueRange{})
	require.NoError(t, err)
}

func TestTypedValueString(t *testing.T) {
	n := &NullValue{}
	require.Equal(t, "NULL", n.String())

	i := &Integer{val: 10}
	require.Equal(t, "10", i.String())

	s := &Varchar{val: "test"}
	require.Equal(t, "'test'", s.String())

	b := &Bool{val: true}
	require.Equal(t, "true", b.String())

	blob := &Blob{val: []byte{1, 2, 3}}
	require.Equal(t, hex.EncodeToString([]byte{1, 2, 3}), blob.String())

	ts := &Timestamp{val: time.Date(2024, time.April, 24, 10, 10, 10, 10, time.UTC)}
	require.Equal(t, "2024-04-24 10:10:10", ts.String())

	id := &UUID{val: uuid.New()}
	require.Equal(t, id.val.String(), id.String())

	count := &CountValue{c: 1}
	require.Equal(t, "1", count.String())

	sum := &SumValue{val: i}
	require.Equal(t, "10", sum.String())

	min := &MinValue{val: i}
	require.Equal(t, "10", min.String())

	max := &MaxValue{val: i}
	require.Equal(t, "10", max.String())

	avg := &AVGValue{s: &Float64{val: 10}, c: 4}
	require.Equal(t, "2.5", avg.String())

	jsVal := &JSON{val: map[string]interface{}{"name": "John Doe"}}
	require.Equal(t, jsVal.String(), `{"name":"John Doe"}`)
}

func TestRequiredPrivileges(t *testing.T) {
	type test struct {
		stmt       SQLStmt
		readOnly   bool
		privileges []SQLPrivilege
	}

	tests := []test{
		{
			stmt:       &SelectStmt{},
			readOnly:   true,
			privileges: []SQLPrivilege{SQLPrivilegeSelect},
		},
		{
			stmt:       &UnionStmt{},
			readOnly:   true,
			privileges: []SQLPrivilege{SQLPrivilegeSelect},
		},
		{
			stmt:       &tableRef{},
			readOnly:   true,
			privileges: []SQLPrivilege{SQLPrivilegeSelect},
		},
		{
			stmt:       &UpsertIntoStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeInsert, SQLPrivilegeUpdate},
		},
		{
			stmt:       &UpsertIntoStmt{isInsert: true},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeInsert},
		},
		{
			stmt:       &UpsertIntoStmt{ds: &SelectStmt{}},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeInsert, SQLPrivilegeUpdate, SQLPrivilegeSelect},
		},
		{
			stmt:       &UpsertIntoStmt{ds: &SelectStmt{}, isInsert: true},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeInsert, SQLPrivilegeSelect},
		},
		{
			stmt:       &DeleteFromStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeDelete},
		},
		{
			stmt:       &CreateDatabaseStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeCreate},
		},
		{
			stmt:       &CreateTableStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeCreate},
		},
		{
			stmt:       &CreateIndexStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeCreate},
		},
		{
			stmt:       &CreateIndexStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeCreate},
		},
		{
			stmt:       &DropTableStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeDrop},
		},
		{
			stmt:       &DropColumnStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeDrop},
		},
		{
			stmt:       &DropIndexStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeDrop},
		},
		{
			stmt:       &DropUserStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeDrop},
		},
		{
			stmt:       &FnDataSourceStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &BeginTransactionStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &CommitStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &RollbackStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &UseDatabaseStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &UseSnapshotStmt{},
			readOnly:   true,
			privileges: nil,
		},
		{
			stmt:       &AddColumnStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeAlter},
		},
		{
			stmt:       &RenameTableStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeAlter},
		},
		{
			stmt:       &RenameColumnStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeAlter},
		},
		{
			stmt:       &RenameColumnStmt{},
			readOnly:   false,
			privileges: []SQLPrivilege{SQLPrivilegeAlter},
		},
	}

	for _, tc := range tests {
		t.Run(reflect.TypeOf(tc.stmt).String(), func(t *testing.T) {
			require.Equal(t, tc.stmt.readOnly(), tc.readOnly)
			require.Equal(t, tc.stmt.requiredPrivileges(), tc.privileges)
		})
	}
}

func TestExprSelectors(t *testing.T) {
	type testCase struct {
		Expr      ValueExp
		selectors []Selector
	}

	tests := []testCase{
		{
			Expr: &Integer{},
		},
		{
			Expr: &Bool{},
		},
		{
			Expr: &Float64{},
		},
		{
			Expr: &NullValue{},
		},
		{
			Expr: &Blob{},
		},
		{
			Expr: &UUID{},
		},
		{
			Expr: &JSON{},
		},
		{
			Expr: &Timestamp{},
		},
		{
			Expr: &Varchar{},
		},
		{
			Expr: &Param{},
		},
		{
			Expr: &ColSelector{col: "col"},
			selectors: []Selector{
				&ColSelector{col: "col"},
			},
		},
		{
			Expr: &JSONSelector{ColSelector: &ColSelector{col: "col"}},
			selectors: []Selector{
				&JSONSelector{ColSelector: &ColSelector{col: "col"}},
			},
		},
		{
			Expr: &BinBoolExp{
				left:  &ColSelector{col: "col"},
				right: &ColSelector{col: "col1"},
			},
			selectors: []Selector{
				&ColSelector{col: "col"},
				&ColSelector{col: "col1"},
			},
		},
		{
			Expr: &NumExp{
				left:  &ColSelector{col: "col"},
				right: &ColSelector{col: "col1"},
			},
			selectors: []Selector{
				&ColSelector{col: "col"},
				&ColSelector{col: "col1"},
			},
		},
		{
			Expr: &LikeBoolExp{
				val: &ColSelector{col: "col"},
			},
			selectors: []Selector{
				&ColSelector{col: "col"},
			},
		},
		{
			Expr: &ExistsBoolExp{},
		},
		{
			Expr: &InSubQueryExp{val: &ColSelector{col: "col"}},
			selectors: []Selector{
				&ColSelector{col: "col"},
			},
		},
		{
			Expr: &InListExp{
				val: &ColSelector{col: "col"},
				values: []ValueExp{
					&ColSelector{col: "col1"},
					&ColSelector{col: "col2"},
					&ColSelector{col: "col3"},
				},
			},
			selectors: []Selector{
				&ColSelector{col: "col"},
				&ColSelector{col: "col1"},
				&ColSelector{col: "col2"},
				&ColSelector{col: "col3"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(reflect.TypeOf(tc.Expr).Elem().Name(), func(t *testing.T) {
			require.Equal(t, tc.selectors, tc.Expr.selectors())
		})
	}
}
