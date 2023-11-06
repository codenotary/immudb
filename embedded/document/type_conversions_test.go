/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
package document

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/protomodel"
)

func TestStructValueToSqlValue(t *testing.T) {
	// Test case for VarcharType
	value := &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: "test"},
	}
	result, err := structValueToSqlValue(value, sql.VarcharType)
	require.NoError(t, err, "Expected no error for VarcharType")
	require.Equal(t, sql.NewVarchar("test"), result, "Expected Varchar value")

	// Test case for VarcharType with NULL value
	value = &structpb.Value{
		Kind: &structpb.Value_NullValue{},
	}
	result, err = structValueToSqlValue(value, sql.VarcharType)
	require.NoError(t, err, "Expected no error for VarcharType with NULL value")
	require.Equal(t, sql.NewNull(sql.AnyType), result, "Expected NULL value")

	// Test case for IntegerType
	value = &structpb.Value{
		Kind: &structpb.Value_NumberValue{NumberValue: 42},
	}
	result, err = structValueToSqlValue(value, sql.IntegerType)
	require.NoError(t, err, "Expected no error for IntegerType")
	require.Equal(t, sql.NewInteger(42), result, "Expected Integer value")

	// Test case for IntegerType with NULL value
	value = &structpb.Value{
		Kind: &structpb.Value_NullValue{},
	}
	result, err = structValueToSqlValue(value, sql.IntegerType)
	require.NoError(t, err, "Expected no error for IntegerType with NULL value")
	require.Equal(t, sql.NewNull(sql.AnyType), result, "Expected NULL value")

	// Test case for BLOBType
	value = &structpb.Value{
		Kind: &structpb.Value_StringValue{StringValue: "1234"},
	}
	result, err = structValueToSqlValue(value, sql.BLOBType)
	require.NoError(t, err, "Expected no error for BLOBType")
	docID, err := NewDocumentIDFromHexEncodedString("1234")
	require.NoError(t, err)
	expectedBlob := sql.NewBlob(docID[:])
	require.Equal(t, expectedBlob, result, "Expected Blob value")

	// Test case for BLOBType with NULL value
	value = &structpb.Value{
		Kind: &structpb.Value_NullValue{},
	}
	result, err = structValueToSqlValue(value, sql.BLOBType)
	require.NoError(t, err, "Expected no error for BLOBType with NULL value")
	require.Equal(t, sql.NewNull(sql.AnyType), result, "Expected NULL value")

	// Test case for Float64Type
	value = &structpb.Value{
		Kind: &structpb.Value_NumberValue{NumberValue: 3.14},
	}
	result, err = structValueToSqlValue(value, sql.Float64Type)
	require.NoError(t, err, "Expected no error for Float64Type")
	require.Equal(t, sql.NewFloat64(3.14), result, "Expected Float64 value")

	// Test case for Float64Type with NULL value
	value = &structpb.Value{
		Kind: &structpb.Value_NullValue{},
	}
	result, err = structValueToSqlValue(value, sql.Float64Type)
	require.NoError(t, err, "Expected no error for Float64Type with NULL value")
	require.Equal(t, sql.NewNull(sql.AnyType), result, "Expected NULL value")

	// Test case for BooleanType
	value = &structpb.Value{
		Kind: &structpb.Value_BoolValue{BoolValue: true},
	}
	result, err = structValueToSqlValue(value, sql.BooleanType)
	require.NoError(t, err, "Expected no error for BooleanType")
	require.Equal(t, sql.NewBool(true), result, "Expected Boolean value")

	// Test case for BooleanType with NULL value
	value = &structpb.Value{
		Kind: &structpb.Value_NullValue{},
	}
	result, err = structValueToSqlValue(value, sql.BooleanType)
	require.NoError(t, err, "Expected no error for BooleanType with NULL value")
	require.Equal(t, sql.NewNull(sql.AnyType), result, "Expected NULL value")

	// Test case for unsupported type
	value = &structpb.Value{
		Kind: &structpb.Value_ListValue{},
	}
	result, err = structValueToSqlValue(value, "datetime")
	require.ErrorIs(t, err, ErrUnsupportedType, "Expected error for unsupported type")
	require.Nil(t, result, "Expected nil result for unsupported type")
}

func TestProtomodelValueTypeToSQLValueType(t *testing.T) {
	testCases := []struct {
		name      string
		valueType protomodel.FieldType
		sqlType   sql.SQLValueType
		expectErr error
	}{
		{
			name:      "string",
			valueType: protomodel.FieldType_STRING,
			sqlType:   sql.VarcharType,
			expectErr: nil,
		},
		{
			name:      "integer",
			valueType: protomodel.FieldType_INTEGER,
			sqlType:   sql.IntegerType,
			expectErr: nil,
		},
		{
			name:      "double",
			valueType: protomodel.FieldType_DOUBLE,
			sqlType:   sql.Float64Type,
			expectErr: nil,
		},
		{
			name:      "boolean",
			valueType: protomodel.FieldType_BOOLEAN,
			sqlType:   sql.BooleanType,
			expectErr: nil,
		},
		{
			name:      "unsupported",
			valueType: 999,
			sqlType:   "",
			expectErr: fmt.Errorf("%w(%d)", ErrUnsupportedType, 999),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlType, err := protomodelValueTypeToSQLValueType(tc.valueType)

			require.Equal(t, tc.expectErr, err)
			require.Equal(t, tc.sqlType, sqlType)
		})
	}
}

func TestSQLValueTypeDefaultLength(t *testing.T) {
	testCases := []struct {
		name      string
		valueType sql.SQLValueType
		length    int
		expectErr error
	}{
		{
			name:      "varchar",
			valueType: sql.VarcharType,
			length:    sql.MaxKeyLen,
			expectErr: nil,
		},
		{
			name:      "integer",
			valueType: sql.IntegerType,
			length:    0,
			expectErr: nil,
		},
		{
			name:      "blob",
			valueType: sql.BLOBType,
			length:    sql.MaxKeyLen,
			expectErr: nil,
		},
		{
			name:      "float64",
			valueType: sql.Float64Type,
			length:    0,
			expectErr: nil,
		},
		{
			name:      "boolean",
			valueType: sql.BooleanType,
			length:    0,
			expectErr: nil,
		},
		{
			name:      "unsupported",
			valueType: "unknown",
			length:    0,
			expectErr: fmt.Errorf("%w(%s)", ErrUnsupportedType, "unknown"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			length, err := sqlValueTypeDefaultLength(tc.valueType)

			require.Equal(t, tc.expectErr, err)
			require.Equal(t, tc.length, length)
		})
	}
}
