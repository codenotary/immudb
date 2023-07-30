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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"

	"google.golang.org/protobuf/types/known/structpb"
)

var structValueToSqlValue = func(value *structpb.Value, sqlType sql.SQLValueType) (sql.ValueExp, error) {
	if _, ok := value.GetKind().(*structpb.Value_NullValue); ok {
		return sql.NewNull(sql.AnyType), nil
	}

	switch sqlType {
	case sql.VarcharType:
		_, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("%w: expecting value of type %s", ErrUnexpectedValue, sqlType)
		}

		return sql.NewVarchar(value.GetStringValue()), nil
	case sql.IntegerType:
		_, ok := value.GetKind().(*structpb.Value_NumberValue)
		if !ok {
			return nil, fmt.Errorf("%w: expecting value of type %s", ErrUnexpectedValue, sqlType)
		}
		return sql.NewInteger(int64(value.GetNumberValue())), nil
	case sql.BLOBType:
		_, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("%w: expecting value of type %s", ErrUnexpectedValue, sqlType)
		}

		docID, err := NewDocumentIDFromHexEncodedString(value.GetStringValue())
		if err != nil {
			return nil, err
		}

		return sql.NewBlob(docID[:]), nil
	case sql.Float64Type:
		_, ok := value.GetKind().(*structpb.Value_NumberValue)
		if !ok {
			return nil, fmt.Errorf("%w: expecting value of type %s", ErrUnexpectedValue, sqlType)
		}
		return sql.NewFloat64(value.GetNumberValue()), nil
	case sql.BooleanType:
		_, ok := value.GetKind().(*structpb.Value_BoolValue)
		if !ok {
			return nil, fmt.Errorf("%w: expecting value of type %s", ErrUnexpectedValue, sqlType)
		}
		return sql.NewBool(value.GetBoolValue()), nil
	}

	return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, sqlType)
}

var protomodelValueTypeToSQLValueType = func(stype protomodel.FieldType) (sql.SQLValueType, error) {
	switch stype {
	case protomodel.FieldType_STRING:
		return sql.VarcharType, nil
	case protomodel.FieldType_INTEGER:
		return sql.IntegerType, nil
	case protomodel.FieldType_DOUBLE:
		return sql.Float64Type, nil
	case protomodel.FieldType_BOOLEAN:
		return sql.BooleanType, nil
	}

	return "", fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
}

var sqlValueTypeDefaultLength = func(stype sql.SQLValueType) (int, error) {
	switch stype {
	case sql.VarcharType:
		return sql.MaxKeyLen, nil
	case sql.IntegerType:
		return 0, nil
	case sql.BLOBType:
		return sql.MaxKeyLen, nil
	case sql.Float64Type:
		return 0, nil
	case sql.BooleanType:
		return 0, nil
	}

	return 0, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
}

func kvMetadataToProto(kvMetadata *store.KVMetadata) *protomodel.DocumentMetadata {
	if kvMetadata == nil {
		return nil
	}

	return &protomodel.DocumentMetadata{
		Deleted: kvMetadata.Deleted(),
	}
}
