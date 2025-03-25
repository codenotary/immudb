/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

import "github.com/google/uuid"

// mayApplyImplicitConversion may do an implicit type conversion
// implicit conversion is currently done in a subset of possible explicit conversions i.e. CAST
func mayApplyImplicitConversion(val interface{}, requiredColumnType SQLValueType) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	var converter converterFunc
	var typedVal TypedValue
	var err error

	switch requiredColumnType {
	case Float64Type:
		switch value := val.(type) {
		case float64:
			return val, nil
		case int:
			converter, err = getConverter(IntegerType, Float64Type)
			if err != nil {
				return nil, err
			}

			typedVal = &Integer{val: int64(value)}
		case int64:
			converter, err = getConverter(IntegerType, Float64Type)
			if err != nil {
				return nil, err
			}

			typedVal = &Integer{val: value}
		case string:
			converter, err = getConverter(VarcharType, Float64Type)
			if err != nil {
				return nil, err
			}

			typedVal = &Varchar{val: value}
		}
	case IntegerType:
		switch value := val.(type) {
		case int64:
			return val, nil
		case float64:
			converter, err = getConverter(Float64Type, IntegerType)
			if err != nil {
				return nil, err
			}

			typedVal = &Float64{val: value}
		case string:
			converter, err = getConverter(VarcharType, IntegerType)
			if err != nil {
				return nil, err
			}

			typedVal = &Varchar{val: value}
		}
	case UUIDType:
		switch value := val.(type) {
		case uuid.UUID:
			return val, nil
		case string:
			converter, err = getConverter(VarcharType, UUIDType)
			if err != nil {
				return nil, err
			}

			typedVal = &Varchar{val: value}
		case []byte:
			converter, err = getConverter(BLOBType, UUIDType)
			if err != nil {
				return nil, err
			}

			typedVal = &Blob{val: value}
		}
	default:
		// No implicit conversion rule found, do not convert at all
		return val, nil
	}

	if typedVal == nil {
		// No implicit conversion rule found, do not convert at all
		return val, nil
	}

	convVal, err := converter(typedVal)
	if err != nil {
		return nil, err
	}

	return convVal.RawValue(), nil
}
