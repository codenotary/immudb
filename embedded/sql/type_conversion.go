/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"strconv"
	"time"

	"github.com/google/uuid"
)

type converterFunc func(TypedValue) (TypedValue, error)

func getConverter(src, dst SQLValueType) (converterFunc, error) {
	if src == dst {
		return func(tv TypedValue) (TypedValue, error) {
			return tv, nil
		}, nil
	}

	if src == AnyType {
		return func(val TypedValue) (TypedValue, error) {
			if val.RawValue() == nil {
				return &NullValue{t: dst}, nil
			}
			return nil, ErrInvalidValue
		}, nil
	}

	if dst == TimestampType {

		if src == IntegerType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: TimestampType}, nil
				}
				return &Timestamp{val: time.Unix(val.RawValue().(int64), 0).Truncate(time.Microsecond).UTC()}, nil
			}, nil
		}

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: TimestampType}, nil
				}

				str := val.RawValue().(string)

				var supportedTimeFormats = []string{
					"2006-01-02 15:04:05.999999",
					"2006-01-02 15:04",
					"2006-01-02",
				}

				for _, layout := range supportedTimeFormats {
					t, err := time.ParseInLocation(layout, str, time.UTC)
					if err == nil {
						return &Timestamp{val: t.Truncate(time.Microsecond).UTC()}, nil
					}
				}

				if len(str) > 30 {
					str = str[:30] + "..."
				}

				return nil, fmt.Errorf(
					"%w: can not cast string '%s' as a TIMESTAMP",
					ErrUnsupportedCast,
					str,
				)
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only INTEGER and VARCHAR types can be cast as TIMESTAMP",
			ErrUnsupportedCast,
		)

	}

	if dst == Float64Type {

		if src == IntegerType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: Float64Type}, nil
				}
				return &Float64{val: float64(val.RawValue().(int64))}, nil
			}, nil
		}

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: Float64Type}, nil
				}

				s, err := strconv.ParseFloat(val.RawValue().(string), 64)
				if err != nil {
					return nil, fmt.Errorf(
						"%w: can not cast string '%s' as a FLOAT",
						ErrUnsupportedCast,
						val.RawValue().(string),
					)
				}
				return &Float64{val: s}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only INTEGER and VARCHAR types can be cast as FLOAT",
			ErrUnsupportedCast,
		)

	}

	if dst == IntegerType {

		if src == Float64Type {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: IntegerType}, nil
				}
				return &Integer{val: int64(val.RawValue().(float64))}, nil
			}, nil
		}

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: IntegerType}, nil
				}

				s, err := strconv.ParseInt(val.RawValue().(string), 10, 64)
				if err != nil {
					return nil, fmt.Errorf(
						"%w: can not cast string '%s' as a INTEGER",
						ErrUnsupportedCast,
						val.RawValue().(string),
					)
				}
				return &Integer{val: s}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only INTEGER and VARCHAR types can be cast as INTEGER",
			ErrUnsupportedCast,
		)

	}

	if dst == UUIDType {

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: UUIDType}, nil
				}

				strVal := val.RawValue().(string)

				u, err := uuid.Parse(strVal)
				if err != nil {
					return nil, fmt.Errorf(
						"%w: can not cast string '%s' as an UUID",
						ErrUnsupportedCast,
						val.RawValue().(string),
					)
				}

				return &UUID{val: u}, nil
			}, nil
		}

		if src == BLOBType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: UUIDType}, nil
				}

				bs := val.RawValue().([]byte)

				u, err := uuid.FromBytes(bs)
				if err != nil {
					return nil, fmt.Errorf(
						"%w: can not cast blob '%s' as an UUID",
						ErrUnsupportedCast,
						val.RawValue().(string),
					)
				}

				return &UUID{val: u}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only BLOB and VARCHAR types can be cast as UUID",
			ErrUnsupportedCast,
		)

	}

	if dst == BLOBType {

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: BLOBType}, nil
				}

				strVal := val.RawValue().(string)

				return &Blob{val: []byte(strVal)}, nil
			}, nil
		}

		if src == UUIDType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: BLOBType}, nil
				}

				u := val.RawValue().(uuid.UUID)

				return &Blob{val: u[:]}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only UUID and VARCHAR types can be cast as BLOB",
			ErrUnsupportedCast,
		)

	}

	if dst == VarcharType {

		if src == UUIDType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: VarcharType}, nil
				}

				u := val.RawValue().(uuid.UUID)

				return &Varchar{val: u.String()}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only UUID type can be cast as VARCHAR",
			ErrUnsupportedCast,
		)

	}

	return nil, fmt.Errorf(
		"%w: can not cast %s value as %s",
		ErrUnsupportedCast,
		src, dst,
	)
}
