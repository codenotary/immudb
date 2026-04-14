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

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type converterFunc func(TypedValue) (TypedValue, error)

func getConverter(src, dst SQLValueType) (converterFunc, error) {
	if src == dst {
		if src == JSONType {
			return jsonConverted(dst), nil
		}

		return func(tv TypedValue) (TypedValue, error) {
			return tv, nil
		}, nil
	}

	if src == AnyType {
		if dst == JSONType {
			return jsonConverted(dst), nil
		}

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
					"2006-01-02 15:04:05 MST",
					"2006-01-02 15:04:05 -0700",
					"2006-01-02 15:04:05.999999",
					"2006-01-02 15:04:05",
					"2006-01-02 15:04",
					"2006-01-02",
					time.RFC3339Nano,                // 2006-01-02T15:04:05.999999999Z07:00
					time.RFC3339,                    // 2006-01-02T15:04:05Z07:00
					"2006-01-02T15:04:05.999999999", // ISO-8601, no timezone
					"2006-01-02T15:04:05",           // ISO-8601, no fractional seconds, no timezone
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

		if src == JSONType {
			jsonToStr, err := getConverter(src, VarcharType)
			if err != nil {
				return nil, err
			}

			strToTimestamp, err := getConverter(VarcharType, TimestampType)
			if err != nil {
				return nil, err
			}

			return func(tv TypedValue) (TypedValue, error) {
				v, err := jsonToStr(tv)
				if err != nil {
					return nil, err
				}
				s, _ := v.RawValue().(string)
				return strToTimestamp(NewVarchar(strings.Trim(s, `"`)))
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

		if src == JSONType {
			return jsonConverted(dst), nil
		}

		return nil, fmt.Errorf(
			"%w: only INTEGER and VARCHAR types can be cast as FLOAT",
			ErrUnsupportedCast,
		)
	}

	if dst == BooleanType {
		if src == JSONType {
			return jsonConverted(dst), nil
		}

		if src == VarcharType {
			// Accept Postgres' text-format boolean literals (t/f, true/
			// false, y/n, yes/no, on/off, 0/1, case-insensitive). Any
			// SQL-level literal `'t'` inserted into a BOOLEAN column —
			// or a varchar parameter from a client that didn't infer
			// the column type — coerces here rather than failing with
			// "value is not a boolean".
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: BooleanType}, nil
				}
				s, _ := val.RawValue().(string)
				b, ok := parsePGTextBool(s)
				if !ok {
					return nil, fmt.Errorf(
						"%w: invalid boolean text value %q", ErrUnsupportedCast, s)
				}
				return &Bool{val: b}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: cannot cast %s to %s",
			ErrUnsupportedCast,
			src,
			dst,
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

		if src == JSONType {
			return jsonConverted(dst), nil
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

		if src == JSONType {
			return func(val TypedValue) (TypedValue, error) {
				jsonStr := val.String()
				return &Blob{val: []byte(jsonStr)}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: cannot cast type %s to BLOB",
			ErrUnsupportedCast,
			src,
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

		if src == JSONType {
			return jsonConverted(dst), nil
		}

		if src == IntegerType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: VarcharType}, nil
				}
				return &Varchar{val: strconv.FormatInt(val.RawValue().(int64), 10)}, nil
			}, nil
		}

		if src == Float64Type {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: VarcharType}, nil
				}
				return &Varchar{val: strconv.FormatFloat(val.RawValue().(float64), 'g', -1, 64)}, nil
			}, nil
		}

		if src == BooleanType {
			return func(val TypedValue) (TypedValue, error) {
				if val.RawValue() == nil {
					return &NullValue{t: VarcharType}, nil
				}
				return &Varchar{val: strconv.FormatBool(val.RawValue().(bool))}, nil
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: cannot cast %s to VARCHAR",
			ErrUnsupportedCast,
			src,
		)
	}

	if dst == JSONType {
		return func(tv TypedValue) (TypedValue, error) {
			if tv.RawValue() == nil {
				return &NullValue{t: JSONType}, nil
			}

			switch tv.Type() {
			case Float64Type, IntegerType, BooleanType, AnyType:
				return &JSON{val: tv.RawValue()}, nil
			case VarcharType:
				var x interface{}
				s := strings.TrimSuffix(strings.TrimPrefix(tv.String(), "'"), "'")

				err := json.Unmarshal([]byte(s), &x)
				return &JSON{val: x}, err
			case BLOBType:
				rawJson, ok := tv.RawValue().([]byte)
				if !ok {
					return nil, fmt.Errorf("invalid %s value", JSONType)
				}
				return NewJsonFromString(string(rawJson))
			}

			return nil, fmt.Errorf(
				"%w: can not cast %s value as %s",
				ErrUnsupportedCast,
				tv.Type(),
				JSONType,
			)
		}, nil
	}

	if dst == AnyType && src == JSONType {
		return func(tv TypedValue) (TypedValue, error) {
			if !tv.IsNull() {
				return &NullValue{t: AnyType}, nil
			}
			return nil, ErrInvalidValue
		}, nil
	}

	return nil, fmt.Errorf(
		"%w: can not cast %s value as %s",
		ErrUnsupportedCast,
		src,
		dst,
	)
}

func jsonConverted(t SQLValueType) converterFunc {
	return func(val TypedValue) (TypedValue, error) {
		if val.IsNull() {
			return &JSON{val: nil}, nil
		}

		jsonVal := val.(*JSON)
		if t == VarcharType {
			return NewVarchar(jsonVal.String()), nil
		}

		val, ok := jsonVal.castToTypedValue()
		if !ok {
			return nil, fmt.Errorf(
				"%w: can not cast JSON as %s",
				ErrUnsupportedCast,
				t,
			)
		}

		conv, err := getConverter(val.Type(), t)
		if err != nil {
			return nil, err
		}
		return conv(val)
	}
}

// parsePGTextBool recognises Postgres' text-format boolean values:
// t/true/y/yes/on/1 → true; f/false/n/no/off/0 → false (case-insensitive,
// surrounding whitespace tolerated). The pgsql wire layer has its own
// copy in `pkg/pgsql/server/types.go` for bind parameters; this engine-
// level copy lets a Varchar value (a SQL literal or an unannotated
// parameter) round-trip into a BOOLEAN column without the wire layer.
func parsePGTextBool(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true", "y", "yes", "on", "1":
		return true, true
	case "f", "false", "n", "no", "off", "0":
		return false, true
	}
	return false, false
}
