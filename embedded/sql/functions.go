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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	LengthFnCall     string = "LENGTH"
	SubstringFnCall  string = "SUBSTRING"
	ConcatFnCall     string = "CONCAT"
	LowerFnCall      string = "LOWER"
	UpperFnCall      string = "UPPER"
	TrimFnCall       string = "TRIM"
	NowFnCall        string = "NOW"
	UUIDFnCall       string = "RANDOM_UUID"
	DatabasesFnCall  string = "DATABASES"
	TablesFnCall     string = "TABLES"
	TableFnCall      string = "TABLE"
	UsersFnCall      string = "USERS"
	ColumnsFnCall    string = "COLUMNS"
	IndexesFnCall    string = "INDEXES"
	GrantsFnCall     string = "GRANTS"
	JSONTypeOfFnCall string = "JSON_TYPEOF"
)

var builtinFunctions = map[string]Function{
	LengthFnCall:     &LengthFn{},
	SubstringFnCall:  &SubstringFn{},
	ConcatFnCall:     &ConcatFn{},
	LowerFnCall:      &LowerUpperFnc{},
	UpperFnCall:      &LowerUpperFnc{isUpper: true},
	TrimFnCall:       &TrimFnc{},
	NowFnCall:        &NowFn{},
	UUIDFnCall:       &UUIDFn{},
	JSONTypeOfFnCall: &JsonTypeOfFn{},
}

type Function interface {
	requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error
	inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error)
	Apply(tx *SQLTx, params []TypedValue) (TypedValue, error)
}

// -------------------------------------
// String Functions
// -------------------------------------

type LengthFn struct{}

func (f *LengthFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (f *LengthFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}
	return nil
}

func (f *LengthFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function does expects one argument but %d were provided", ErrIllegalArguments, LengthFnCall, len(params))
	}

	v := params[0]
	if v.IsNull() {
		return &NullValue{t: IntegerType}, nil
	}

	if v.Type() != VarcharType {
		return nil, fmt.Errorf("%w: '%s' function expects an argument of type %s", ErrIllegalArguments, LengthFnCall, VarcharType)
	}

	s, _ := v.RawValue().(string)
	return &Integer{val: int64(len(s))}, nil
}

type ConcatFn struct{}

func (f *ConcatFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *ConcatFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *ConcatFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("%w: '%s' function does expects at least one argument", ErrIllegalArguments, ConcatFnCall)
	}

	for _, v := range params {
		if v.Type() != AnyType && v.Type() != VarcharType {
			return nil, fmt.Errorf("%w: '%s' function doesn't accept arguments of type %s", ErrIllegalArguments, ConcatFnCall, v.Type())
		}
	}

	var builder strings.Builder
	for _, v := range params {
		s, _ := v.RawValue().(string)
		builder.WriteString(s)
	}
	return &Varchar{val: builder.String()}, nil
}

type SubstringFn struct {
}

func (f *SubstringFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *SubstringFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *SubstringFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("%w: '%s' function does expects one argument but %d were provided", ErrIllegalArguments, SubstringFnCall, len(params))
	}

	v1, v2, v3 := params[0], params[1], params[2]

	if v1.IsNull() || v2.IsNull() || v3.IsNull() {
		return &NullValue{t: VarcharType}, nil
	}

	s, _ := v1.RawValue().(string)
	pos, _ := v2.RawValue().(int64)
	length, _ := v3.RawValue().(int64)

	if pos <= 0 {
		return nil, fmt.Errorf("%w: parameter 'position' must be greater than zero", ErrIllegalArguments)
	}

	if length < 0 {
		return nil, fmt.Errorf("%w: parameter 'length' cannot be negative", ErrIllegalArguments)
	}

	end := pos - 1 + length
	if end > int64(len(s)) {
		end = int64(len(s))
	}
	return &Varchar{val: s[pos-1 : end]}, nil
}

type LowerUpperFnc struct {
	isUpper bool
}

func (f *LowerUpperFnc) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *LowerUpperFnc) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *LowerUpperFnc) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function does expects one argument but %d were provided", ErrIllegalArguments, f.name(), len(params))
	}

	v := params[0]
	if v.IsNull() {
		return &NullValue{t: VarcharType}, nil
	}

	if v.Type() != VarcharType {
		return nil, fmt.Errorf("%w: '%s' function expects an argument of type %s", ErrIllegalArguments, f.name(), VarcharType)
	}

	s, _ := v.RawValue().(string)

	var res string
	if f.isUpper {
		res = strings.ToUpper(s)
	} else {
		res = strings.ToLower(s)
	}
	return &Varchar{val: res}, nil
}

func (f *LowerUpperFnc) name() string {
	if f.isUpper {
		return UpperFnCall
	}
	return LowerFnCall
}

type TrimFnc struct {
}

func (f *TrimFnc) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *TrimFnc) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *TrimFnc) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function does expects one argument but %d were provided", ErrIllegalArguments, TrimFnCall, len(params))
	}

	v := params[0]
	if v.IsNull() {
		return &NullValue{t: VarcharType}, nil
	}

	if v.Type() != VarcharType {
		return nil, fmt.Errorf("%w: '%s' function expects an argument of type %s", ErrIllegalArguments, TrimFnCall, VarcharType)
	}

	s, _ := v.RawValue().(string)
	return &Varchar{val: strings.Trim(s, " \t\n\r\v\f")}, nil
}

// -------------------------------------
// Time Functions
// -------------------------------------

type NowFn struct{}

func (f *NowFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return TimestampType, nil
}

func (f *NowFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != TimestampType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
	}
	return nil
}

func (f *NowFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) > 0 {
		return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, NowFnCall, len(params))
	}
	return &Timestamp{val: tx.Timestamp().Truncate(time.Microsecond).UTC()}, nil
}

// -------------------------------------
// JSON Functions
// -------------------------------------

type JsonTypeOfFn struct{}

func (f *JsonTypeOfFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *JsonTypeOfFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *JsonTypeOfFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, JSONTypeOfFnCall, 1, len(params))
	}

	v := params[0]
	if v.IsNull() {
		return NewNull(AnyType), nil
	}

	jsonVal, ok := v.(*JSON)
	if !ok {
		return nil, fmt.Errorf("%w: '%s' function expects an argument of type JSON", ErrIllegalArguments, JSONTypeOfFnCall)
	}
	return NewVarchar(jsonVal.primitiveType()), nil
}

// -------------------------------------
// UUID Functions
// -------------------------------------

type UUIDFn struct{}

func (f *UUIDFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return UUIDType, nil
}

func (f *UUIDFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != UUIDType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, UUIDType, t)
	}
	return nil
}

func (f *UUIDFn) Apply(_ *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) > 0 {
		return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, UUIDFnCall, len(params))
	}
	return &UUID{val: uuid.New()}, nil
}
