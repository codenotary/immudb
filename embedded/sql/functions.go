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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	LengthFnCall             string = "LENGTH"
	SubstringFnCall          string = "SUBSTRING"
	ConcatFnCall             string = "CONCAT"
	LowerFnCall              string = "LOWER"
	UpperFnCall              string = "UPPER"
	TrimFnCall               string = "TRIM"
	NowFnCall                string = "NOW"
	UUIDFnCall               string = "RANDOM_UUID"
	DatabasesFnCall          string = "DATABASES"
	TablesFnCall             string = "TABLES"
	TableFnCall              string = "TABLE"
	UsersFnCall              string = "USERS"
	ColumnsFnCall            string = "COLUMNS"
	IndexesFnCall            string = "INDEXES"
	GrantsFnCall             string = "GRANTS"
	JSONTypeOfFnCall         string = "JSON_TYPEOF"
	PGGetUserByIDFnCall      string = "PG_GET_USERBYID"
	PgTableIsVisibleFnCall   string = "PG_TABLE_IS_VISIBLE"
	PgShobjDescriptionFnCall string = "SHOBJ_DESCRIPTION"
)

var builtinFunctions = map[string]Function{
	LengthFnCall:             &LengthFn{},
	SubstringFnCall:          &SubstringFn{},
	ConcatFnCall:             &ConcatFn{},
	LowerFnCall:              &LowerUpperFnc{},
	UpperFnCall:              &LowerUpperFnc{isUpper: true},
	TrimFnCall:               &TrimFnc{},
	NowFnCall:                &NowFn{},
	UUIDFnCall:               &UUIDFn{},
	JSONTypeOfFnCall:         &JsonTypeOfFn{},
	PGGetUserByIDFnCall:      &pgGetUserByIDFunc{},
	PgTableIsVisibleFnCall:   &pgTableIsVisible{},
	PgShobjDescriptionFnCall: &pgShobjDescription{},
}

type Function interface {
	RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error
	InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error)
	Apply(tx *SQLTx, params []TypedValue) (TypedValue, error)
}

// -------------------------------------
// String Functions
// -------------------------------------

type LengthFn struct{}

func (f *LengthFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (f *LengthFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *ConcatFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *ConcatFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *SubstringFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *SubstringFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *SubstringFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("%w: '%s' function does expects three argument but %d were provided", ErrIllegalArguments, SubstringFnCall, len(params))
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

	if pos-1 >= int64(len(s)) {
		return &Varchar{val: ""}, nil
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

func (f *LowerUpperFnc) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *LowerUpperFnc) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *TrimFnc) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *TrimFnc) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *NowFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return TimestampType, nil
}

func (f *NowFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *JsonTypeOfFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *JsonTypeOfFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (f *UUIDFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return UUIDType, nil
}

func (f *UUIDFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

// pg functions

type pgGetUserByIDFunc struct{}

func (f *pgGetUserByIDFunc) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}
	return nil
}

func (f *pgGetUserByIDFunc) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgGetUserByIDFunc) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, PGGetUserByIDFnCall, 1, len(params))
	}

	if params[0].RawValue() != int64(0) {
		return nil, fmt.Errorf("user not found")
	}

	users, err := tx.ListUsers(tx.tx.Context())
	if err != nil {
		return nil, err
	}

	idx := findSysAdmin(users)
	if idx < 0 {
		return nil, fmt.Errorf("admin not found")
	}
	return NewVarchar(users[idx].Username()), nil
}

func findSysAdmin(users []User) int {
	for i, u := range users {
		if u.Permission() == PermissionSysAdmin {
			return i
		}
	}
	return -1
}

type pgTableIsVisible struct{}

func (f *pgTableIsVisible) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return nil
}

func (f *pgTableIsVisible) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (f *pgTableIsVisible) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, PgTableIsVisibleFnCall, 1, len(params))
	}
	return NewBool(true), nil
}

type pgShobjDescription struct{}

func (f *pgShobjDescription) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgShobjDescription) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgShobjDescription) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, PgShobjDescriptionFnCall, 2, len(params))
	}
	return NewVarchar(""), nil
}
