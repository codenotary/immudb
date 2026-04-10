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
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	CoalesceFnCall           string = "COALESCE"
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
	PGGetUserByIDFnCall          string = "PG_GET_USERBYID"
	PgTableIsVisibleFnCall       string = "PG_TABLE_IS_VISIBLE"
	PgShobjDescriptionFnCall     string = "SHOBJ_DESCRIPTION"
	CurrentDatabaseFnCall        string = "CURRENT_DATABASE"
	CurrentSchemaFnCall          string = "CURRENT_SCHEMA"
	CurrentUserFnCall            string = "CURRENT_USER"
	FormatTypeFnCall             string = "FORMAT_TYPE"
	PgGetExprFnCall              string = "PG_GET_EXPR"
	PgGetConstraintDefFnCall     string = "PG_GET_CONSTRAINTDEF"
	PgEncodingToCharFnCall       string = "PG_ENCODING_TO_CHAR"
	ObjDescriptionFnCall         string = "OBJ_DESCRIPTION"
	HasTablePrivilegeFnCall      string = "HAS_TABLE_PRIVILEGE"
	HasSchemaPrivilegeFnCall     string = "HAS_SCHEMA_PRIVILEGE"
	ArrayUpperFnCall             string = "ARRAY_UPPER"
	PgGetSerialSequenceFnCall    string = "PG_GET_SERIAL_SEQUENCE"
	ColDescriptionFnCall         string = "COL_DESCRIPTION"

	// Math functions
	AbsFnCall     string = "ABS"
	CeilFnCall    string = "CEIL"
	FloorFnCall   string = "FLOOR"
	RoundFnCall   string = "ROUND"
	PowerFnCall   string = "POWER"
	SqrtFnCall    string = "SQRT"
	ModFnCall     string = "MOD"
	SignFnCall    string = "SIGN"

	// String functions
	ReplaceFnCall  string = "REPLACE"
	ReverseFnCall  string = "REVERSE"
	LeftFnCall     string = "LEFT"
	RightFnCall    string = "RIGHT"
	RepeatFnCall   string = "REPEAT"
	PositionFnCall string = "POSITION"
	CharLengthFnCall string = "CHAR_LENGTH"
	OctetLengthFnCall string = "OCTET_LENGTH"

	// Conditional functions
	NullIfFnCall   string = "NULLIF"
	GreatestFnCall string = "GREATEST"
	LeastFnCall    string = "LEAST"
)

var builtinFunctions = map[string]Function{
	CoalesceFnCall:           &CoalesceFn{},
	LengthFnCall:             &LengthFn{},
	SubstringFnCall:          &SubstringFn{},
	ConcatFnCall:             &ConcatFn{},
	LowerFnCall:              &LowerUpperFnc{},
	UpperFnCall:              &LowerUpperFnc{isUpper: true},
	TrimFnCall:               &TrimFnc{},
	NowFnCall:                &NowFn{},
	UUIDFnCall:               &UUIDFn{},
	JSONTypeOfFnCall:         &JsonTypeOfFn{},
	PGGetUserByIDFnCall:          &pgGetUserByIDFunc{},
	PgTableIsVisibleFnCall:       &pgTableIsVisible{},
	PgShobjDescriptionFnCall:     &pgShobjDescription{},
	CurrentDatabaseFnCall:        &pgCurrentDatabase{},
	CurrentSchemaFnCall:          &pgCurrentSchema{},
	CurrentUserFnCall:            &pgCurrentUser{},
	FormatTypeFnCall:             &pgFormatType{},
	PgGetExprFnCall:              &pgVarcharStub{name: PgGetExprFnCall, nParams: -1},
	PgGetConstraintDefFnCall:     &pgVarcharStub{name: PgGetConstraintDefFnCall, nParams: -1},
	PgEncodingToCharFnCall:       &pgEncodingToChar{},
	ObjDescriptionFnCall:         &pgVarcharStub{name: ObjDescriptionFnCall, nParams: -1},
	HasTablePrivilegeFnCall:      &pgBoolStub{name: HasTablePrivilegeFnCall, nParams: -1},
	HasSchemaPrivilegeFnCall:     &pgBoolStub{name: HasSchemaPrivilegeFnCall, nParams: -1},
	ArrayUpperFnCall:             &pgNullIntStub{name: ArrayUpperFnCall},
	PgGetSerialSequenceFnCall:    &pgVarcharStub{name: PgGetSerialSequenceFnCall, nParams: -1},
	ColDescriptionFnCall:         &pgVarcharStub{name: ColDescriptionFnCall, nParams: -1},

	// Math functions
	AbsFnCall:     &mathFn{name: AbsFnCall},
	CeilFnCall:    &mathFn{name: CeilFnCall},
	FloorFnCall:   &mathFn{name: FloorFnCall},
	RoundFnCall:   &mathFn{name: RoundFnCall},
	PowerFnCall:   &mathFn{name: PowerFnCall},
	SqrtFnCall:    &mathFn{name: SqrtFnCall},
	ModFnCall:     &mathFn{name: ModFnCall},
	SignFnCall:    &mathFn{name: SignFnCall},

	// String functions
	ReplaceFnCall:    &replaceFn{},
	ReverseFnCall:    &reverseFn{},
	LeftFnCall:       &leftRightFn{isRight: false},
	RightFnCall:      &leftRightFn{isRight: true},
	RepeatFnCall:     &repeatFn{},
	PositionFnCall:   &positionFn{},
	CharLengthFnCall: &LengthFn{},
	OctetLengthFnCall: &LengthFn{},

	// Conditional functions
	NullIfFnCall:   &nullIfFn{},
	GreatestFnCall: &greatestLeastFn{isGreatest: true},
	LeastFnCall:    &greatestLeastFn{isGreatest: false},
}

type Function interface {
	RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error
	InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error)
	Apply(tx *SQLTx, params []TypedValue) (TypedValue, error)
}

type CoalesceFn struct{}

func (f *CoalesceFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return AnyType, nil
}

func (f *CoalesceFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return nil
}

func (f *CoalesceFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	t := AnyType

	for _, p := range params {
		if !p.IsNull() {
			if t == AnyType {
				t = p.Type()
			} else if p.Type() != t && !(IsNumericType(t) && IsNumericType(p.Type())) {
				return nil, fmt.Errorf("coalesce: %w", ErrInvalidTypes)
			}
		}
	}

	for _, p := range params {
		if !p.IsNull() {
			return p, nil
		}
	}
	return NewNull(t), nil
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

// -------------------------------------
// PostgreSQL Compatibility Functions
// -------------------------------------

// current_database() — returns "defaultdb" (immudb default database name)
type pgCurrentDatabase struct{}

func (f *pgCurrentDatabase) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgCurrentDatabase) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgCurrentDatabase) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) > 0 {
		return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, CurrentDatabaseFnCall, len(params))
	}
	return NewVarchar("defaultdb"), nil
}

// current_schema() — returns "public"
type pgCurrentSchema struct{}

func (f *pgCurrentSchema) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgCurrentSchema) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgCurrentSchema) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) > 0 {
		return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, CurrentSchemaFnCall, len(params))
	}
	return NewVarchar("public"), nil
}

// current_user — returns the logged-in username
type pgCurrentUser struct{}

func (f *pgCurrentUser) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgCurrentUser) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgCurrentUser) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) > 0 {
		return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, CurrentUserFnCall, len(params))
	}

	users, err := tx.ListUsers(tx.tx.Context())
	if err != nil {
		return NewVarchar("immudb"), nil
	}

	idx := findSysAdmin(users)
	if idx >= 0 {
		return NewVarchar(users[idx].Username()), nil
	}
	return NewVarchar("immudb"), nil
}

// format_type(oid, typmod) — maps type OID to type name
type pgFormatType struct{}

func (f *pgFormatType) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgFormatType) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

var oidToTypeName = map[int64]string{
	16:   "boolean",
	17:   "bytea",
	20:   "bigint",
	25:   "text",
	114:  "json",
	701:  "double precision",
	1114: "timestamp without time zone",
	2950: "uuid",
}

func (f *pgFormatType) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, FormatTypeFnCall, 2, len(params))
	}

	if params[0].IsNull() {
		return NewNull(VarcharType), nil
	}

	oid, ok := params[0].RawValue().(int64)
	if !ok {
		return NewVarchar("???"), nil
	}

	name, exists := oidToTypeName[oid]
	if !exists {
		return NewVarchar(fmt.Sprintf("unknown (OID=%d)", oid)), nil
	}
	return NewVarchar(name), nil
}

// pg_encoding_to_char(encoding_id) — returns encoding name
type pgEncodingToChar struct{}

func (f *pgEncodingToChar) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgEncodingToChar) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgEncodingToChar) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, PgEncodingToCharFnCall, 1, len(params))
	}
	return NewVarchar("UTF8"), nil
}

// pgVarcharStub — generic stub that returns empty string for any PG function.
// nParams=-1 means accept any number of parameters.
type pgVarcharStub struct {
	name    string
	nParams int
}

func (f *pgVarcharStub) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (f *pgVarcharStub) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (f *pgVarcharStub) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if f.nParams >= 0 && len(params) != f.nParams {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, f.name, f.nParams, len(params))
	}
	return NewVarchar(""), nil
}

// pgBoolStub — generic stub that returns true for any PG privilege-check function.
// nParams=-1 means accept any number of parameters.
type pgBoolStub struct {
	name    string
	nParams int
}

func (f *pgBoolStub) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (f *pgBoolStub) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return nil
}

func (f *pgBoolStub) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if f.nParams >= 0 && len(params) != f.nParams {
		return nil, fmt.Errorf("%w: '%s' function expects %d arguments but %d were provided", ErrIllegalArguments, f.name, f.nParams, len(params))
	}
	return NewBool(true), nil
}

// pgNullIntStub — returns NULL integer (for array_upper etc.)
type pgNullIntStub struct {
	name string
}

func (f *pgNullIntStub) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (f *pgNullIntStub) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}
	return nil
}

func (f *pgNullIntStub) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	return NewNull(IntegerType), nil
}

// -------------------------------------
// Math Functions
// -------------------------------------

type mathFn struct {
	name string
}

func (f *mathFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return Float64Type, nil
}

func (f *mathFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != Float64Type && t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as numeric type", ErrInvalidTypes, t)
	}
	return nil
}

func (f *mathFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	switch f.name {
	case AbsFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], math.Abs)
	case CeilFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], math.Ceil)
	case FloorFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], math.Floor)
	case RoundFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], math.Round)
	case SqrtFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], math.Sqrt)
	case SignFnCall:
		if len(params) != 1 {
			return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, f.name)
		}
		return f.applyUnary(params[0], func(v float64) float64 {
			if v > 0 {
				return 1
			}
			if v < 0 {
				return -1
			}
			return 0
		})
	case PowerFnCall:
		if len(params) != 2 {
			return nil, fmt.Errorf("%w: '%s' expects 2 arguments", ErrIllegalArguments, f.name)
		}
		return f.applyBinary(params[0], params[1], math.Pow)
	case ModFnCall:
		if len(params) != 2 {
			return nil, fmt.Errorf("%w: '%s' expects 2 arguments", ErrIllegalArguments, f.name)
		}
		return f.applyBinary(params[0], params[1], math.Mod)
	default:
		return nil, fmt.Errorf("%w: unknown math function '%s'", ErrIllegalArguments, f.name)
	}
}

func (f *mathFn) applyUnary(p TypedValue, fn func(float64) float64) (TypedValue, error) {
	if p.IsNull() {
		return NewNull(Float64Type), nil
	}

	var v float64
	switch raw := p.RawValue().(type) {
	case int64:
		v = float64(raw)
	case float64:
		v = raw
	default:
		return nil, fmt.Errorf("%w: '%s' expects a numeric argument", ErrIllegalArguments, f.name)
	}
	return NewFloat64(fn(v)), nil
}

func (f *mathFn) applyBinary(a, b TypedValue, fn func(float64, float64) float64) (TypedValue, error) {
	if a.IsNull() || b.IsNull() {
		return NewNull(Float64Type), nil
	}

	var va, vb float64
	switch raw := a.RawValue().(type) {
	case int64:
		va = float64(raw)
	case float64:
		va = raw
	default:
		return nil, fmt.Errorf("%w: '%s' expects numeric arguments", ErrIllegalArguments, f.name)
	}
	switch raw := b.RawValue().(type) {
	case int64:
		vb = float64(raw)
	case float64:
		vb = raw
	default:
		return nil, fmt.Errorf("%w: '%s' expects numeric arguments", ErrIllegalArguments, f.name)
	}
	return NewFloat64(fn(va, vb)), nil
}

// -------------------------------------
// Additional String Functions
// -------------------------------------

type replaceFn struct{}

func (f *replaceFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}
func (f *replaceFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}
func (f *replaceFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 3 {
		return nil, fmt.Errorf("%w: '%s' expects 3 arguments", ErrIllegalArguments, ReplaceFnCall)
	}
	if params[0].IsNull() {
		return NewNull(VarcharType), nil
	}
	s, _ := params[0].RawValue().(string)
	old, _ := params[1].RawValue().(string)
	new, _ := params[2].RawValue().(string)
	return NewVarchar(strings.ReplaceAll(s, old, new)), nil
}

type reverseFn struct{}

func (f *reverseFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}
func (f *reverseFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}
func (f *reverseFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, ReverseFnCall)
	}
	if params[0].IsNull() {
		return NewNull(VarcharType), nil
	}
	s, _ := params[0].RawValue().(string)
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return NewVarchar(string(runes)), nil
}

type leftRightFn struct {
	isRight bool
}

func (f *leftRightFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}
func (f *leftRightFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}
func (f *leftRightFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: function expects 2 arguments", ErrIllegalArguments)
	}
	if params[0].IsNull() || params[1].IsNull() {
		return NewNull(VarcharType), nil
	}
	s, _ := params[0].RawValue().(string)
	n, _ := params[1].RawValue().(int64)
	if n < 0 {
		n = 0
	}
	if int(n) > len(s) {
		return NewVarchar(s), nil
	}
	if f.isRight {
		return NewVarchar(s[len(s)-int(n):]), nil
	}
	return NewVarchar(s[:int(n)]), nil
}

type repeatFn struct{}

func (f *repeatFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}
func (f *repeatFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}
func (f *repeatFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: '%s' expects 2 arguments", ErrIllegalArguments, RepeatFnCall)
	}
	if params[0].IsNull() || params[1].IsNull() {
		return NewNull(VarcharType), nil
	}
	s, _ := params[0].RawValue().(string)
	n, _ := params[1].RawValue().(int64)
	if n <= 0 {
		return NewVarchar(""), nil
	}
	return NewVarchar(strings.Repeat(s, int(n))), nil
}

type positionFn struct{}

func (f *positionFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}
func (f *positionFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}
	return nil
}
func (f *positionFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: '%s' expects 2 arguments", ErrIllegalArguments, PositionFnCall)
	}
	if params[0].IsNull() || params[1].IsNull() {
		return NewNull(IntegerType), nil
	}
	substr, _ := params[0].RawValue().(string)
	s, _ := params[1].RawValue().(string)
	pos := strings.Index(s, substr)
	if pos < 0 {
		return NewInteger(0), nil
	}
	return NewInteger(int64(pos + 1)), nil // 1-based
}

// -------------------------------------
// Conditional Functions
// -------------------------------------

type nullIfFn struct{}

func (f *nullIfFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return AnyType, nil
}
func (f *nullIfFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return nil
}
func (f *nullIfFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) != 2 {
		return nil, fmt.Errorf("%w: '%s' expects 2 arguments", ErrIllegalArguments, NullIfFnCall)
	}
	r, err := params[0].Compare(params[1])
	if err != nil {
		return params[0], nil
	}
	if r == 0 {
		return NewNull(params[0].Type()), nil
	}
	return params[0], nil
}

type greatestLeastFn struct {
	isGreatest bool
}

func (f *greatestLeastFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return AnyType, nil
}
func (f *greatestLeastFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return nil
}
func (f *greatestLeastFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("%w: function expects at least 1 argument", ErrIllegalArguments)
	}
	result := params[0]
	for _, p := range params[1:] {
		if result.IsNull() {
			result = p
			continue
		}
		if p.IsNull() {
			continue
		}
		cmp, err := result.Compare(p)
		if err != nil {
			continue
		}
		if (f.isGreatest && cmp < 0) || (!f.isGreatest && cmp > 0) {
			result = p
		}
	}
	return result, nil
}
