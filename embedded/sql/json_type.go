package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	JSONTypeNumber = "NUMBER"
	JSONTypeBool   = "BOOL"
	JSONTypeString = "STRING"
	JSONTypeArray  = "ARRAY"
	JSONTypeObject = "OBJECT"
	JSONTypeNull   = "NULL"
)

type JSON struct {
	val interface{}
}

func NewJsonFromString(s string) (*JSON, error) {
	var val interface{}
	if err := json.Unmarshal([]byte(s), &val); err != nil {
		return nil, err
	}
	return &JSON{val: val}, nil
}

func NewJson(val interface{}) *JSON {
	return &JSON{val: val}
}

func (v *JSON) Type() SQLValueType {
	return JSONType
}

func (v *JSON) IsNull() bool {
	return false
}

func (v *JSON) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return JSONType, nil
}

func (v *JSON) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	ok := t == JSONType
	switch t {
	case IntegerType, Float64Type:
		_, isInt := v.val.(int64)
		_, isFloat := v.val.(float64)
		ok = isInt || (isFloat && t == Float64Type)
	case VarcharType:
		_, ok = v.val.(string)
	case BooleanType:
		_, ok = v.val.(bool)
	case AnyType:
		ok = v.val == nil
	}

	if !ok {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, JSONType, t)
	}
	return nil
}

func (v *JSON) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *JSON) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *JSON) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *JSON) isConstant() bool {
	return true
}

func (v *JSON) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *JSON) RawValue() interface{} {
	return v.val
}

func (v *JSON) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return val.Compare(v)
	}

	tv, ok := v.castToTypedValue()
	if !ok {
		return -1, fmt.Errorf("%w: comparison not defined for JSON %s", ErrNotComparableValues, v.primitiveType())
	}

	if val.Type() != JSONType {
		return tv.Compare(val)
	}

	res, err := val.Compare(tv)
	return -res, err
}

func (v *JSON) primitiveType() string {
	switch v.val.(type) {
	case int64, float64:
		return JSONTypeNumber
	case string:
		return JSONTypeString
	case bool:
		return JSONTypeBool
	case nil:
		return JSONTypeNull
	case []interface{}:
		return JSONTypeArray
	}
	return JSONTypeObject
}

func (v *JSON) castToTypedValue() (TypedValue, bool) {
	var tv TypedValue
	switch val := v.val.(type) {
	case int64:
		tv = NewInteger(val)
	case string:
		tv = NewVarchar(val)
	case float64:
		tv = NewFloat64(val)
	case bool:
		tv = NewBool(val)
	case nil:
		tv = NewNull(JSONType)
	default:
		return nil, false
	}
	return tv, true
}

func (v *JSON) String() string {
	data, _ := json.Marshal(v.val)
	return string(data)
}

func (v *JSON) lookup(fields []string) TypedValue {
	currVal := v.val
	for i, field := range fields {
		switch cv := currVal.(type) {
		case map[string]interface{}:
			v, hasField := cv[field]
			if !hasField || (v == nil && i < len(field)-1) {
				return NewNull(AnyType)
			}
			currVal = v

			if currVal == nil {
				break
			}
		case []interface{}:
			idx, err := strconv.ParseInt(field, 10, 64)
			if err != nil || idx < 0 || idx >= int64(len(cv)) {
				return NewNull(AnyType)
			}
			currVal = cv[idx]
		default:
			return NewNull(AnyType)
		}
	}
	return NewJson(currVal)
}

type JSONSelector struct {
	*ColSelector
	fields []string
}

func (sel *JSONSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (v *JSONSelector) alias() string {
	if v.ColSelector.as != "" {
		return v.ColSelector.as
	}
	return v.String()
}

func (v *JSONSelector) resolve(implicitTable string) (string, string, string) {
	aggFn, table, _ := v.ColSelector.resolve(implicitTable)
	return aggFn, table, v.String()
}

func (v *JSONSelector) String() string {
	return fmt.Sprintf("%s->'%s'", v.ColSelector.col, strings.Join(v.fields, "->"))
}

func (sel *JSONSelector) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	val, err := sel.ColSelector.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	jsonVal, ok := val.(*JSON)
	if !ok {
		return val, fmt.Errorf("-> operator cannot be applied on column of type %s", val.Type())
	}
	return jsonVal.lookup(sel.fields), nil
}

func (sel *JSONSelector) reduceSelectors(row *Row, implicitTable string) ValueExp {
	val := sel.ColSelector.reduceSelectors(row, implicitTable)

	jsonVal, ok := val.(*JSON)
	if !ok {
		return sel
	}
	return jsonVal.lookup(sel.fields)
}
