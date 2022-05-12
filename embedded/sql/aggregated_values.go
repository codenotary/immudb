/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

type AggregatedValue interface {
	TypedValue
	updateWith(val TypedValue) error
	Selector() string
	ColBounded() bool
}

type CountValue struct {
	c   int64
	sel string
}

func (v *CountValue) Selector() string {
	return v.sel
}

func (v *CountValue) ColBounded() bool {
	return false
}

func (v *CountValue) Type() SQLValueType {
	return IntegerType
}

func (v *CountValue) IsNull() bool {
	return false
}

func (v *CountValue) Value() interface{} {
	return v.c
}

func (v *CountValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.Value().(int64)

	if v.c == nv {
		return 0, nil
	}

	if v.c > nv {
		return 1, nil
	}

	return -1, nil
}

func (v *CountValue) updateWith(val TypedValue) error {
	v.c++
	return nil
}

// ValueExp

func (v *CountValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *CountValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return ErrNotComparableValues
	}
	return nil
}

func (v *CountValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrUnexpected
}

func (v *CountValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return nil, ErrUnexpected
}

func (v *CountValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *CountValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return nil
}

func (v *CountValue) isConstant() bool {
	return false
}

func (v *CountValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type SumValue struct {
	s   int64
	sel string
}

func (v *SumValue) Selector() string {
	return v.sel
}

func (v *SumValue) ColBounded() bool {
	return true
}

func (v *SumValue) Type() SQLValueType {
	return IntegerType
}

func (v *SumValue) IsNull() bool {
	return false
}

func (v *SumValue) Value() interface{} {
	return v.s
}

func (v *SumValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.Value().(int64)

	if v.s == nv {
		return 0, nil
	}

	if v.s > nv {
		return 1, nil
	}

	return -1, nil
}

func (v *SumValue) updateWith(val TypedValue) error {
	if val.Type() != IntegerType {
		return ErrNotComparableValues
	}

	v.s += val.Value().(int64)

	return nil
}

// ValueExp

func (v *SumValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *SumValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return ErrNotComparableValues
	}
	return nil
}

func (v *SumValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrUnexpected
}

func (v *SumValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return nil, ErrUnexpected
}

func (v *SumValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *SumValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *SumValue) isConstant() bool {
	return false
}

func (v *SumValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type MinValue struct {
	val TypedValue
	sel string
}

func (v *MinValue) Selector() string {
	return v.sel
}

func (v *MinValue) ColBounded() bool {
	return true
}

func (v *MinValue) Type() SQLValueType {
	return v.val.Type()
}

func (v *MinValue) IsNull() bool {
	return false
}

func (v *MinValue) Value() interface{} {
	return v.val.Value()
}

func (v *MinValue) Compare(val TypedValue) (int, error) {
	return v.val.Compare(val)
}

func (v *MinValue) updateWith(val TypedValue) error {
	if v.val == nil {
		v.val = val
		return nil
	}

	cmp, err := v.val.Compare(val)
	if err != nil {
		return err
	}

	if cmp == 1 {
		v.val = val
	}

	return nil
}

// ValueExp

func (v *MinValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if v.val == nil {
		return AnyType, ErrUnexpected
	}

	return v.val.Type(), nil
}

func (v *MinValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if v.val == nil {
		return ErrUnexpected
	}

	if t != v.val.Type() {
		return ErrNotComparableValues
	}

	return nil
}

func (v *MinValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrUnexpected
}

func (v *MinValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return nil, ErrUnexpected
}

func (v *MinValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *MinValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return nil
}

func (v *MinValue) isConstant() bool {
	return false
}

func (v *MinValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type MaxValue struct {
	val TypedValue
	sel string
}

func (v *MaxValue) Selector() string {
	return v.sel
}

func (v *MaxValue) ColBounded() bool {
	return true
}

func (v *MaxValue) Type() SQLValueType {
	return v.val.Type()
}

func (v *MaxValue) IsNull() bool {
	return false
}

func (v *MaxValue) Value() interface{} {
	return v.val.Value()
}

func (v *MaxValue) Compare(val TypedValue) (int, error) {
	return v.val.Compare(val)
}

func (v *MaxValue) updateWith(val TypedValue) error {
	if v.val == nil {
		v.val = val
		return nil
	}

	cmp, err := v.val.Compare(val)
	if err != nil {
		return err
	}

	if cmp == -1 {
		v.val = val
	}

	return nil
}

// ValueExp

func (v *MaxValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if v.val == nil {
		return AnyType, ErrUnexpected
	}

	return v.val.Type(), nil
}

func (v *MaxValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if v.val == nil {
		return ErrUnexpected
	}

	if t != v.val.Type() {
		return ErrNotComparableValues
	}

	return nil
}

func (v *MaxValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrUnexpected
}

func (v *MaxValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return nil, ErrUnexpected
}

func (v *MaxValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *MaxValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return nil
}

func (v *MaxValue) isConstant() bool {
	return false
}

func (v *MaxValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type AVGValue struct {
	s   int64
	c   int64
	sel string
}

func (v *AVGValue) Selector() string {
	return v.sel
}

func (v *AVGValue) ColBounded() bool {
	return true
}

func (v *AVGValue) Type() SQLValueType {
	return IntegerType
}

func (v *AVGValue) IsNull() bool {
	return false
}

func (v *AVGValue) Value() interface{} {
	return v.s / v.c
}

func (v *AVGValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	avg := v.s / v.c
	nv := val.Value().(int64)

	if avg == nv {
		return 0, nil
	}

	if avg > nv {
		return 1, nil
	}

	return -1, nil
}

func (v *AVGValue) updateWith(val TypedValue) error {
	if val.Type() != IntegerType {
		return ErrNotComparableValues
	}

	v.s += val.Value().(int64)
	v.c++

	return nil
}

// ValueExp

func (v *AVGValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *AVGValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return ErrNotComparableValues
	}

	return nil
}

func (v *AVGValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrUnexpected
}

func (v *AVGValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return nil, ErrUnexpected
}

func (v *AVGValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *AVGValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return nil
}

func (v *AVGValue) isConstant() bool {
	return false
}

func (v *AVGValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}
