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

import "strconv"

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

func (v *CountValue) String() string {
	return strconv.FormatInt(v.c, 10)
}

func (v *CountValue) RawValue() interface{} {
	return v.c
}

func (v *CountValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.RawValue().(int64)

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

func (v *CountValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *CountValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (v *CountValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *CountValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return nil
}

func (v *CountValue) isConstant() bool {
	return false
}

func (v *CountValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type SumValue struct {
	val TypedValue
	sel string
}

func (v *SumValue) Selector() string {
	return v.sel
}

func (v *SumValue) ColBounded() bool {
	return true
}

func (v *SumValue) Type() SQLValueType {
	return v.val.Type()
}

func (v *SumValue) IsNull() bool {
	return v.val.IsNull()
}

func (v *SumValue) String() string {
	return v.val.String()
}

func (v *SumValue) RawValue() interface{} {
	return v.val.RawValue()
}

func (v *SumValue) Compare(val TypedValue) (int, error) {
	return v.val.Compare(val)
}

func (v *SumValue) updateWith(val TypedValue) error {
	if val.IsNull() {
		// Skip NULL values
		return nil
	}

	if !IsNumericType(val.Type()) {
		return ErrNumericTypeExpected
	}

	if v.val.IsNull() {
		// First non-null value
		v.val = val
		return nil
	}

	newVal, err := applyNumOperator(ADDOP, v.val, val)
	if err != nil {
		return err
	}

	v.val = newVal

	return nil
}

// ValueExp

func (v *SumValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *SumValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (v *SumValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *SumValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
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
	return v.val.IsNull()
}

func (v *MinValue) String() string {
	return v.val.String()
}

func (v *MinValue) RawValue() interface{} {
	return v.val.RawValue()
}

func (v *MinValue) Compare(val TypedValue) (int, error) {
	return v.val.Compare(val)
}

func (v *MinValue) updateWith(val TypedValue) error {
	if val.IsNull() {
		// Skip NULL values
		return nil
	}

	if v.val.IsNull() {
		// First non-null value
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

func (v *MinValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if v.val.IsNull() {
		return AnyType, ErrUnexpected
	}

	return v.val.Type(), nil
}

func (v *MinValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if v.val.IsNull() {
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

func (v *MinValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *MinValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
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
	return v.val.IsNull()
}

func (v *MaxValue) String() string {
	return v.val.String()
}

func (v *MaxValue) RawValue() interface{} {
	return v.val.RawValue()
}

func (v *MaxValue) Compare(val TypedValue) (int, error) {
	return v.val.Compare(val)
}

func (v *MaxValue) updateWith(val TypedValue) error {
	if val.IsNull() {
		// Skip NULL values
		return nil
	}

	if v.val.IsNull() {
		// First non-null value
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

func (v *MaxValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if v.val.IsNull() {
		return AnyType, ErrUnexpected
	}

	return v.val.Type(), nil
}

func (v *MaxValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if v.val.IsNull() {
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

func (v *MaxValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *MaxValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return nil
}

func (v *MaxValue) isConstant() bool {
	return false
}

func (v *MaxValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type AVGValue struct {
	s   TypedValue
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
	return v.s.Type()
}

func (v *AVGValue) IsNull() bool {
	return v.s.IsNull()
}

func (v *AVGValue) String() string {
	return v.calculate().String()
}

func (v *AVGValue) calculate() TypedValue {
	if v.s.IsNull() {
		return nil
	}

	val, err := applyNumOperator(DIVOP, v.s, &Integer{val: v.c})
	if err != nil {
		return &NullValue{t: AnyType}
	}

	return val
}

func (v *AVGValue) RawValue() interface{} {
	return v.calculate().RawValue()
}

func (v *AVGValue) Compare(val TypedValue) (int, error) {
	return v.calculate().Compare(val)
}

func (v *AVGValue) updateWith(val TypedValue) error {
	if val.IsNull() {
		// Skip NULL values
		return nil
	}

	if !IsNumericType(val.Type()) {
		return ErrNumericTypeExpected
	}

	if v.s.IsNull() {
		// First non-null value
		v.s = val
		v.c++
		return nil
	}

	newVal, err := applyNumOperator(ADDOP, v.s, val)
	if err != nil {
		return err
	}

	v.s = newVal
	v.c++
	return nil
}

// ValueExp

func (v *AVGValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *AVGValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
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

func (v *AVGValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (v *AVGValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return nil
}

func (v *AVGValue) isConstant() bool {
	return false
}

func (v *AVGValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}
