/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
}

type CountValue struct {
	c   uint64
	sel string
}

func (v *CountValue) Selector() string {
	return v.sel
}

func (v *CountValue) Type() SQLValueType {
	return IntegerType
}

func (v *CountValue) Value() interface{} {
	return v.c
}

func (v *CountValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.Value().(uint64)

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

type SumValue struct {
	s   uint64
	sel string
}

func (v *SumValue) Selector() string {
	return v.sel
}

func (v *SumValue) Type() SQLValueType {
	return IntegerType
}

func (v *SumValue) Value() interface{} {
	return v.s
}

func (v *SumValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.Value().(uint64)

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

	v.s += val.Value().(uint64)

	return nil
}

type MinValue struct {
	val TypedValue
	sel string
}

func (v *MinValue) Selector() string {
	return v.sel
}

func (v *MinValue) Type() SQLValueType {
	return v.val.Type()
}

func (v *MinValue) Value() interface{} {
	return v.val.Value()
}

func (v *MinValue) Compare(val TypedValue) (int, error) {
	if val.Type() != val.Type() {
		return 0, ErrNotComparableValues
	}

	return v.val.Compare(val)
}

func (v *MinValue) updateWith(val TypedValue) error {
	if v.val == nil {
		v.val = val
		return nil
	}

	if val.Type() != val.Type() {
		return ErrNotComparableValues
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

type MaxValue struct {
	val TypedValue
	sel string
}

func (v *MaxValue) Selector() string {
	return v.sel
}

func (v *MaxValue) Type() SQLValueType {
	return v.val.Type()
}

func (v *MaxValue) Value() interface{} {
	return v.val.Value()
}

func (v *MaxValue) Compare(val TypedValue) (int, error) {
	if val.Type() != val.Type() {
		return 0, ErrNotComparableValues
	}

	return v.val.Compare(val)
}

func (v *MaxValue) updateWith(val TypedValue) error {
	if v.val == nil {
		v.val = val
		return nil
	}

	if val.Type() != val.Type() {
		return ErrNotComparableValues
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

type AVGValue struct {
	s   uint64
	c   uint64
	sel string
}

func (v *AVGValue) Selector() string {
	return v.sel
}

func (v *AVGValue) Type() SQLValueType {
	return IntegerType
}

func (v *AVGValue) Value() interface{} {
	return v.s / v.c
}

func (v *AVGValue) Compare(val TypedValue) (int, error) {
	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	nv := val.Value().(uint64)

	if v.s == nv {
		return 0, nil
	}

	if v.s > nv {
		return 1, nil
	}

	return -1, nil
}

func (v *AVGValue) updateWith(val TypedValue) error {
	if val.Type() != IntegerType {
		return ErrNotComparableValues
	}

	v.s += val.Value().(uint64)
	v.c++

	return nil
}
