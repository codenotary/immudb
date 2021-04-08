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

type CountValue struct {
	c uint64
}

func (v *CountValue) Type() SQLValueType {
	return IntegerType
}

func (v *CountValue) Value() interface{} {
	return v.c
}

func (v *CountValue) Compare(val TypedValue) (CmpOperator, error) {
	ov, isNumber := val.(*Number)
	if !isNumber {
		return 0, ErrNotComparableValues
	}

	if v.c == ov.val {
		return EQ, nil
	}

	if v.c > ov.val {
		return GT, nil
	}

	return LT, nil
}

func (v *CountValue) IsAggregatedValue() bool {
	return true
}

func (v *CountValue) UpdateWith(val TypedValue) error {
	v.c++

	return nil
}
