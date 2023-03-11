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

import "fmt"

func applyNumOperator(op NumOperator, vl, vr TypedValue) (TypedValue, error) {
	if vl.Type() == Float64Type || vr.Type() == Float64Type {
		return applyNumOperatorFloat64(op, vl, vr)
	}

	return applyNumOperatorInteger(op, vl, vr)
}

func applyNumOperatorInteger(op NumOperator, vl, vr TypedValue) (TypedValue, error) {
	nl, isNumber := vl.Value().(int64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	nr, isNumber := vr.Value().(int64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	switch op {
	case ADDOP:
		{
			return &Integer{val: nl + nr}, nil
		}
	case SUBSOP:
		{
			return &Integer{val: nl - nr}, nil
		}
	case DIVOP:
		{
			if nr == 0 {
				return nil, ErrDivisionByZero
			}

			return &Integer{val: nl / nr}, nil
		}
	case MULTOP:
		{
			return &Integer{val: nl * nr}, nil
		}
	}

	return nil, ErrUnexpected
}

func applyNumOperatorFloat64(op NumOperator, vl, vr TypedValue) (TypedValue, error) {
	nl, isNumber := applyImplicitConversion(vl, Float64Type).(float64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	nr, isNumber := applyImplicitConversion(vr, Float64Type).(float64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	switch op {
	case ADDOP:
		{
			return &Float64{val: nl + nr}, nil
		}
	case SUBSOP:
		{
			return &Float64{val: nl - nr}, nil
		}
	case DIVOP:
		{
			if nr == 0 {
				return nil, ErrDivisionByZero
			}

			return &Float64{val: nl / nr}, nil
		}
	case MULTOP:
		{
			return &Float64{val: nl * nr}, nil
		}
	}

	return nil, ErrUnexpected
}
