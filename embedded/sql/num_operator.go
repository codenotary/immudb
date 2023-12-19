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

import "fmt"

func applyNumOperator(op NumOperator, vl, vr TypedValue) (TypedValue, error) {
	if vl.Type() == Float64Type || vr.Type() == Float64Type {
		return applyNumOperatorFloat64(op, vl, vr)
	}

	return applyNumOperatorInteger(op, vl, vr)
}

func applyNumOperatorInteger(op NumOperator, vl, vr TypedValue) (TypedValue, error) {
	convl, err := mayApplyImplicitConversion(vl.RawValue(), IntegerType)
	if err != nil {
		return nil, fmt.Errorf("%w (expecting numeric value)", err)
	}

	nl, isNumber := convl.(int64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	convr, err := mayApplyImplicitConversion(vr.RawValue(), IntegerType)
	if err != nil {
		return nil, fmt.Errorf("%w (expecting numeric value)", err)
	}

	nr, isNumber := convr.(int64)
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
	convl, err := mayApplyImplicitConversion(vl.RawValue(), Float64Type)
	if err != nil {
		return nil, fmt.Errorf("%w (expecting numeric value)", err)
	}

	nl, isNumber := convl.(float64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	convr, err := mayApplyImplicitConversion(vr.RawValue(), Float64Type)
	if err != nil {
		return nil, fmt.Errorf("%w (expecting numeric value)", err)
	}

	nr, isNumber := convr.(float64)
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
