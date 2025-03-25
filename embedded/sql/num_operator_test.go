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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumOperator(t *testing.T) {

	t.Run("Successful operator", func(t *testing.T) {
		for _, d := range []struct {
			op NumOperator
			lv TypedValue
			rv TypedValue
			ev interface{}
		}{
			{ADDOP, &Integer{val: 1}, &Integer{val: 2}, int64(3)},
			{ADDOP, &Integer{val: 1}, &Float64{val: 2}, float64(3)},
			{ADDOP, &Float64{val: 1}, &Integer{val: 2}, float64(3)},
			{ADDOP, &Float64{val: 1}, &Float64{val: 2}, float64(3)},

			{SUBSOP, &Integer{val: 1}, &Integer{val: 2}, int64(-1)},
			{SUBSOP, &Integer{val: 1}, &Float64{val: 2}, float64(-1)},
			{SUBSOP, &Float64{val: 1}, &Integer{val: 2}, float64(-1)},
			{SUBSOP, &Float64{val: 1}, &Float64{val: 2}, float64(-1)},

			{DIVOP, &Integer{val: 10}, &Integer{val: 3}, int64(3)},
			{DIVOP, &Integer{val: 10}, &Float64{val: 3}, float64(10.0 / 3.0)},
			{DIVOP, &Float64{val: 10}, &Integer{val: 3}, float64(10.0 / 3.0)},
			{DIVOP, &Float64{val: 10}, &Float64{val: 3}, float64(10.0 / 3.0)},

			{MODOP, &Integer{val: 10}, &Integer{val: 3}, int64(1)},
			{MODOP, &Integer{val: 10}, &Float64{val: 3}, float64(1)},
			{MODOP, &Float64{val: 10}, &Integer{val: 3}, float64(1)},
			{MODOP, &Float64{val: 10.5}, &Float64{val: 3.2}, math.Mod(10.5, 3.2)},

			{MULTOP, &Integer{val: 10}, &Integer{val: 3}, int64(30)},
			{MULTOP, &Float64{val: 10}, &Integer{val: 3}, float64(30)},
			{MULTOP, &Integer{val: 10}, &Float64{val: 3}, float64(30)},
			{MULTOP, &Float64{val: 10}, &Float64{val: 3}, float64(30)},
		} {
			t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
				result, err := applyNumOperator(d.op, d.lv, d.rv)
				require.NoError(t, err)
				require.Equal(t, d.ev, result.RawValue())
			})
		}
	})

	t.Run("Division by 0", func(t *testing.T) {
		for _, d := range []struct {
			lv TypedValue
			rv TypedValue
		}{
			{&Integer{val: 100}, &Integer{val: 0}},
			{&Float64{val: 100}, &Integer{val: 0}},
			{&Integer{val: 100}, &Float64{val: 0}},
			{&Float64{val: 100}, &Float64{val: 0}},
		} {
			t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
				result, err := applyNumOperator(DIVOP, d.lv, d.rv)
				require.ErrorIs(t, err, ErrDivisionByZero)
				require.Nil(t, result)
			})
		}
	})

	t.Run("Incompatible types", func(t *testing.T) {
		for _, d := range []struct {
			lv TypedValue
			rv TypedValue
		}{
			{&Integer{val: 100}, &Bool{}},
			{&Float64{val: 100}, &Bool{}},
			{&Bool{}, &Integer{val: 100}},
			{&Bool{}, &Float64{val: 100}},
		} {
			t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
				result, err := applyNumOperator(ADDOP, d.lv, d.rv)
				require.ErrorIs(t, err, ErrInvalidValue)
				require.Nil(t, result)
			})
		}
	})

	t.Run("Invalid operation", func(t *testing.T) {
		for _, d := range []struct {
			lv TypedValue
			rv TypedValue
		}{
			{&Integer{val: 100}, &Integer{val: 1}},
			{&Float64{val: 100}, &Float64{val: 1}},
		} {
			t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
				result, err := applyNumOperator(NumOperator(-1), d.lv, d.rv)
				require.ErrorIs(t, err, ErrUnexpected)
				require.Nil(t, result)
			})
		}
	})

}
