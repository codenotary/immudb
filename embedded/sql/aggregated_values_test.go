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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountValue(t *testing.T) {
	cval := &CountValue{}
	require.Equal(t, "", cval.Selector())
	require.False(t, cval.ColBounded())
	require.False(t, cval.IsNull())

	err := cval.updateWith(&Bool{val: true})
	require.NoError(t, err)

	require.Equal(t, IntegerType, cval.Type())

	_, err = cval.Compare(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	cmp, err := cval.Compare(&Integer{val: 1})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	err = cval.updateWith(&Bool{val: true})
	require.NoError(t, err)

	cmp, err = cval.Compare(&Integer{val: 1})
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	cmp, err = cval.Compare(&Integer{val: 3})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// ValueExp

	sqlt, err := cval.inferType(nil, nil, "table1")
	require.NoError(t, err)
	require.Equal(t, IntegerType, sqlt)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.NoError(t, err)

	err = cval.requiresType(BooleanType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrNotComparableValues)

	_, err = cval.jointColumnTo(nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.substitute(nil)
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.reduce(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	require.Nil(t, cval.reduceSelectors(nil, "table1"))

	require.False(t, cval.isConstant())

	require.Nil(t, cval.selectorRanges(nil, "", nil, nil))
}

func TestSumValue(t *testing.T) {
	cval := &SumValue{
		val: &Integer{},
		sel: "db1.table1.amount",
	}
	require.Equal(t, "db1.table1.amount", cval.Selector())
	require.True(t, cval.ColBounded())
	require.False(t, cval.IsNull())

	err := cval.updateWith(&Integer{val: 1})
	require.NoError(t, err)

	require.Equal(t, IntegerType, cval.Type())

	_, err = cval.Compare(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	cmp, err := cval.Compare(&Integer{val: 1})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	err = cval.updateWith(&Bool{val: true})
	require.ErrorIs(t, err, ErrNumericTypeExpected)

	err = cval.updateWith(&Integer{val: 10})
	require.NoError(t, err)

	cmp, err = cval.Compare(&Integer{val: 10})
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	cmp, err = cval.Compare(&Integer{val: 12})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// ValueExp

	sqlt, err := cval.inferType(nil, nil, "table1")
	require.NoError(t, err)
	require.Equal(t, IntegerType, sqlt)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.NoError(t, err)

	err = cval.requiresType(BooleanType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrNotComparableValues)

	_, err = cval.jointColumnTo(nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.substitute(nil)
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.reduce(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	require.Equal(t, cval, cval.reduceSelectors(nil, "table1"))

	require.False(t, cval.isConstant())

	require.Nil(t, cval.selectorRanges(nil, "", nil, nil))
}

func TestMinValue(t *testing.T) {
	cval := &MinValue{
		val: &NullValue{},
		sel: "db1.table1.amount",
	}
	require.Equal(t, "db1.table1.amount", cval.Selector())
	require.True(t, cval.ColBounded())
	require.True(t, cval.IsNull())

	_, err := cval.inferType(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	err = cval.updateWith(&Integer{val: 10})
	require.NoError(t, err)

	require.Equal(t, IntegerType, cval.Type())

	cmp, err := cval.Compare(&Integer{val: 10})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	_, err = cval.Compare(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = cval.updateWith(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = cval.updateWith(&Integer{val: 2})
	require.NoError(t, err)

	cmp, err = cval.Compare(&Integer{val: 2})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = cval.Compare(&Integer{val: 4})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// ValueExp

	sqlt, err := cval.inferType(nil, nil, "table1")
	require.NoError(t, err)
	require.Equal(t, IntegerType, sqlt)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.NoError(t, err)

	err = cval.requiresType(BooleanType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrNotComparableValues)

	_, err = cval.jointColumnTo(nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.substitute(nil)
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.reduce(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	require.Nil(t, cval.reduceSelectors(nil, "table1"))

	require.False(t, cval.isConstant())

	require.Nil(t, cval.selectorRanges(nil, "", nil, nil))
}

func TestMaxValue(t *testing.T) {
	cval := &MaxValue{
		val: &NullValue{},
		sel: "db1.table1.amount",
	}
	require.Equal(t, "db1.table1.amount", cval.Selector())
	require.True(t, cval.ColBounded())
	require.True(t, cval.IsNull())

	_, err := cval.inferType(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	err = cval.updateWith(&Integer{val: 10})
	require.NoError(t, err)

	require.Equal(t, IntegerType, cval.Type())

	cmp, err := cval.Compare(&Integer{val: 10})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	_, err = cval.Compare(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = cval.updateWith(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = cval.updateWith(&Integer{val: 2})
	require.NoError(t, err)

	cmp, err = cval.Compare(&Integer{val: 2})
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	cmp, err = cval.Compare(&Integer{val: 11})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// ValueExp

	sqlt, err := cval.inferType(nil, nil, "table1")
	require.NoError(t, err)
	require.Equal(t, IntegerType, sqlt)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.NoError(t, err)

	err = cval.requiresType(BooleanType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrNotComparableValues)

	_, err = cval.jointColumnTo(nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.substitute(nil)
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.reduce(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	require.Nil(t, cval.reduceSelectors(nil, "table1"))

	require.False(t, cval.isConstant())

	require.Nil(t, cval.selectorRanges(nil, "", nil, nil))
}

func TestAVGValue(t *testing.T) {
	cval := &AVGValue{
		s:   &Integer{},
		sel: "db1.table1.amount",
	}
	require.Equal(t, "db1.table1.amount", cval.Selector())
	require.True(t, cval.ColBounded())
	require.False(t, cval.IsNull())

	err := cval.updateWith(&Integer{val: 10})
	require.NoError(t, err)

	require.Equal(t, IntegerType, cval.Type())

	cmp, err := cval.Compare(&Integer{val: 10})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	_, err = cval.Compare(&Bool{val: true})
	require.ErrorIs(t, err, ErrNotComparableValues)

	err = cval.updateWith(&Bool{val: true})
	require.ErrorIs(t, err, ErrNumericTypeExpected)

	err = cval.updateWith(&Integer{val: 2})
	require.NoError(t, err)

	cmp, err = cval.Compare(&Integer{val: 6})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = cval.Compare(&Integer{val: 7})
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// ValueExp

	sqlt, err := cval.inferType(nil, nil, "table1")
	require.NoError(t, err)
	require.Equal(t, IntegerType, sqlt)

	err = cval.requiresType(IntegerType, nil, nil, "table1")
	require.NoError(t, err)

	err = cval.requiresType(BooleanType, nil, nil, "table1")
	require.ErrorIs(t, err, ErrNotComparableValues)

	_, err = cval.jointColumnTo(nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.substitute(nil)
	require.ErrorIs(t, err, ErrUnexpected)

	_, err = cval.reduce(nil, nil, "table1")
	require.ErrorIs(t, err, ErrUnexpected)

	require.Nil(t, cval.reduceSelectors(nil, "table1"))

	require.False(t, cval.isConstant())

	require.Nil(t, cval.selectorRanges(nil, "", nil, nil))
}
