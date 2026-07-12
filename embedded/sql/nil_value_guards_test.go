/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

// The projection-pushdown optimization can leave Row.ValuesByPosition[i]
// nil for columns a query does not reference (see rawRowReader.Read and
// the fileSorter.encodeRow fix for issue #2100). These tests encode the
// contract that every row consumer must treat a nil slot as SQL NULL
// instead of dereferencing it.

func TestRowDigestTreatsNilSlotAsNull(t *testing.T) {
	cols := []ColDescriptor{
		{Column: "a", Type: IntegerType},
		{Column: "b", Type: IntegerType},
	}

	rowWithNil := &Row{ValuesByPosition: []TypedValue{nil, NewInteger(1)}}
	rowWithNull := &Row{ValuesByPosition: []TypedValue{NewNull(IntegerType), NewInteger(1)}}

	dNil, err := rowWithNil.digest(cols)
	require.NoError(t, err)

	dNull, err := rowWithNull.digest(cols)
	require.NoError(t, err)

	require.Equal(t, dNull, dNil)
}

func TestSetOpRowDigestTreatsNilSlotAsNull(t *testing.T) {
	rowWithNil := &Row{ValuesByPosition: []TypedValue{nil, NewVarchar("x")}}
	rowWithNull := &Row{ValuesByPosition: []TypedValue{NewNull(VarcharType), NewVarchar("x")}}

	require.NotPanics(t, func() {
		require.Equal(t, rowDigest(rowWithNull), rowDigest(rowWithNil))
	})
}

func TestRowValuesToValueExpsSubstitutesTypedNullForNilSlot(t *testing.T) {
	cols := []ColDescriptor{
		{Column: "a", Type: IntegerType},
		{Column: "b", Type: VarcharType},
	}

	row := &Row{ValuesByPosition: []TypedValue{nil, NewVarchar("x")}}

	exps := rowValuesToValueExps(row, cols)
	require.Len(t, exps, 2)

	nv, ok := exps[0].(*NullValue)
	require.True(t, ok)
	require.Equal(t, IntegerType, nv.Type())

	require.Equal(t, NewVarchar("x"), exps[1])
}
