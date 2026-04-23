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

package bmessages

import (
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/stretchr/testify/require"
)

func TestDataRowTextFormat(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewInteger(42),
			sql.NewVarchar("hello"),
		},
	}

	result := DataRow([]*sql.Row{row}, 2, nil)
	require.NotNil(t, result)

	// First byte should be 'D' for DataRow
	require.Equal(t, byte('D'), result[0])
}

func TestDataRowBinaryFormatInteger(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewInteger(100),
		},
	}

	// Format code 1 = binary
	result := DataRow([]*sql.Row{row}, 1, []int16{1})
	require.NotNil(t, result)
	require.Equal(t, byte('D'), result[0])
}

func TestDataRowBinaryFormatVarchar(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewVarchar("test"),
		},
	}

	result := DataRow([]*sql.Row{row}, 1, []int16{1})
	require.NotNil(t, result)
	require.Equal(t, byte('D'), result[0])
}

func TestDataRowBinaryFormatBoolean(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewBool(true),
		},
	}

	result := DataRow([]*sql.Row{row}, 1, []int16{1})
	require.NotNil(t, result)

	row2 := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewBool(false),
		},
	}

	result2 := DataRow([]*sql.Row{row2}, 1, []int16{1})
	require.NotNil(t, result2)
}

func TestDataRowBinaryFormatBlob(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewBlob([]byte{0x01, 0x02, 0x03}),
		},
	}

	result := DataRow([]*sql.Row{row}, 1, []int16{1})
	require.NotNil(t, result)
}

func TestDataRowNilValue(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{nil},
	}

	result := DataRow([]*sql.Row{row}, 1, nil)
	require.Nil(t, result)
}

func TestDataRowEmptyRows(t *testing.T) {
	result := DataRow([]*sql.Row{}, 0, nil)
	require.Empty(t, result)
}

func TestDataRowNullBinaryFormat(t *testing.T) {
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewNull(sql.IntegerType),
		},
	}

	result := DataRow([]*sql.Row{row}, 1, []int16{1})
	require.NotNil(t, result)
}

func TestDataRowGlobalBinaryFormatCode(t *testing.T) {
	// Single format code applies to all columns
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewInteger(1),
			sql.NewVarchar("test"),
		},
	}

	result := DataRow([]*sql.Row{row}, 2, []int16{1})
	require.NotNil(t, result)
}

func TestRenderValueAsByte(t *testing.T) {
	t.Run("null value", func(t *testing.T) {
		v := sql.NewNull(sql.IntegerType)
		result := renderValueAsByte(v)
		require.Nil(t, result)
	})

	t.Run("varchar", func(t *testing.T) {
		v := sql.NewVarchar("hello")
		result := renderValueAsByte(v)
		require.Equal(t, []byte("hello"), result)
	})

	t.Run("integer as text", func(t *testing.T) {
		v := sql.NewInteger(42)
		result := renderValueAsByte(v)
		require.NotEmpty(t, result)
	})

	t.Run("boolean as text", func(t *testing.T) {
		v := sql.NewBool(true)
		result := renderValueAsByte(v)
		require.NotEmpty(t, result)
	})
}

func TestTrimQuotes(t *testing.T) {
	require.Equal(t, "hello", trimQuotes("'hello'"))
	require.Equal(t, "hello", trimQuotes("hello"))
	require.Equal(t, "", trimQuotes("''"))
	require.Equal(t, "", trimQuotes(""))
}
