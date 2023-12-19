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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValuesRowReader(t *testing.T) {
	_, err := newValuesRowReader(nil, nil, nil, "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	cols := []ColDescriptor{
		{Column: "col1"},
	}

	_, err = newValuesRowReader(nil, nil, cols, "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = newValuesRowReader(nil, nil, cols, "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = newValuesRowReader(nil, nil, cols, "table1", nil)
	require.NoError(t, err)

	_, err = newValuesRowReader(nil, nil, cols, "table1",
		[][]ValueExp{
			{
				&Bool{val: true},
				&Bool{val: false},
			},
		})
	require.ErrorIs(t, err, ErrInvalidNumberOfValues)

	_, err = newValuesRowReader(nil, nil,
		[]ColDescriptor{
			{Table: "table1", Column: "col1"},
		}, "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	values := [][]ValueExp{
		{
			&Bool{val: true},
		},
	}

	params := map[string]interface{}{
		"param1": 1,
	}

	rowReader, err := newValuesRowReader(nil, params, cols, "table1", values)
	require.NoError(t, err)
	require.Nil(t, rowReader.OrderBy())
	require.Nil(t, rowReader.ScanSpecs())

	require.Equal(t, params, rowReader.Parameters())

	paramTypes := make(map[string]string)
	err = rowReader.InferParameters(context.Background(), paramTypes)
	require.NoError(t, err)

	require.NoError(t, rowReader.Close())
	require.ErrorIs(t, rowReader.Close(), ErrAlreadyClosed)
}
