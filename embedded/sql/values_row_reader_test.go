/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValuesRowReader(t *testing.T) {
	_, err := newValuesRowReader(nil, nil, "", "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	cols := []ColDescriptor{
		{Column: "col1"},
	}

	_, err = newValuesRowReader(nil, cols, "", "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = newValuesRowReader(nil, cols, "db1", "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = newValuesRowReader(nil, cols, "db1", "table1", nil)
	require.NoError(t, err)

	_, err = newValuesRowReader(nil, cols, "db1", "table1",
		[][]ValueExp{
			{
				&Bool{val: true},
				&Bool{val: false},
			},
		})
	require.ErrorIs(t, err, ErrInvalidNumberOfValues)

	_, err = newValuesRowReader(nil,
		[]ColDescriptor{
			{Table: "table1", Column: "col1"},
		}, "", "", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	values := [][]ValueExp{
		{
			&Bool{val: true},
		},
	}

	rowReader, err := newValuesRowReader(nil, cols, "db1", "table1", values)
	require.NoError(t, err)

	require.Equal(t, "db1", rowReader.Database())
	require.Nil(t, rowReader.OrderBy())
	require.Nil(t, rowReader.ScanSpecs())

	duplicatedParams := map[string]interface{}{
		"param1": 1,
		"Param1": true,
	}

	err = rowReader.SetParameters(duplicatedParams)
	require.ErrorIs(t, err, ErrDuplicatedParameters)

	params := map[string]interface{}{
		"param1": 1,
	}

	err = rowReader.SetParameters(params)
	require.NoError(t, err)

	require.Equal(t, params, rowReader.Parameters())

	paramTypes := make(map[string]string)
	err = rowReader.InferParameters(paramTypes)
	require.NoError(t, err)

	require.NoError(t, rowReader.Close())
	require.ErrorIs(t, rowReader.Close(), ErrAlreadyClosed)
}
