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

func TestUnionRowReader(t *testing.T) {
	_, err := newUnionRowReader(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	dummyr := &dummyRowReader{
		database:             "db1",
		failReturningColumns: true,
	}

	_, err = newUnionRowReader([]RowReader{dummyr})
	require.ErrorIs(t, err, errDummy)

	dummyr.failReturningColumns = false

	rowReader, err := newUnionRowReader([]RowReader{dummyr})
	require.NoError(t, err)
	require.NotNil(t, rowReader)

	require.NotNil(t, rowReader.Database())
	require.Equal(t, "db1", rowReader.Database())

	require.Equal(t, "", rowReader.TableAlias())

	require.Nil(t, rowReader.OrderBy())

	require.Nil(t, rowReader.ScanSpecs())

	params := map[string]interface{}{
		"param1": 1,
	}

	err = rowReader.SetParameters(params)
	require.NoError(t, err)

	require.Equal(t, params, rowReader.Parameters())

	paramTypes := make(map[string]string)
	err = rowReader.InferParameters(paramTypes)
	require.NoError(t, err)

	dummyr.failInferringParams = true
	err = rowReader.InferParameters(paramTypes)
	require.ErrorIs(t, err, errDummy)
}
