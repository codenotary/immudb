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

func TestUnionRowReader(t *testing.T) {
	_, err := newUnionRowReader(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	params := map[string]interface{}{
		"param1": 1,
	}

	dummyr := &dummyRowReader{
		database:             "db1",
		failReturningColumns: true,
		params:               params,
	}

	_, err = newUnionRowReader(context.Background(), []RowReader{dummyr})
	require.ErrorIs(t, err, errDummy)

	dummyr.failReturningColumns = false

	rowReader, err := newUnionRowReader(context.Background(), []RowReader{dummyr})
	require.NoError(t, err)
	require.NotNil(t, rowReader)

	require.Equal(t, "", rowReader.TableAlias())

	require.Nil(t, rowReader.OrderBy())

	require.Nil(t, rowReader.ScanSpecs())

	require.Equal(t, params, rowReader.Parameters())

	paramTypes := make(map[string]string)
	err = rowReader.InferParameters(context.Background(), paramTypes)
	require.NoError(t, err)

	dummyr.failInferringParams = true
	err = rowReader.InferParameters(context.Background(), paramTypes)
	require.ErrorIs(t, err, errDummy)
}
