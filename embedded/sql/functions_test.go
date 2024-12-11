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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPGFunctions(t *testing.T) {
	t.Run("pg_get_userbyid", func(t *testing.T) {
		var f pgGetUserByIDFunc

		err := f.RequiresType(BooleanType, nil, nil, "")
		require.Error(t, err, ErrInvalidTypes)

		err = f.RequiresType(VarcharType, nil, nil, "")
		require.NoError(t, err)

		funcType, err := f.InferType(nil, nil, "")
		require.NoError(t, err)
		require.Equal(t, VarcharType, funcType)

		_, err = f.Apply(nil, []TypedValue{NewInteger(0), NewInteger(0)})
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = f.Apply(nil, []TypedValue{NewInteger(1)})
		require.ErrorContains(t, err, "user not found")
	})

	t.Run("pg_table_is_visible", func(t *testing.T) {
		var f pgTableIsVisible

		err := f.RequiresType(VarcharType, nil, nil, "")
		require.Error(t, err, ErrInvalidTypes)

		err = f.RequiresType(BooleanType, nil, nil, "")
		require.NoError(t, err)

		funcType, err := f.InferType(nil, nil, "")
		require.NoError(t, err)
		require.Equal(t, BooleanType, funcType)

		_, err = f.Apply(nil, []TypedValue{})
		require.ErrorIs(t, err, ErrIllegalArguments)

		v, err := f.Apply(nil, []TypedValue{NewVarchar("my_table")})
		require.NoError(t, err)
		require.True(t, v.RawValue().(bool))
	})

	t.Run("pg_shobj_description", func(t *testing.T) {
		var f pgShobjDescription

		err := f.RequiresType(BooleanType, nil, nil, "")
		require.Error(t, err, ErrInvalidTypes)

		err = f.RequiresType(VarcharType, nil, nil, "")
		require.NoError(t, err)

		funcType, err := f.InferType(nil, nil, "")
		require.NoError(t, err)
		require.Equal(t, VarcharType, funcType)

		_, err = f.Apply(nil, []TypedValue{NewVarchar("")})
		require.ErrorIs(t, err, ErrIllegalArguments)

		v, err := f.Apply(nil, []TypedValue{NewVarchar(""), NewVarchar("")})
		require.NoError(t, err)
		require.Empty(t, v.RawValue())
	})
}
