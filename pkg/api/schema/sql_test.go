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

package schema

import (
	"errors"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/stretchr/testify/require"
)

func TestEncodeParams(t *testing.T) {
	p, err := EncodeParams(nil)
	require.NoError(t, err)
	require.Nil(t, p)

	p, err = EncodeParams(map[string]interface{}{
		"param1": 1,
	})
	require.NoError(t, err)
	require.Len(t, p, 1)
	require.Equal(t, "param1", p[0].Name)
	require.EqualValues(t, &SQLValue{Value: &SQLValue_N{N: 1}}, p[0].Value)

	p, err = EncodeParams(map[string]interface{}{
		"param1": struct{}{},
	})
	require.True(t, errors.Is(err, sql.ErrInvalidValue))
	require.Nil(t, p)
}

func TestAsSQLValue(t *testing.T) {
	for _, d := range []struct {
		n      string
		val    interface{}
		sqlVal *SQLValue
		isErr  bool
	}{
		{
			"nil", nil, &SQLValue{Value: &SQLValue_Null{}}, false,
		},
		{
			"uint", uint(10), &SQLValue{Value: &SQLValue_N{N: 10}}, false,
		},
		{
			"uint8", uint8(10), &SQLValue{Value: &SQLValue_N{N: 10}}, false,
		},
		{
			"uint16", uint16(10), &SQLValue{Value: &SQLValue_N{N: 10}}, false,
		},
		{
			"uint32", uint32(10), &SQLValue{Value: &SQLValue_N{N: 10}}, false,
		},
		{
			"uint64", uint64(13), &SQLValue{Value: &SQLValue_N{N: 13}}, false,
		},
		{
			"int", 11, &SQLValue{Value: &SQLValue_N{N: 11}}, false,
		},
		{
			"int8", int8(11), &SQLValue{Value: &SQLValue_N{N: 11}}, false,
		},
		{
			"int16", int16(11), &SQLValue{Value: &SQLValue_N{N: 11}}, false,
		},
		{
			"int32", int32(11), &SQLValue{Value: &SQLValue_N{N: 11}}, false,
		},
		{
			"int64", int64(12), &SQLValue{Value: &SQLValue_N{N: 12}}, false,
		},
		{
			"string", string("14"), &SQLValue{Value: &SQLValue_S{S: "14"}}, false,
		},
		{
			"bool", true, &SQLValue{Value: &SQLValue_B{B: true}}, false,
		},
		{
			"[]byte", []byte{1, 5}, &SQLValue{Value: &SQLValue_Bs{Bs: []byte{1, 5}}}, false,
		},
		{
			"struct{}", struct{}{}, nil, true,
		},
		{
			"nil", (*string)(nil), nil, true,
		},
		{
			"timestamp", time.Date(2021, 12, 7, 14, 12, 54, 12345, time.UTC),
			&SQLValue{Value: &SQLValue_Ts{Ts: 1638886374000012}}, false,
		},
	} {
		t.Run(d.n, func(t *testing.T) {
			sqlVal, err := AsSQLValue(d.val)
			require.EqualValues(t, d.sqlVal, sqlVal)
			if d.isErr {
				require.ErrorIs(t, err, sql.ErrInvalidValue)
			}
		})
	}
}
