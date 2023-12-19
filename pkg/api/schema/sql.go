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
	"time"

	"github.com/codenotary/immudb/embedded/sql"
)

func EncodeParams(params map[string]interface{}) ([]*NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*NamedParam, len(params))

	i := 0
	for n, v := range params {
		sqlVal, err := asSQLValue(v)
		if err != nil {
			return nil, err
		}

		namedParams[i] = &NamedParam{Name: n, Value: sqlVal}
		i++
	}

	return namedParams, nil
}

func asSQLValue(v interface{}) (*SQLValue, error) {
	if v == nil {
		return &SQLValue{Value: &SQLValue_Null{}}, nil
	}
	switch tv := v.(type) {
	case uint:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case uint8:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case uint16:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case uint32:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case uint64:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case int:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case int8:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case int16:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case int32:
		{
			return &SQLValue{Value: &SQLValue_N{N: int64(tv)}}, nil
		}
	case int64:
		{
			return &SQLValue{Value: &SQLValue_N{N: tv}}, nil
		}
	case string:
		{
			return &SQLValue{Value: &SQLValue_S{S: tv}}, nil
		}
	case bool:
		{
			return &SQLValue{Value: &SQLValue_B{B: tv}}, nil
		}
	case []byte:
		{
			return &SQLValue{Value: &SQLValue_Bs{Bs: tv}}, nil
		}
	case time.Time:
		{
			return &SQLValue{Value: &SQLValue_Ts{Ts: sql.TimeToInt64(tv)}}, nil
		}
	case float64:
		{
			return &SQLValue{Value: &SQLValue_F{F: tv}}, nil
		}
	}
	return nil, sql.ErrInvalidValue
}
