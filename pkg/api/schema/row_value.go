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
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/codenotary/immudb/embedded/sql"
)

type SqlValue interface {
	isSQLValue_Value
	Equal(sqlv SqlValue) (bool, error)
}

func (v *SQLValue_Null) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if !isNull {
		return false, nil
	}
	return true, nil
}

func (v *SQLValue_N) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	n, isNumber := sqlv.(*SQLValue_N)
	if !isNumber {
		return false, sql.ErrNotComparableValues
	}
	return v.N == n.N, nil
}

func (v *SQLValue_S) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	s, isString := sqlv.(*SQLValue_S)
	if !isString {
		return false, sql.ErrNotComparableValues
	}
	return v.S == s.S, nil
}

func (v *SQLValue_B) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	b, isBool := sqlv.(*SQLValue_B)
	if !isBool {
		return false, sql.ErrNotComparableValues
	}
	return v.B == b.B, nil
}

func (v *SQLValue_Bs) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	b, isBytes := sqlv.(*SQLValue_Bs)
	if !isBytes {
		return false, sql.ErrNotComparableValues
	}
	return bytes.Equal(v.Bs, b.Bs), nil
}

func (v *SQLValue_Ts) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	ts, isTimestamp := sqlv.(*SQLValue_Ts)
	if !isTimestamp {
		return false, sql.ErrNotComparableValues
	}
	return v.Ts == ts.Ts, nil
}

func (v *SQLValue_F) Equal(sqlv SqlValue) (bool, error) {
	_, isNull := sqlv.(*SQLValue_Null)
	if isNull {
		return false, nil
	}

	f, isFloat := sqlv.(*SQLValue_F)
	if !isFloat {
		return false, sql.ErrNotComparableValues
	}
	return v.F == f.F, nil
}

func RenderValue(op isSQLValue_Value) string {
	switch v := op.(type) {
	case *SQLValue_Null:
		{
			return "NULL"
		}
	case *SQLValue_N:
		{
			return strconv.FormatInt(int64(v.N), 10)
		}
	case *SQLValue_S:
		{
			return fmt.Sprintf("\"%s\"", v.S)
		}
	case *SQLValue_B:
		{
			return strconv.FormatBool(v.B)
		}
	case *SQLValue_Bs:
		{
			return hex.EncodeToString(v.Bs)
		}
	case *SQLValue_Ts:
		{
			t := sql.TimeFromInt64(v.Ts)
			return t.Format("2006-01-02 15:04:05.999999")
		}
	case *SQLValue_F:
		{
			return strconv.FormatFloat(float64(v.F), 'f', -1, 64)
		}
	}

	return fmt.Sprintf("%v", op)
}

func RenderValueAsByte(op isSQLValue_Value) []byte {
	switch v := op.(type) {
	case *SQLValue_Null:
		{
			return nil
		}
	case *SQLValue_N:
		{
			return []byte(strconv.FormatInt(int64(v.N), 10))
		}
	case *SQLValue_S:
		{
			return []byte(v.S)
		}
	case *SQLValue_B:
		{
			return []byte(strconv.FormatBool(v.B))
		}
	case *SQLValue_Bs:
		{
			return []byte(hex.EncodeToString(v.Bs))
		}
	case *SQLValue_Ts:
		{
			t := sql.TimeFromInt64(v.Ts)
			return []byte(t.Format("2006-01-02 15:04:05.999999"))
		}
	case *SQLValue_F:
		{
			return []byte(strconv.FormatFloat(float64(v.F), 'f', -1, 64))
		}
	}
	return []byte(fmt.Sprintf("%v", op))
}

func RawValue(v *SQLValue) interface{} {
	if v == nil {
		return nil
	}

	switch tv := v.Value.(type) {
	case *SQLValue_Null:
		{
			return nil
		}
	case *SQLValue_N:
		{
			return tv.N
		}
	case *SQLValue_S:
		{
			return tv.S
		}
	case *SQLValue_B:
		{
			return tv.B
		}
	case *SQLValue_Bs:
		{
			return tv.Bs
		}
	case *SQLValue_Ts:
		{
			return sql.TimeFromInt64(tv.Ts)
		}
	case *SQLValue_F:
		{
			return tv.F
		}
	}
	return nil
}
