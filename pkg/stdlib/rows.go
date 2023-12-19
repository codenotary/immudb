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

package stdlib

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

type Rows struct {
	index   uint64
	rows    []*schema.Row
	columns []*schema.Column
}

func (r *Rows) Columns() []string {
	names := make([]string, 0)
	for _, n := range r.columns {
		name := n.Name[strings.LastIndex(n.Name, ".")+1 : len(n.Name)-1]
		names = append(names, string(name))
	}
	return names
}

// ColumnTypeDatabaseTypeName
// 	IntegerType   SQLValueType = "INTEGER"
//	BooleanType   SQLValueType = "BOOLEAN"
//	VarcharType   SQLValueType = "VARCHAR"
//	BLOBType      SQLValueType = "BLOB"
//	TimestampType SQLValueType = "TIMESTAMP"
//	AnyType       SQLValueType = "ANY"
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if len(r.rows) <= 0 || len(r.rows[0].Values)-1 < index {
		return ""
	}
	op := r.rows[0].Values[index].Value

	switch op.(type) {
	case *schema.SQLValue_Null:
		{
			return "ANY"
		}
	case *schema.SQLValue_N:
		{
			return "INTEGER"
		}
	case *schema.SQLValue_S:
		{
			return "VARCHAR"
		}
	case *schema.SQLValue_B:
		{
			return "BOOLEAN"
		}
	case *schema.SQLValue_Bs:
		{
			return "BLOB"
		}
	case *schema.SQLValue_Ts:
		{
			return "TIMESTAMP"
		}
	default:
		return "ANY"
	}
}

// ColumnTypeLength If length is not limited other than system limits, it should return math.MaxInt64
func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	if len(r.rows) <= 0 || len(r.rows[0].Values)-1 < index {
		return 0, false
	}

	op := r.rows[0].Values[index].Value

	switch op.(type) {
	case *schema.SQLValue_Null:
		{
			return 0, false
		}
	case *schema.SQLValue_N:
		{
			return 8, false
		}
	case *schema.SQLValue_S:
		{
			return math.MaxInt64, true
		}
	case *schema.SQLValue_B:
		{
			return 1, false
		}
	case *schema.SQLValue_Bs:
		{
			return math.MaxInt64, true
		}
	case *schema.SQLValue_Ts:
		{
			return math.MaxInt64, true
		}
	default:
		return math.MaxInt64, true
	}
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, variableLength should be false.
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return 0, 0, false
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if len(r.rows) <= 0 || len(r.rows[0].Values)-1 < index {
		return nil
	}

	op := r.rows[0].Values[index].Value

	switch op.(type) {
	case *schema.SQLValue_Null:
		{
			return reflect.TypeOf(nil)
		}
	case *schema.SQLValue_N:
		{
			return reflect.TypeOf(int64(0))
		}
	case *schema.SQLValue_S:
		{
			return reflect.TypeOf("")
		}
	case *schema.SQLValue_B:
		{
			return reflect.TypeOf(true)
		}
	case *schema.SQLValue_Bs:
		{
			return reflect.TypeOf([]byte{})
		}
	case *schema.SQLValue_Ts:
		{
			return reflect.TypeOf(time.Time{})
		}
	default:
		return reflect.TypeOf("")
	}
}

func (r *Rows) Close() error {
	// no reader here
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= uint64(len(r.rows)) {
		return io.EOF
	}

	row := r.rows[r.index]

	for idx, val := range row.Values {
		dest[idx] = RenderValue(val.Value)
	}

	r.index++

	return nil
}

func namedValuesToSqlMap(argsV []driver.NamedValue) (map[string]interface{}, error) {
	args := make([]interface{}, 0, len(argsV))

	for _, v := range argsV {
		if v.Value != nil {
			args = append(args, v.Value.(interface{}))
		} else {
			args = append(args, nil)
		}
	}

	args, err := convertDriverValuers(args)
	if err != nil {
		return nil, err
	}

	vals := make(map[string]interface{})

	for id, nv := range args {
		key := "param" + strconv.Itoa(id+1)
		vals[key] = nv
	}

	vals = convertToPlainVals(vals)

	return vals, nil
}

func convertToPlainVals(vals map[string]interface{}) map[string]interface{} {
	for key, nv := range vals {
		if reflect.ValueOf(nv).Kind() == reflect.Ptr && reflect.ValueOf(nv).IsNil() {
			nv = nil
		}
		t := nv
		switch t.(type) {
		case *uint:
			vals[key] = *t.(*uint)
		case *uint8:
			vals[key] = *t.(*uint8)
		case *uint16:
			vals[key] = *t.(*uint16)
		case *uint32:
			vals[key] = *t.(*uint32)
		case *uint64:
			vals[key] = *t.(*uint64)
		case *int:
			vals[key] = *t.(*int)
		case *int8:
			vals[key] = *t.(*int8)
		case *int16:
			vals[key] = *t.(*int16)
		case *int32:
			vals[key] = *t.(*int32)
		case *int64:
			vals[key] = *t.(*int64)
		case *string:
			vals[key] = *t.(*string)
		case *bool:
			vals[key] = *t.(*bool)
		case *float32:
			vals[key] = *t.(*float32)
		case *float64:
			vals[key] = *t.(*float64)
		case *complex64:
			vals[key] = *t.(*complex64)
		case *complex128:
			vals[key] = *t.(*complex128)
		case *time.Time:
			vals[key] = *t.(*time.Time)
		default:
			vals[key] = nv
		}
	}
	return vals
}

func convertDriverValuers(args []interface{}) ([]interface{}, error) {
	for i, arg := range args {
		switch arg := arg.(type) {
		case driver.Valuer:
			v, err := callValuerValue(arg)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
	}
	return args, nil
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value()
// This function is mirrored in the database/sql/driver package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}

func RenderValue(op interface{}) interface{} {
	switch v := op.(type) {
	case *schema.SQLValue_Null:
		{
			return nil
		}
	case *schema.SQLValue_N:
		{
			return v.N
		}
	case *schema.SQLValue_S:
		{
			return v.S
		}
	case *schema.SQLValue_B:
		{
			return v.B
		}
	case *schema.SQLValue_Bs:
		{
			return v.Bs
		}
	case *schema.SQLValue_Ts:
		{
			return sql.TimeFromInt64(v.Ts)
		}
	}
	return []byte(fmt.Sprintf("%v", op))
}

// RowsAffected implements Result for an INSERT or UPDATE operation
// which mutates a number of rows.
type RowsAffected struct {
	er *schema.SQLExecResult
}

func (rows RowsAffected) LastInsertId() (int64, error) {
	// if immudb will returns a no monotonic primary key sequence this will not work anymore
	if rows.er != nil && len(rows.er.Txs) >= 1 {
		for _, v := range rows.er.FirstInsertedPks() {
			return v.GetN(), nil
		}
	}
	return 0, errors.New("unable to retrieve LastInsertId")
}

func (rows RowsAffected) RowsAffected() (int64, error) {
	if len(rows.er.Txs) == 0 {
		return 0, nil
	}

	// TODO: consider the case when multiple txs are committed
	return int64(rows.er.Txs[0].UpdatedRows), nil
}
