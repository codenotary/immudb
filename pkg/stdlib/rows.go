/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package stdlib

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Rows struct {
	index uint64
	conn  *Conn
	rows  []*schema.Row
}

func (r *Rows) Columns() []string {
	names := make([]string, 0)
	if len(r.rows) > 0 {
		for _, n := range r.rows[0].Columns {
			name := n[strings.LastIndex(n, ".")+1 : len(n)-1]
			names = append(names, string(name))
		}
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
	default:
		return math.MaxInt64, true
	}
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, variableLenght should be false.
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
	default:
		return reflect.TypeOf("")
	}
}

func (r *Rows) Close() error {
	return r.conn.Close()
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
		// nil pointers are here converted in nil value. Immudb expects only plain values
		if reflect.ValueOf(nv).Kind() == reflect.Ptr && reflect.ValueOf(nv).IsNil() {
			nv = nil
		}
		switch args[id].(type) {
		case time.Time:
			return nil, ErrTimeValuesNotSupported
		case float64:
			return nil, ErrFloatValuesNotSupported
		default:
			vals[key] = nv
		}
	}
	return vals, nil
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
	}
	return []byte(fmt.Sprintf("%v", op))
}

// RowsAffected implements Result for an INSERT or UPDATE operation
// which mutates a number of rows.
type RowsAffected struct {
	er *schema.SQLExecResult
}

func (rows RowsAffected) LastInsertId() (int64, error) {
	if rows.er != nil && rows.er.LastInsertedPKs != nil && len(rows.er.LastInsertedPKs) == 1 {
		for _, v := range rows.er.LastInsertedPKs {
			return v.GetN(), nil
		}
	}
	return 0, errors.New("unable to retrieve LastInsertId")
}

func (rows RowsAffected) RowsAffected() (int64, error) {
	return int64(rows.er.UpdatedRows), nil
}
