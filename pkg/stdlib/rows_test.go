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
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestRows(t *testing.T) {
	rows := []client.Row{nil}
	cols := []client.Column{{Name: "(defaultdb.emptytable.c1)"}}

	reader := newMockRowReader(cols, rows)

	r := newRows(reader)

	ast := r.Columns()
	require.Equal(t, "c1", ast[0])

	st := r.ColumnTypeDatabaseTypeName(1)
	require.Equal(t, "", st)

	num, b := r.ColumnTypeLength(1)
	require.Equal(t, int64(0), num)
	require.False(t, b)

	_, _, _ = r.ColumnTypePrecisionScale(1)
	ty := r.ColumnTypeScanType(1)
	require.Nil(t, ty)
}

func TestRows_ColumnTypeDatabaseTypeName(t *testing.T) {
	var tests = []struct {
		reader   client.SQLQueryRowReader
		name     string
		expected string
	}{
		{
			name:     "INTEGER",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.IntegerType}}, []client.Row{{1}}),
			expected: "INTEGER",
		},
		{
			name:     "VARCHAR",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.VarcharType}}, []client.Row{{"string"}}),
			expected: "VARCHAR",
		},
		{
			name:     "BLOB",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BLOBType}}, []client.Row{{[]byte("bytes")}}),
			expected: "BLOB",
		},
		{
			name:     "BOOLEAN",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BooleanType}}, []client.Row{{true}}),
			expected: "BOOLEAN",
		},
		{
			name:     "TIMESTAMP",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.TimestampType}}, []client.Row{{sql.TimeToInt64(time.Now())}}),
			expected: "TIMESTAMP",
		},
		{
			name:     "default",
			reader:   newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.AnyType}}, []client.Row{{nil}}),
			expected: "ANY",
		},
		{
			name: "no rows",
			reader: &mockRowReader{
				rows: nil,
			},
			expected: "",
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("rows %d: %s", i, tt.name), func(t *testing.T) {
			rows := newRows(tt.reader)

			vt := rows.ColumnTypeDatabaseTypeName(0)
			require.Equal(t, tt.expected, vt)
		})
	}
}

func TestRows_ColumnTypeLength(t *testing.T) {
	var tests = []struct {
		reader         client.SQLQueryRowReader
		name           string
		lenght         int64
		variableLenght bool
	}{
		{
			name:           "INTEGER",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.IntegerType}}, []client.Row{{1}}),
			lenght:         8,
			variableLenght: false,
		},
		{
			name:           "VARCHAR",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.TimestampType}}, []client.Row{{"string"}}),
			lenght:         math.MaxInt64,
			variableLenght: true,
		},
		{
			name:           "BLOB",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BLOBType}}, []client.Row{{[]byte("bytes")}}),
			lenght:         math.MaxInt64,
			variableLenght: true,
		},
		{
			name:           "BOOLEAN",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BooleanType}}, []client.Row{{true}}),
			lenght:         1,
			variableLenght: false,
		},
		{
			name:           "TIMESTAMP",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.TimestampType}}, []client.Row{{sql.TimeToInt64(time.Now())}}),
			lenght:         math.MaxInt64,
			variableLenght: true,
		},
		{
			name:           "default",
			reader:         newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.AnyType}}, []client.Row{{nil}}),
			lenght:         math.MaxInt64,
			variableLenght: true,
		},
		{
			name: "no rows",
			reader: &mockRowReader{
				rows: nil,
			},
			lenght:         0,
			variableLenght: false,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("rows %d: %s", i, tt.name), func(t *testing.T) {
			rows := newRows(tt.reader)

			vl, ok := rows.ColumnTypeLength(0)
			require.Equal(t, tt.lenght, vl)
			require.Equal(t, tt.variableLenght, ok)
		})
	}
}

func TestRows_ColumnTypeScanType(t *testing.T) {
	var tests = []struct {
		reader       client.SQLQueryRowReader
		name         string
		expectedType reflect.Type
	}{
		{
			name:         "INTEGER",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.IntegerType}}, []client.Row{{1}}),
			expectedType: reflect.TypeOf(int64(0)),
		},
		{
			name:         "VARCHAR",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.VarcharType}}, []client.Row{{"string"}}),
			expectedType: reflect.TypeOf(""),
		},
		{
			name:         "BLOB",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BLOBType}}, []client.Row{{[]byte("bytes")}}),
			expectedType: reflect.TypeOf([]byte{}),
		},
		{
			name:         "BOOLEAN",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.BooleanType}}, []client.Row{{true}}),
			expectedType: reflect.TypeOf(true),
		},
		{
			name:         "TIMESTAMP",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.TimestampType}}, []client.Row{{sql.TimeToInt64(time.Now())}}),
			expectedType: reflect.TypeOf(time.Now()),
		},
		{
			name:         "default",
			reader:       newMockRowReader([]client.Column{{Name: "(defaultdb.emptytable.c1)", Type: sql.AnyType}}, []client.Row{nil}),
			expectedType: reflect.TypeOf(""),
		},
		{
			name: "no rows",
			reader: &mockRowReader{
				rows: nil,
			},
			expectedType: nil,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("rows %d: %s", i, tt.name), func(t *testing.T) {
			rows := newRows(tt.reader)

			vt := rows.ColumnTypeScanType(0)
			require.Equal(t, tt.expectedType, vt)
		})
	}
}

func TestRowsAffected_LastInsertId(t *testing.T) {
	ra := RowsAffected{
		er: &schema.SQLExecResult{
			Txs: []*schema.CommittedSQLTx{
				{
					UpdatedRows: 1,
					LastInsertedPKs: map[string]*schema.SQLValue{
						"table1": {Value: &schema.SQLValue_N{N: 1}},
					},
					FirstInsertedPKs: map[string]*schema.SQLValue{
						"table1": {Value: &schema.SQLValue_N{N: 1}},
					},
				},
			},
		},
	}
	lID, err := ra.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(1), lID)
}

func TestRowsAffected_LastInsertIdErr(t *testing.T) {
	ra := RowsAffected{
		er: &schema.SQLExecResult{},
	}
	_, err := ra.LastInsertId()
	require.ErrorContains(t, err, "unable to retrieve LastInsertId")
}

func TestRowsAffected_RowsAffected(t *testing.T) {
	ra := RowsAffected{
		er: &schema.SQLExecResult{},
	}
	rac, err := ra.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), rac)
}

func TestRows_convertToPlainVals(t *testing.T) {
	var tests = []struct {
		vals map[string]interface{}
	}{
		{vals: map[string]interface{}{"v": (*string)(nil)}},
		{vals: map[string]interface{}{"v": new(int)}},
		{vals: map[string]interface{}{"v": new(int8)}},
		{vals: map[string]interface{}{"v": new(int16)}},
		{vals: map[string]interface{}{"v": new(int32)}},
		{vals: map[string]interface{}{"v": new(int64)}},
		{vals: map[string]interface{}{"v": new(uint)}},
		{vals: map[string]interface{}{"v": new(uint8)}},
		{vals: map[string]interface{}{"v": new(uint16)}},
		{vals: map[string]interface{}{"v": new(uint32)}},
		{vals: map[string]interface{}{"v": new(uint64)}},
		{vals: map[string]interface{}{"v": new(string)}},
		{vals: map[string]interface{}{"v": new(bool)}},
		{vals: map[string]interface{}{"v": new(float32)}},
		{vals: map[string]interface{}{"v": new(float64)}},
		{vals: map[string]interface{}{"v": new(complex64)}},
		{vals: map[string]interface{}{"v": new(complex128)}},
		{vals: map[string]interface{}{"v": &time.Time{}}},
		{vals: map[string]interface{}{"v": "default"}},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("rows %d: %s", i, reflect.ValueOf(tt.vals["v"]).Type().String()), func(t *testing.T) {
			vals := convertToPlainVals(tt.vals)
			require.False(t, reflect.ValueOf(vals["v"]).Kind() == reflect.Ptr)
		})
	}
}

func TestEmptyRowsForColumns(t *testing.T) {
	r := Rows{
		columns: []client.Column{
			{
				Name: "(defaultdb.emptytable.id)",
			},
			{
				Name: "(defaultdb.emptytable.name)",
			},
		},
	}

	ast := r.Columns()
	require.Equal(t, "id", ast[0])
	require.Equal(t, "name", ast[1])
}

type mockRowReader struct {
	client.SQLQueryRowReader

	columns []client.Column
	rows    []client.Row
	nextRow int
}

func newMockRowReader(cols []client.Column, rows []client.Row) *mockRowReader {
	return &mockRowReader{
		columns: cols,
		rows:    rows,
	}
}

func (r *mockRowReader) Next() bool {
	if r.nextRow+1 < len(r.rows) {
		r.nextRow++
		return true
	}
	return false
}

func (r *mockRowReader) Columns() []client.Column {
	return r.columns
}

func (r *mockRowReader) Read() (client.Row, error) {
	if r.nextRow >= len(r.rows) {
		return nil, sql.ErrNoMoreRows
	}

	row := r.rows[r.nextRow]
	return row, nil
}
