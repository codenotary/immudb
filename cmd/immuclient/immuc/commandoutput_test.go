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

package immuc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func toJsonString(t *testing.T, obj interface{}) string {
	data, err := json.MarshalIndent(obj, "", "  ")
	require.NoError(t, err)
	return string(data)
}

func TestErrorOutput(t *testing.T) {
	var o CommandOutput = &errorOutput{
		err: "Test error",
	}

	require.Equal(t, "Test error", o.Plain())
	require.Equal(t, "Test error", o.ValueOnly())
	require.JSONEq(t, `{"error": "Test error"}`, toJsonString(t, o.Json()))
}

func TestResultOutput(t *testing.T) {
	var o CommandOutput = &resultOutput{
		Result:  "Test result",
		Warning: "Test warning",
	}

	require.Equal(t, "Test result\nTest warning", o.Plain())
	require.Equal(t, "Test result", o.ValueOnly())
	require.JSONEq(t, `{"result": "Test result", "warning": "Test warning"}`, toJsonString(t, o.Json()))
}

func TestResultOutputNoWarning(t *testing.T) {
	var o CommandOutput = &resultOutput{
		Result: "Test result",
	}

	require.Equal(t, "Test result", o.Plain())
	require.Equal(t, "Test result", o.ValueOnly())
	require.JSONEq(t, `{"result": "Test result"}`, toJsonString(t, o.Json()))
}

func TestCurrentStatusOutput(t *testing.T) {

	var o CommandOutput = &currentStateOutput{
		Db:     "test_db",
		TxId:   1234,
		TxHash: "123456789abcdef",
	}

	require.Regexp(t, `database:\s*test_db`, o.Plain())
	require.Regexp(t, `txID:\s*1234`, o.Plain())
	require.Regexp(t, `hash:\s*123456789abcdef`, o.Plain())

	require.Equal(t, o.Plain(), o.ValueOnly())

	require.JSONEq(t, `{"database": "test_db", "txID": 1234, "hash": "123456789abcdef"}`, toJsonString(t, o.Json()))
}

func TestCurrentStatusOutputEmptyDb(t *testing.T) {

	var o CommandOutput = &currentStateOutput{
		Db:   "test_db",
		TxId: 0,
	}

	require.Regexp(t, `database 'test_db' is empty`, o.Plain())
	require.Equal(t, o.Plain(), o.ValueOnly())

	require.JSONEq(t, `{"database": "test_db", "txID": 0}`, toJsonString(t, o.Json()))
}

func TestHealthOutput(t *testing.T) {
	var o CommandOutput = &healthOutput{
		h: &schema.DatabaseHealthResponse{
			PendingRequests:        123,
			LastRequestCompletedAt: time.Date(2022, 06, 10, 14, 03, 25, 123456789, time.UTC).UnixMilli(),
		},
	}

	require.Regexp(t, `pendingRequests:\s*123`, o.Plain())
	require.Regexp(t, `lastRequestCompletedAt:\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+`, o.Plain())
	require.Equal(t, o.Plain(), o.ValueOnly())

	require.JSONEq(t, `
		{
			"pendingRequests": 123,
			"lastRequestCompletedAt": "2022-06-10T14:03:25.123Z"
		}
	`, toJsonString(t, o.Json()))
}

func TestKVOutput(t *testing.T) {
	var o CommandOutput = &kvOutput{
		entry: &schema.Entry{
			Tx:       123,
			Key:      []byte("test_key"),
			Value:    []byte("test_value"),
			Revision: 321,
		},
		verified: true,
	}

	require.Regexp(t, `tx:\s*123`, o.Plain())
	require.Regexp(t, `rev:\s*321`, o.Plain())
	require.Regexp(t, `key:\s*test_key`, o.Plain())
	require.Regexp(t, `value:\s*test_value`, o.Plain())
	require.Regexp(t, `verified:\s*true`, o.Plain())
	require.Equal(t, "test_value", o.ValueOnly())

	require.JSONEq(t, `
		{
			"tx": 123,
			"revision": 321,
			"key": "test_key",
			"value": "test_value",
			"verified": true
		}
	`, toJsonString(t, o.Json()))
}

func TestMultiKVOutput(t *testing.T) {
	var o CommandOutput = &multiKVOutput{
		entries: []kvOutput{
			{
				entry: &schema.Entry{
					Tx:       123,
					Key:      []byte("test_key1"),
					Value:    []byte("test_value1"),
					Revision: 3211,
				},
				verified: true,
			},
			{
				entry: &schema.Entry{
					Tx:       124,
					Key:      []byte("test_key2"),
					Value:    []byte("test_value2"),
					Revision: 3210,
				},
				verified: false,
			},
		},
	}

	require.Regexp(t, `tx:\s*123`, o.Plain())
	require.Regexp(t, `rev:\s*3211`, o.Plain())
	require.Regexp(t, `key:\s*test_key1`, o.Plain())
	require.Regexp(t, `value:\s*test_value1`, o.Plain())
	require.Regexp(t, `verified:\s*true`, o.Plain())

	require.Regexp(t, `tx:\s*124`, o.Plain())
	require.Regexp(t, `rev:\s*3210`, o.Plain())
	require.Regexp(t, `key:\s*test_key2`, o.Plain())
	require.Regexp(t, `value:\s*test_value2`, o.Plain())

	require.Equal(t, "test_value1\ntest_value2", o.ValueOnly())

	require.JSONEq(t, `
		{
			"items": [
				{
					"tx": 123,
					"revision": 3211,
					"key": "test_key1",
					"value": "test_value1",
					"verified": true
				},
				{
					"tx": 124,
					"revision": 3210,
					"key": "test_key2",
					"value": "test_value2"
				}
			]
		}
	`, toJsonString(t, o.Json()))
}

func TestTxInfoOutput(t *testing.T) {
	var o CommandOutput = &txInfoOutput{
		tx: &schema.Tx{
			Header: &schema.TxHeader{
				Id:       123,
				Ts:       time.Date(2022, 06, 10, 14, 56, 10, 123456789, time.UTC).Unix(),
				Nentries: 321,
			},
		},
		verified: true,
	}

	require.Regexp(t, `tx:\s*123`, o.Plain())
	require.Regexp(t, `time:\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, o.Plain())
	require.Regexp(t, `entries:\s*321`, o.Plain())
	require.Regexp(t, `hash:\s*[0-9a-f]{64}`, o.Plain())
	require.Regexp(t, `verified:\s*true`, o.Plain())
	require.Equal(t, o.Plain(), o.ValueOnly())

	require.JSONEq(t, `
		{
			"tx": 123,
			"time": "2022-06-10T14:56:10Z",
			"entriesCount": 321,
			"hash": "f990ac11dbbf45d49afb1c7950a950b9f73980cb433e8feae1e9f8fd58aae64a",
			"verified": true
		}
	`, toJsonString(t, o.Json()))
}

func TestZEntryOutput(t *testing.T) {
	var o CommandOutput = &zEntryOutput{
		set:           []byte("test_set"),
		referencedKey: []byte("test_ref_key"),
		score:         1234.56,
		txhdr: &schema.TxHeader{
			Id: 321,
			EH: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		},
		verified: true,
	}

	require.Regexp(t, `tx:\s*321`, o.Plain())
	require.Regexp(t, `set:\s*test_set`, o.Plain())
	require.Regexp(t, `referenced key:\s*test_ref_key`, o.Plain())
	require.Regexp(t, `score:\s*1234.56`, o.Plain())
	require.Regexp(t, `hash:\s*01020304050607080900`, o.Plain())
	require.Regexp(t, `verified:\s*true`, o.Plain())
	require.Equal(t, "test_ref_key", o.ValueOnly())

	require.JSONEq(t, `
		{
			"tx": 321,
			"set": "test_set",
			"referencedKey": "test_ref_key",
			"score": 1234.56,
			"hash": "01020304050607080900",
			"verified": true
		}
	`, toJsonString(t, o.Json()))
}

func TestSQLExecOutput(t *testing.T) {
	var o CommandOutput = &sqlExecOutput{
		UpdatedRows: 1234,
	}

	require.Regexp(t, `Updated rows:\s*1234`, o.Plain())
	require.Equal(t, "1234", o.ValueOnly())

	require.JSONEq(t, `
		{
			"updatedRows": 1234
		}
	`, toJsonString(t, o.Json()))
}

func TestTableOutput(t *testing.T) {
	var o CommandOutput = &tableOutput{
		resp: &schema.SQLQueryResult{
			Columns: []*schema.Column{
				{
					Name: "col1",
					Type: "INTEGER",
				},
				{
					Name: "col2",
					Type: "VARCHAR",
				},
				{
					Name: "col3",
					Type: "BOOLEAN",
				},
				{
					Name: "col4",
					Type: "TIMESTAMP",
				},
				{
					Name: "col5",
					Type: "BLOB",
				},
			},
			Rows: []*schema.Row{
				{
					Values: []*schema.SQLValue{
						{Value: &schema.SQLValue_N{N: 1231}},
						{Value: &schema.SQLValue_S{S: "tests1"}},
						{Value: &schema.SQLValue_B{B: true}},
						{Value: &schema.SQLValue_Ts{Ts: time.Date(2022, 6, 10, 15, 51, 15, 123456789, time.UTC).UnixMicro()}},
						{Value: &schema.SQLValue_Bs{Bs: []byte{0x12, 0x34, 0x56}}},
					},
				},
				{
					Values: []*schema.SQLValue{
						{Value: &schema.SQLValue_N{N: 1232}},
						{Value: &schema.SQLValue_S{S: "tests2"}},
						{Value: &schema.SQLValue_B{B: false}},
						{Value: &schema.SQLValue_Ts{Ts: time.Date(2022, 6, 9, 15, 51, 15, 123456789, time.UTC).UnixMicro()}},
						{Value: &schema.SQLValue_Bs{Bs: []byte{0x78, 0xab, 0xcd}}},
					},
				},
			},
		},
	}

	plain := o.Plain()

	require.Regexp(t, `\|\s*COL1\s*\|\s*COL2\s*\|\s*COL3\s*\|\s*COL4\s*\|\s*COL5\s*\|`, plain)
	require.Regexp(t, `\|\s*1231\s*\|\s*"tests1"\s*\|\s*true\s*\|\s*2022-06-10 15:51:15\.123456\s*\|\s*123456\s*\|`, plain)
	require.Regexp(t, `\|\s*1232\s*\|\s*"tests2"\s*\|\s*false\s*\|\s*2022-06-09 15:51:15\.123456\s*\|\s*78abcd\s*\|`, plain)
	require.Regexp(t, `1231,"tests1",true,2022-06-10 15:51:15\.123456,123456`, o.ValueOnly())
	require.Regexp(t, `1232,"tests2",false,2022-06-09 15:51:15\.123456,78abcd`, o.ValueOnly())

	require.JSONEq(t, `
		{
			"rows": [
				{
					"col1": 1231,
					"col2": "tests1",
					"col3": true,
					"col4": "2022-06-10T15:51:15.123456Z",
					"col5": "EjRW"
				},
				{
					"col1": 1232,
					"col2": "tests2",
					"col3": false,
					"col4": "2022-06-09T15:51:15.123456Z",
					"col5": "eKvN"
				}
			]
		}
	`, toJsonString(t, o.Json()))
}
