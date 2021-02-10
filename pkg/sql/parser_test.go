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

package sql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyInput(t *testing.T) {
	_, err := ParseString("")
	require.Error(t, err)
}

func TestCreateDatabaseStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input:          "CREATE DATABASE db1",
			expectedOutput: []SQLStmt{&CreateDatabaseStmt{db: "db1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or TABLE or INDEX"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestUseDatabaseStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input:          "USE DATABASE db1",
			expectedOutput: []SQLStmt{&UseDatabaseStmt{db: "db1"}},
			expectedError:  nil,
		},
		{
			input:          "USE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestCreateTableStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input:          "CREATE TABLE table1",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE TABLE table1()",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE TABLE table1 ( )",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:    "table1",
					colsSpec: []*ColSpec{{colName: "id", colType: IntegerType}},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, name STRING, ts TIMESTAMP, active BOOLEAN, content BLOB)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "name", colType: StringType},
						{colName: "ts", colType: TimestampType},
						{colName: "active", colType: BooleanType},
						{colName: "content", colType: BLOBType},
					},
				}},
			expectedError: nil,
		},
		{
			input:          "CREATE table1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or TABLE or INDEX"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestCreateIndexStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input:          "CREATE INDEX ON table1(id)",
			expectedOutput: []SQLStmt{&CreateIndexStmt{table: "table1", col: "id"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE INDEX table1(id)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting ON"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestAlterTableStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "ALTER TABLE table1 ADD COLUMN title STRING",
			expectedOutput: []SQLStmt{
				&AddColumnStmt{
					table:   "table1",
					colSpec: &ColSpec{colName: "title", colType: StringType},
				}},
			expectedError: nil,
		},
		{
			input: "ALTER TABLE table1 ALTER COLUMN title BLOB",
			expectedOutput: []SQLStmt{
				&AlterColumnStmt{
					table:   "table1",
					colSpec: &ColSpec{colName: "title", colType: BLOBType},
				}},
			expectedError: nil,
		},
		{
			input:          "ALTER TABLE table1 COLUMN title STRING",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected COLUMN, expecting ALTER or ADD"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestInsertIntoStmt(t *testing.T) {
	decodedBLOB, err := hex.DecodeString("AED0393F")
	require.NoError(t, err)

	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "INSERT INTO table1(id, title, active, compressed, payload) VALUES (2, 'untitled row', TRUE, false, b'AED0393F')",
			expectedOutput: []SQLStmt{
				&InsertIntoStmt{
					table: "table1",
					cols:  []string{"id", "title", "active", "compressed", "payload"},
					values: []Value{
						&IntegerValue{value: 2},
						&StringValue{value: "untitled row"},
						&BooleanValue{value: true},
						&BooleanValue{value: false},
						&BLOBValue{value: decodedBLOB},
					},
				}},
			expectedError: nil,
		},
		{
			input:          "INSERT INTO table1() VALUES (2, 'untitled')",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ')', expecting IDENTIFIER"),
		},
		{
			input:          "INSERT INTO VALUES (2)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected VALUES, expecting IDENTIFIER"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestStmtSeparator(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input:          "CREATE TABLE table1;",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE TABLE table1 \n",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE TABLE table1\r\n",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  nil,
		},
		{
			input: "CREATE DATABASE db1; USE DATABASE db1; CREATE TABLE table1",
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{db: "db1"},
				&UseDatabaseStmt{db: "db1"},
				&CreateTableStmt{table: "table1"},
			},
			expectedError: nil,
		},
		{
			input: "CREATE DATABASE db1; USE DATABASE db1 \r\n CREATE TABLE table1",
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{db: "db1"},
				&UseDatabaseStmt{db: "db1"},
				&CreateTableStmt{table: "table1"},
			},
			expectedError: nil,
		},
		{
			input:          "CREATE TABLE table1 USE DATABASE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected USE"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestTxStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "BEGIN; INSERT INTO table1 (id, label) VALUES (100, 'label1'); INSERT INTO table2 (id) VALUES (10) END;",
			expectedOutput: []SQLStmt{
				&TxStmt{stmts: []SQLStmt{
					&InsertIntoStmt{
						table: "table1",
						cols:  []string{"id", "label"},
						values: []Value{
							&IntegerValue{value: 100},
							&StringValue{value: "label1"},
						},
					},
					&InsertIntoStmt{
						table: "table2",
						cols:  []string{"id"},
						values: []Value{
							&IntegerValue{value: 10},
						},
					},
				}},
			},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1; BEGIN; INSERT INTO table1 (id, label) VALUES (100, 'label1'); END;",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
				},
				&TxStmt{stmts: []SQLStmt{
					&InsertIntoStmt{
						table: "table1",
						cols:  []string{"id", "label"},
						values: []Value{
							&IntegerValue{value: 100},
							&StringValue{value: "label1"},
						},
					},
				}},
			},
			expectedError: nil,
		},
		{
			input: "BEGIN; CREATE TABLE table1; INSERT INTO table1 (id, label) VALUES (100, 'label1') END;",
			expectedOutput: []SQLStmt{
				&TxStmt{stmts: []SQLStmt{
					&CreateTableStmt{
						table: "table1",
					},
					&InsertIntoStmt{
						table: "table1",
						cols:  []string{"id", "label"},
						values: []Value{
							&IntegerValue{value: 100},
							&StringValue{value: "label1"},
						},
					},
				}},
			},
			expectedError: nil,
		},
		{
			input:          "BEGIN; INSERT INTO table1 (id, label) VALUES (100, 'label1');",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected $end, expecting END"),
		},
		{
			input:          "BEGIN; INSERT INTO table1 (id, label) VALUES (100, 'label1'); BEGIN; CREATE TABLE table1; END; END",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected BEGIN, expecting END"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}
