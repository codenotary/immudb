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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyInput(t *testing.T) {
	res, err := ParseString("")
	require.NoError(t, err)
	require.Equal(t, []SQLStmt{}, res)
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
			expectedError:  errors.New("syntax error: unexpected ID, expecting DATABASE"),
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
			expectedError:  errors.New("syntax error: unexpected ID, expecting DATABASE or TABLE"),
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
					colsSpec: []*ColSpec{{colName: "id", colType: "INTEGER"}},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, name STRING, active BOOLEAN, content BLOB)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: "INTEGER"},
						{colName: "name", colType: "STRING"},
						{colName: "active", colType: "BOOLEAN"},
						{colName: "content", colType: "BLOB"},
					},
				}},
			expectedError: nil,
		},
		{
			input:          "CREATE table1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ID, expecting DATABASE or TABLE"),
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
			expectedError:  errors.New("syntax error: unexpected ID, expecting ON"),
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
