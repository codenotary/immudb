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
			expectedOutput: []SQLStmt{&CreateDatabaseStmt{DB: "db1"}},
			expectedError:  nil,
		},
		{
			input:          "CREATE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or TABLE or UNIQUE or INDEX at position 10"),
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
			expectedOutput: []SQLStmt{&UseDatabaseStmt{DB: "db1"}},
			expectedError:  nil,
		},
		{
			input:          "USE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or SNAPSHOT at position 7"),
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

func TestUseSnapshotStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "USE SNAPSHOT SINCE TX 100",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{sinceTx: uint64(100)},
			},
			expectedError: nil,
		},
		{
			input:          "USE SNAPSHOT SINCE 10",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected NUMBER, expecting TX at position 21"),
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
			input: "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table1",
					ifNotExists: false,
					colsSpec:    []*ColSpec{{colName: "id", colType: IntegerType}},
					pkColNames:  []string{"id"},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER AUTO_INCREMENT, PRIMARY KEY id)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table1",
					ifNotExists: false,
					colsSpec:    []*ColSpec{{colName: "id", colType: IntegerType, autoIncrement: true}},
					pkColNames:  []string{"id"},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE xtable1 (xid INTEGER, PRIMARY KEY xid)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "xtable1",
					ifNotExists: false,
					colsSpec:    []*ColSpec{{colName: "xid", colType: IntegerType}},
					pkColNames:  []string{"xid"},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE IF NOT EXISTS table1 (id INTEGER, PRIMARY KEY (id))",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table1",
					ifNotExists: true,
					colsSpec:    []*ColSpec{{colName: "id", colType: IntegerType}},
					pkColNames:  []string{"id"},
				}},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, name VARCHAR[50], ts TIMESTAMP, active BOOLEAN, content BLOB, PRIMARY KEY (id, name))",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table1",
					ifNotExists: false,
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "name", colType: VarcharType, maxLen: 50},
						{colName: "ts", colType: TimestampType},
						{colName: "active", colType: BooleanType},
						{colName: "content", colType: BLOBType},
					},
					pkColNames: []string{"id", "name"},
				}},
			expectedError: nil,
		},
		{
			input:          "CREATE table1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or TABLE or UNIQUE or INDEX at position 13"),
		},
		{
			input:          "CREATE TABLE table1",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  errors.New("syntax error: unexpected $end, expecting '(' at position 20"),
		},
		{
			input:          "CREATE TABLE table1()",
			expectedOutput: []SQLStmt{&CreateTableStmt{table: "table1"}},
			expectedError:  errors.New("syntax error: unexpected ')', expecting IDENTIFIER at position 21"),
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
			expectedOutput: []SQLStmt{&CreateIndexStmt{table: "table1", cols: []string{"id"}}},
			expectedError:  nil,
		},
		{
			input:          "CREATE INDEX IF NOT EXISTS ON table1(id)",
			expectedOutput: []SQLStmt{&CreateIndexStmt{table: "table1", ifNotExists: true, cols: []string{"id"}}},
			expectedError:  nil,
		},
		{
			input:          "CREATE INDEX table1(id)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting ON at position 19"),
		},
		{
			input:          "CREATE UNIQUE INDEX ON table1(id, title)",
			expectedOutput: []SQLStmt{&CreateIndexStmt{unique: true, table: "table1", cols: []string{"id", "title"}}},
			expectedError:  nil,
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
			input: "ALTER TABLE table1 ADD COLUMN title VARCHAR",
			expectedOutput: []SQLStmt{
				&AddColumnStmt{
					table:   "table1",
					colSpec: &ColSpec{colName: "title", colType: VarcharType},
				}},
			expectedError: nil,
		},
		{
			input: "ALTER TABLE table1 ADD COLUMN title BLOB",
			expectedOutput: []SQLStmt{
				&AddColumnStmt{
					table:   "table1",
					colSpec: &ColSpec{colName: "title", colType: BLOBType},
				}},
			expectedError: nil,
		},
		{
			input:          "ALTER TABLE table1 COLUMN title VARCHAR",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected COLUMN, expecting ADD at position 25"),
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
			input: "UPSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, now(), 'un''titled row', TRUE, false, x'AED0393F', @param1)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*RowSpec{
						{Values: []ValueExp{
							&Number{val: 2},
							&SysFn{fn: "now"},
							&Varchar{val: "un'titled row"},
							&Bool{val: true},
							&Bool{val: false},
							&Blob{val: decodedBLOB},
							&Param{id: "param1"},
						},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "UPSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, now(), '', TRUE, false, x'AED0393F', @param1)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*RowSpec{
						{Values: []ValueExp{
							&Number{val: 2},
							&SysFn{fn: "now"},
							&Varchar{val: ""},
							&Bool{val: true},
							&Bool{val: false},
							&Blob{val: decodedBLOB},
							&Param{id: "param1"},
						},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "UPSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, now(), '''', TRUE, false, x'AED0393F', @param1)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*RowSpec{
						{Values: []ValueExp{
							&Number{val: 2},
							&SysFn{fn: "now"},
							&Varchar{val: "'"},
							&Bool{val: true},
							&Bool{val: false},
							&Blob{val: decodedBLOB},
							&Param{id: "param1"},
						},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "UPSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, now(), 'untitled row', TRUE, ?, x'AED0393F', ?)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*RowSpec{
						{Values: []ValueExp{
							&Number{val: 2},
							&SysFn{fn: "now"},
							&Varchar{val: "untitled row"},
							&Bool{val: true},
							&Param{id: "param1", pos: 1},
							&Blob{val: decodedBLOB},
							&Param{id: "param2", pos: 2},
						},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "UPSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, now(), $1, TRUE, $2, x'AED0393F', $1)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*RowSpec{
						{Values: []ValueExp{
							&Number{val: 2},
							&SysFn{fn: "now"},
							&Param{id: "param1", pos: 1},
							&Bool{val: true},
							&Param{id: "param2", pos: 2},
							&Blob{val: decodedBLOB},
							&Param{id: "param1", pos: 1},
						},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES ($0, $1)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR, expecting ')' at position 40"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES (?, @title)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 42"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES (@id, ?)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 44"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES (@id, $1)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 44"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES ($1, @title)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 43"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES ($1, ?)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 43"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES (?, $1)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 42"),
		},
		{
			input:          "UPSERT INTO table1(id, title) VALUES ($1, $title)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ERROR at position 43"),
		},
		{
			input: "UPSERT INTO table1(id, active) VALUES (1, false), (2, true), (3, true)",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "active"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 1}, &Bool{val: false}}},
						{Values: []ValueExp{&Number{val: 2}, &Bool{val: true}}},
						{Values: []ValueExp{&Number{val: 3}, &Bool{val: true}}},
					},
				},
			},
			expectedError: nil,
		},
		{
			input:          "UPSERT INTO table1() VALUES (2, 'untitled')",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected ')', expecting IDENTIFIER at position 20"),
		},
		{
			input:          "UPSERT INTO VALUES (2)",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected VALUES, expecting IDENTIFIER at position 18"),
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
			input: "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id);",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
					},
					pkColNames: []string{"id"},
				},
			},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)\n",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
					},
					pkColNames: []string{"id"},
				},
			},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)\r\n",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
					},
					pkColNames: []string{"id"},
				},
			},
			expectedError: nil,
		},
		{
			input: "CREATE DATABASE db1; USE DATABASE db1;",
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{DB: "db1"},
				&UseDatabaseStmt{DB: "db1"},
			},
			expectedError: nil,
		},
		{
			input: "CREATE DATABASE db1; /* some comment here */ USE /* another comment here */ DATABASE db1",
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{DB: "db1"},
				&UseDatabaseStmt{DB: "db1"},
			},
			expectedError: nil,
		},
		{
			input: "CREATE DATABASE db1; USE DATABASE db1; USE DATABASE db1",
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{DB: "db1"},
				&UseDatabaseStmt{DB: "db1"},
				&UseDatabaseStmt{DB: "db1"},
			},
			expectedError: nil,
		},
		{
			input:          "CREATE DATABASE db1 USE DATABASE db1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected USE at position 23"),
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
			input: "BEGIN TRANSACTION; UPSERT INTO table1 (id, label) VALUES (100, 'label1'); UPSERT INTO table2 (id) VALUES (10); ROLLBACK;",
			expectedOutput: []SQLStmt{
				&BeginTransactionStmt{},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "label"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 100}, &Varchar{val: "label1"}}},
					},
				},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table2"},
					cols:     []string{"id"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 10}}},
					},
				},
				&RollbackStmt{},
			},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1 (id INTEGER, label VARCHAR, PRIMARY KEY id); BEGIN TRANSACTION; UPSERT INTO table1 (id, label) VALUES (100, 'label1'); COMMIT;",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "label", colType: VarcharType},
					},
					pkColNames: []string{"id"},
				},
				&BeginTransactionStmt{},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "label"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 100}, &Varchar{val: "label1"}}},
					},
				},
				&CommitStmt{},
			},
			expectedError: nil,
		},
		{
			input: "BEGIN TRANSACTION; UPDATE table1 SET label = 'label1' WHERE id = 100; COMMIT;",
			expectedOutput: []SQLStmt{
				&BeginTransactionStmt{},
				&UpdateStmt{
					tableRef: &tableRef{table: "table1"},
					updates: []*colUpdate{
						{col: "label", op: EQ, val: &Varchar{val: "label1"}},
					},
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "id"},
						right: &Number{val: 100},
					},
				},
				&CommitStmt{},
			},
			expectedError: nil,
		},
		{
			input: "BEGIN TRANSACTION; CREATE TABLE table1 (id INTEGER, label VARCHAR NOT NULL, PRIMARY KEY id); UPSERT INTO table1 (id, label) VALUES (100, 'label1'); COMMIT;",
			expectedOutput: []SQLStmt{
				&BeginTransactionStmt{},
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "label", colType: VarcharType, notNull: true},
					},
					pkColNames: []string{"id"},
				},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "label"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 100}, &Varchar{val: "label1"}}},
					},
				},
				&CommitStmt{},
			},
			expectedError: nil,
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

func TestSelectStmt(t *testing.T) {
	bs, _ := hex.DecodeString("AED0393F")

	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "SELECT id, title FROM table1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{table: "table1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title FROM db1.table1 AS t1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{db: "db1", table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT t1.id, title FROM db1.table1 t1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{table: "t1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{db: "db1", table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT db1.table1.id, title FROM db1.table1 AS t1 WHERE payload >= x'AED0393F'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{db: "db1", table: "table1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{db: "db1", table: "table1", as: "t1"},
					where: &CmpBoolExp{
						op: GE,
						left: &ColSelector{
							col: "payload",
						},
						right: &Blob{val: bs},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT db1.table1.id, title FROM db1.table1 AS t1 WHERE id <> 1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{db: "db1", table: "table1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{db: "db1", table: "table1", as: "t1"},
					where: &CmpBoolExp{
						op: NE,
						left: &ColSelector{
							col: "id",
						},
						right: &Number{val: 1},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT db1.table1.id, title FROM db1.table1 AS t1 WHERE id != 1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{db: "db1", table: "table1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &tableRef{db: "db1", table: "table1", as: "t1"},
					where: &CmpBoolExp{
						op: NE,
						left: &ColSelector{
							col: "id",
						},
						right: &Number{val: 1},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT DISTINCT id, time, name FROM table1 WHERE country = 'US' AND time <= NOW() AND name = @pname",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: true,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "time"},
						&ColSelector{col: "name"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &BinBoolExp{
							op: AND,
							left: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									col: "country",
								},
								right: &Varchar{val: "US"},
							},
							right: &CmpBoolExp{
								op: LE,
								left: &ColSelector{
									col: "time",
								},
								right: &SysFn{fn: "now"},
							},
						},
						right: &CmpBoolExp{
							op: EQ,
							left: &ColSelector{
								col: "name",
							},
							right: &Param{id: "pname"},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title, year FROM table1 ORDER BY title ASC, year DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "title"},
						&ColSelector{col: "year"},
					},
					ds: &tableRef{table: "table1"},
					orderBy: []*OrdCol{
						{sel: &ColSelector{col: "title"}},
						{sel: &ColSelector{col: "year"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 INNER JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "name"},
						&ColSelector{table: "table2", col: "status"},
					},
					ds: &tableRef{table: "table1"},
					joins: []*JoinSpec{
						{
							joinType: InnerJoin,
							ds:       &tableRef{table: "table2"},
							cond: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: &ColSelector{
									table: "table2",
									col:   "id",
								},
							},
						},
					},
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "name"},
						right: &Varchar{val: "John"},
					},
					orderBy: []*OrdCol{
						{sel: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "name"},
						&ColSelector{table: "table2", col: "status"},
					},
					ds: &tableRef{table: "table1"},
					joins: []*JoinSpec{
						{
							joinType: InnerJoin,
							ds:       &tableRef{table: "table2"},
							cond: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: &ColSelector{
									table: "table2",
									col:   "id",
								},
							},
						},
					},
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "name"},
						right: &Varchar{val: "John"},
					},
					orderBy: []*OrdCol{
						{sel: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "name"},
						&ColSelector{table: "table2", col: "status"},
					},
					ds: &tableRef{table: "table1"},
					joins: []*JoinSpec{
						{
							joinType: LeftJoin,
							ds:       &tableRef{table: "table2"},
							cond: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: &ColSelector{
									table: "table2",
									col:   "id",
								},
							},
						},
					},
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "name"},
						right: &Varchar{val: "John"},
					},
					orderBy: []*OrdCol{
						{sel: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title FROM (SELECT col1 AS id, col2 AS title FROM table2 LIMIT 100) LIMIT 10",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &SelectStmt{
						distinct: false,
						selectors: []Selector{
							&ColSelector{col: "col1", as: "id"},
							&ColSelector{col: "col2", as: "title"},
						},
						ds:    &tableRef{table: "table2"},
						limit: 100,
					},
					limit: 10,
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, time FROM table1 WHERE time >= '20210101 00:00:00.000' AND time < '20210211 00:00:00.000'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "name"},
						&ColSelector{col: "time"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GE,
							left: &ColSelector{
								col: "time",
							},
							right: &Varchar{val: "20210101 00:00:00.000"},
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "time",
							},
							right: &Varchar{val: "20210211 00:00:00.000"},
						},
					},
				}},
			expectedError: nil,
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

func TestAggFnStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "SELECT COUNT(*) FROM table1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&AggColSelector{aggFn: COUNT, col: "*"},
					},
					ds: &tableRef{table: "table1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT country, SUM(amount) FROM table1 GROUP BY country HAVING SUM(amount) > 0",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "country"},
						&AggColSelector{aggFn: SUM, col: "amount"},
					},
					ds: &tableRef{table: "table1"},
					groupBy: []*ColSelector{
						{col: "country"},
					},
					having: &CmpBoolExp{
						op:    GT,
						left:  &AggColSelector{aggFn: SUM, col: "amount"},
						right: &Number{val: 0},
					},
				}},
			expectedError: nil,
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

func TestExpressions(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "SELECT id FROM table1 WHERE id > 0",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &CmpBoolExp{
						op: GT,
						left: &ColSelector{
							col: "id",
						},
						right: &Number{val: 0},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE NOT id > 0 AND id < 10",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &NotBoolExp{
							exp: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Number{val: 0},
							},
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "id",
							},
							right: &Number{val: 10},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE NOT (id > 0 AND id < 10)",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &NotBoolExp{
						exp: &BinBoolExp{
							op: AND,
							left: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Number{val: 0},
							},
							right: &CmpBoolExp{
								op: LT,
								left: &ColSelector{
									col: "id",
								},
								right: &Number{val: 10},
							},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE NOT active OR active",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: OR,
						left: &NotBoolExp{
							exp: &ColSelector{col: "active"},
						},
						right: &ColSelector{col: "active"},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE id > 0 AND NOT (table1.id >= 10)",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GT,
							left: &ColSelector{
								col: "id",
							},
							right: &Number{val: 0},
						},
						right: &NotBoolExp{
							exp: &CmpBoolExp{
								op: GE,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: &Number{val: 10},
							},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE table1.title LIKE 'J%O'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &LikeBoolExp{
						val: &ColSelector{
							table: "table1",
							col:   "title",
						},
						pattern: &Varchar{val: "J%O"},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE table1.title LIKE @param1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &LikeBoolExp{
						val: &ColSelector{
							table: "table1",
							col:   "title",
						},
						pattern: &Param{id: "param1"},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE (id > 0 AND NOT table1.id >= 10) OR table1.title LIKE 'J%O'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: OR,
						left: &BinBoolExp{
							op: AND,
							left: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Number{val: 0},
							},
							right: &NotBoolExp{
								exp: &CmpBoolExp{
									op: GE,
									left: &ColSelector{
										table: "table1",
										col:   "id",
									},
									right: &Number{val: 10},
								},
							},
						},
						right: &LikeBoolExp{
							val: &ColSelector{
								table: "table1",
								col:   "title",
							},
							pattern: &Varchar{val: "J%O"},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM clients WHERE EXISTS (SELECT id FROM orders WHERE clients.id = orders.id_client)",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "clients"},
					where: &ExistsBoolExp{
						q: &SelectStmt{
							selectors: []Selector{
								&ColSelector{col: "id"},
							},
							ds: &tableRef{table: "orders"},
							where: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									table: "clients",
									col:   "id",
								},
								right: &ColSelector{
									table: "orders",
									col:   "id_client",
								},
							},
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM clients WHERE deleted_at IS NULL",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "clients"},
					where: &CmpBoolExp{
						left: &ColSelector{
							col: "deleted_at",
						},
						op: EQ,
						right: &NullValue{
							t: AnyType,
						},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM clients WHERE deleted_at IS NOT NULL",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &tableRef{table: "clients"},
					where: &CmpBoolExp{
						left: &ColSelector{
							col: "deleted_at",
						},
						op: NE,
						right: &NullValue{
							t: AnyType,
						},
					},
				}},
			expectedError: nil,
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

func TestMultiLineStmts(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: `

			/*
				SAMPLE MULTILINE COMMENT
				IMMUDB SQL SCRIPT
			*/

			CREATE DATABASE db1;

			CREATE TABLE table1 (id INTEGER, name VARCHAR NULL, ts TIMESTAMP NOT NULL, active BOOLEAN, content BLOB, PRIMARY KEY id);

			BEGIN TRANSACTION;
				UPSERT INTO table1 (id, label) VALUES (100, 'label1');

				UPSERT INTO table2 (id) VALUES (10);
			COMMIT;

			SELECT id, name, time FROM table1 WHERE time >= '20210101 00:00:00.000' AND time < '20210211 00:00:00.000';

			`,
			expectedOutput: []SQLStmt{
				&CreateDatabaseStmt{DB: "db1"},
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "name", colType: VarcharType},
						{colName: "ts", colType: TimestampType, notNull: true},
						{colName: "active", colType: BooleanType},
						{colName: "content", colType: BLOBType},
					},
					pkColNames: []string{"id"},
				},
				&BeginTransactionStmt{},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "label"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 100}, &Varchar{val: "label1"}}},
					},
				},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table2"},
					cols:     []string{"id"},
					rows: []*RowSpec{
						{Values: []ValueExp{&Number{val: 10}}},
					},
				},
				&CommitStmt{},
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "name"},
						&ColSelector{col: "time"},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GE,
							left: &ColSelector{
								col: "time",
							},
							right: &Varchar{val: "20210101 00:00:00.000"},
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "time",
							},
							right: &Varchar{val: "20210211 00:00:00.000"},
						},
					},
				},
			},
			expectedError: nil,
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
