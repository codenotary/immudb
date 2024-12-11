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

package sql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func init() {
	yyErrorVerbose = true
}

func TestEmptyInput(t *testing.T) {
	_, err := ParseSQLString("")
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
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER at position 10"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
			input:          "USE db1",
			expectedOutput: []SQLStmt{&UseDatabaseStmt{DB: "db1"}},
			expectedError:  nil,
		},
		{
			input:          "USE DATABASE db1",
			expectedOutput: []SQLStmt{&UseDatabaseStmt{DB: "db1"}},
			expectedError:  nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
				&UseSnapshotStmt{
					period: period{
						start: &openPeriod{instant: periodInstant{instantType: txInstant, exp: &Integer{val: 100}}, inclusive: true},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT BEFORE now()",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{
					period: period{
						end: &openPeriod{instant: periodInstant{instantType: timeInstant, exp: &FnCall{fn: "now"}}},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT UNTIL now()",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{
					period: period{
						end: &openPeriod{instant: periodInstant{instantType: timeInstant, exp: &FnCall{fn: "now"}}, inclusive: true},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT SINCE TX 1 UNTIL TX 10",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{
					period: period{
						start: &openPeriod{instant: periodInstant{instantType: txInstant, exp: &Integer{val: 1}}, inclusive: true},
						end:   &openPeriod{instant: periodInstant{instantType: txInstant, exp: &Integer{val: 10}}, inclusive: true},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT SINCE TX @fromTx BEFORE TX 10",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{
					period: period{
						start: &openPeriod{instant: periodInstant{instantType: txInstant, exp: &Param{id: "fromtx"}}, inclusive: true},
						end:   &openPeriod{instant: periodInstant{instantType: txInstant, exp: &Integer{val: 10}}},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT AFTER TX @fromTx-1 BEFORE now()",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{
					period: period{
						start: &openPeriod{
							instant: periodInstant{
								instantType: txInstant,
								exp:         &NumExp{op: SUBSOP, left: &Param{id: "fromtx"}, right: &Integer{val: 1}},
							},
						},
						end: &openPeriod{instant: periodInstant{instantType: timeInstant, exp: &FnCall{fn: "now"}}},
					},
				},
			},
			expectedError: nil,
		},
		{
			input:          "USE SNAPSHOT BEFORE TX 10 SINCE TX 1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected SINCE at position 31"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
			input: "CREATE TABLE table1 (id UUID, PRIMARY KEY id)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table1",
					ifNotExists: false,
					colsSpec:    []*ColSpec{{colName: "id", colType: UUIDType}},
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
			input: "CREATE TABLE \"table\" (\"primary\" INTEGER AUTO_INCREMENT, PRIMARY KEY \"primary\")",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table:       "table",
					ifNotExists: false,
					colsSpec:    []*ColSpec{{colName: "primary", colType: IntegerType, autoIncrement: true}},
					pkColNames:  []string{"primary"},
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
			input: "CREATE TABLE table1 (id INTEGER, name VARCHAR(50), ts TIMESTAMP, active BOOLEAN, content BLOB, json_data JSON, PRIMARY KEY (id, name))",
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
						{colName: "json_data", colType: JSONType},
					},
					pkColNames: []string{"id", "name"},
				}},
			expectedError: nil,
		},
		{
			input:          "CREATE table1",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER at position 13"),
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
		{
			input: "CREATE TABLE table1(id INTEGER, balance FLOAT, CONSTRAINT non_negative_balance CHECK (balance >= 0), PRIMARY KEY id)",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
					colsSpec: []*ColSpec{
						{colName: "id", colType: IntegerType},
						{colName: "balance", colType: Float64Type},
					},
					checks: []CheckConstraint{
						{
							name: "non_negative_balance",
							exp: &CmpBoolExp{
								op:    GE,
								left:  &ColSelector{col: "balance"},
								right: &Integer{val: 0},
							},
						},
					},
					pkColNames: []string{"id"},
				}},
			expectedError: nil,
		},
		{
			input: "DROP TABLE table1",
			expectedOutput: []SQLStmt{
				&DropTableStmt{
					table: "table1",
				}},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
			input:          "CREATE INDEX ON \"table\"(\"primary\")",
			expectedOutput: []SQLStmt{&CreateIndexStmt{table: "table", cols: []string{"primary"}}},
			expectedError:  nil,
		},
		{
			input:          "CREATE INDEX ON \"table(\"primary\")",
			expectedOutput: []SQLStmt{&CreateIndexStmt{table: "table", cols: []string{"primary"}}},
			expectedError:  errors.New("syntax error: unexpected ERROR, expecting IDENTIFIER at position 22"),
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
		{
			input: "DROP INDEX ON table1(id, title)",
			expectedOutput: []SQLStmt{
				&DropIndexStmt{
					table: "table1",
					cols:  []string{"id", "title"},
				}},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestAlterTable(t *testing.T) {
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
			expectedError:  errors.New("syntax error: unexpected COLUMN, expecting DROP or ADD or RENAME at position 25"),
		},
		{
			input: "ALTER TABLE table1 RENAME COLUMN title TO newtitle",
			expectedOutput: []SQLStmt{
				&RenameColumnStmt{
					table:   "table1",
					oldName: "title",
					newName: "newtitle",
				}},
			expectedError: nil,
		},
		{
			input:          "ALTER TABLE table1 RENAME COLUMN TO newtitle",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected TO, expecting IDENTIFIER at position 35"),
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
					ds: &valuesDataSource{
						rows: []*RowSpec{{
							Values: []ValueExp{
								&Integer{val: 2},
								&FnCall{fn: "now"},
								&Varchar{val: "un'titled row"},
								&Bool{val: true},
								&Bool{val: false},
								&Blob{val: decodedBLOB},
								&Param{id: "param1"},
							}},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{
								&Integer{val: 2},
								&FnCall{fn: "now"},
								&Varchar{val: ""},
								&Bool{val: true},
								&Bool{val: false},
								&Blob{val: decodedBLOB},
								&Param{id: "param1"},
							}},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{
								&Integer{val: 2},
								&FnCall{fn: "now"},
								&Varchar{val: "'"},
								&Bool{val: true},
								&Bool{val: false},
								&Blob{val: decodedBLOB},
								&Param{id: "param1"},
							}}},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{
								&Integer{val: 2},
								&FnCall{fn: "now"},
								&Varchar{val: "untitled row"},
								&Bool{val: true},
								&Param{id: "param1", pos: 1},
								&Blob{val: decodedBLOB},
								&Param{id: "param2", pos: 2},
							}},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{
								&Integer{val: 2},
								&FnCall{fn: "now"},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 1}, &Bool{val: false}}},
							{Values: []ValueExp{&Integer{val: 2}, &Bool{val: true}}},
							{Values: []ValueExp{&Integer{val: 3}, &Bool{val: true}}},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "INSERT INTO table1(id, active) SELECT * FROM my_table",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					isInsert: true,
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "active"},
					ds: &SelectStmt{
						ds: &tableRef{
							table: "my_table",
						},
						targets: nil,
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "UPSERT INTO table1(id, active) SELECT * FROM my_table WHERE balance >= 0 AND deleted_at IS NULL",
			expectedOutput: []SQLStmt{
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table1"},
					cols:     []string{"id", "active"},
					ds: &SelectStmt{
						ds: &tableRef{
							table: "my_table",
						},
						targets: nil,
						where: &BinBoolExp{
							op:    And,
							left:  &CmpBoolExp{op: GE, left: &ColSelector{col: "balance"}, right: &Integer{val: 0}},
							right: &CmpBoolExp{op: EQ, left: &ColSelector{col: "deleted_at"}, right: &NullValue{t: AnyType}},
						},
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
		res, err := ParseSQLString(tc.input)
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
		res, err := ParseSQLString(tc.input)
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 100}, &Varchar{val: "label1"}}},
						},
					},
				},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table2"},
					cols:     []string{"id"},
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 10}}},
						},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 100}, &Varchar{val: "label1"}}},
						},
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
						right: &Integer{val: 100},
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 100}, &Varchar{val: "label1"}}},
						},
					},
				},
				&CommitStmt{},
			},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title FROM table1 AS t1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT t1.id, title FROM table1 t1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{table: "t1", col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT table1.id, title FROM table1 AS t1 WHERE payload >= x'AED0393F'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{table: "table1", col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1", as: "t1"},
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
			input: "SELECT table1.id, title FROM table1 AS t1 WHERE id <> 1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{table: "table1", col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1", as: "t1"},
					where: &CmpBoolExp{
						op: NE,
						left: &ColSelector{
							col: "id",
						},
						right: &Integer{val: 1},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT table1.id, title FROM table1 AS t1 WHERE id != 1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{table: "table1", col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &tableRef{table: "table1", as: "t1"},
					where: &CmpBoolExp{
						op: NE,
						left: &ColSelector{
							col: "id",
						},
						right: &Integer{val: 1},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT DISTINCT id, time, name FROM table1 WHERE country = 'US' AND time <= NOW() AND name = @pname",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: true,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "time"}},
						{Exp: &ColSelector{col: "name"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: And,
						left: &BinBoolExp{
							op: And,
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
								right: &FnCall{fn: "now"},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "title"}},
						{Exp: &ColSelector{col: "year"}},
					},
					ds: &tableRef{table: "table1"},
					orderBy: []*OrdExp{
						{exp: &ColSelector{col: "title"}},
						{exp: &ColSelector{col: "year"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 INNER JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "name"}},
						{Exp: &ColSelector{table: "table2", col: "status"}},
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
					orderBy: []*OrdExp{
						{exp: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "name"}},
						{Exp: &ColSelector{table: "table2", col: "status"}},
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
					orderBy: []*OrdExp{
						{exp: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, table2.status FROM table1 LEFT JOIN table2 ON table1.id = table2.id WHERE name = 'John' ORDER BY name DESC",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "name"}},
						{Exp: &ColSelector{table: "table2", col: "status"}},
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
					orderBy: []*OrdExp{
						{exp: &ColSelector{col: "name"}, descOrder: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title FROM (SELECT col1 AS id, col2 AS title FROM table2 LIMIT 100 OFFSET 1) LIMIT 10",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "title"}},
					},
					ds: &SelectStmt{
						distinct: false,
						targets: []TargetEntry{
							{Exp: &ColSelector{col: "col1"}, As: "id"},
							{Exp: &ColSelector{col: "col2"}, As: "title"},
						},
						ds:     &tableRef{table: "table2"},
						limit:  &Integer{val: 100},
						offset: &Integer{val: 1},
					},
					limit: &Integer{val: 10},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, name, time FROM table1 WHERE time >= '20210101 00:00:00.000' AND time < '20210211 00:00:00.000'",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "name"}},
						{Exp: &ColSelector{col: "time"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: And,
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
		{
			input: "SELECT json_data->'info'->'address'->'street' FROM table1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{
							Exp: &JSONSelector{
								ColSelector: &ColSelector{col: "json_data"},
								fields:      []string{"info", "address", "street"},
							},
						},
					},
					ds: &tableRef{table: "table1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT 1, (balance * balance) + 1, amount % 2, data::JSON FROM table1",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{
							Exp: &Integer{
								val: int64(1),
							},
						},
						{
							Exp: &NumExp{
								op: ADDOP,
								left: &NumExp{
									op:    MULTOP,
									left:  &ColSelector{col: "balance"},
									right: &ColSelector{col: "balance"},
								},
								right: &Integer{val: int64(1)},
							},
						},
						{
							Exp: &NumExp{
								op:    MODOP,
								left:  &ColSelector{col: "amount"},
								right: &Integer{val: int64(2)},
							},
						},
						{
							Exp: &Cast{val: &ColSelector{col: "data"}, t: JSONType},
						},
					},
					ds: &tableRef{table: "table1"},
				}},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestSelectUnionStmt(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []SQLStmt
		expectedError  error
	}{
		{
			input: "SELECT id, title FROM table1 UNION SELECT id, title FROM table1",
			expectedOutput: []SQLStmt{
				&UnionStmt{
					distinct: true,
					left: &SelectStmt{
						distinct: false,
						targets: []TargetEntry{
							{Exp: &ColSelector{col: "id"}},
							{Exp: &ColSelector{col: "title"}},
						},
						ds: &tableRef{table: "table1"},
					},
					right: &SelectStmt{
						distinct: false,
						targets: []TargetEntry{
							{Exp: &ColSelector{col: "id"}},
							{Exp: &ColSelector{col: "title"}},
						},
						ds: &tableRef{table: "table1"},
					}},
			},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
					targets: []TargetEntry{
						{Exp: &AggColSelector{aggFn: COUNT, col: "*"}},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "country"}},
						{Exp: &AggColSelector{aggFn: SUM, col: "amount"}},
					},
					ds: &tableRef{table: "table1"},
					groupBy: []*ColSelector{
						{col: "country"},
					},
					having: &CmpBoolExp{
						op:    GT,
						left:  &AggColSelector{aggFn: SUM, col: "amount"},
						right: &Integer{val: 0},
					},
				}},
			expectedError: nil,
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestParseExp(t *testing.T) {
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &CmpBoolExp{
						op: GT,
						left: &ColSelector{
							col: "id",
						},
						right: &Integer{val: 0},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE NOT id > 0 AND id < 10",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: And,
						left: &NotBoolExp{
							exp: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Integer{val: 0},
							},
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "id",
							},
							right: &Integer{val: 10},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &NotBoolExp{
						exp: &BinBoolExp{
							op: And,
							left: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Integer{val: 0},
							},
							right: &CmpBoolExp{
								op: LT,
								left: &ColSelector{
									col: "id",
								},
								right: &Integer{val: 10},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: Or,
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: And,
						left: &CmpBoolExp{
							op: GT,
							left: &ColSelector{
								col: "id",
							},
							right: &Integer{val: 0},
						},
						right: &NotBoolExp{
							exp: &CmpBoolExp{
								op: GE,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: &Integer{val: 10},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: Or,
						left: &BinBoolExp{
							op: And,
							left: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: &Integer{val: 0},
							},
							right: &NotBoolExp{
								exp: &CmpBoolExp{
									op: GE,
									left: &ColSelector{
										table: "table1",
										col:   "id",
									},
									right: &Integer{val: 10},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
					},
					ds: &tableRef{table: "clients"},
					where: &ExistsBoolExp{
						q: &SelectStmt{
							targets: []TargetEntry{
								{Exp: &ColSelector{col: "id"}},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
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
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
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
		{
			input: "SELECT CASE 1 + 1 WHEN 2 THEN 1 ELSE 0 END FROM my_table",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "my_table"},
					targets: []TargetEntry{
						{
							Exp: &CaseWhenExp{
								exp: &NumExp{
									op:    ADDOP,
									left:  &Integer{1},
									right: &Integer{1},
								},
								whenThen: []whenThenClause{
									{
										when: &Integer{2},
										then: &Integer{1},
									},
								},
								elseExp: &Integer{0},
							},
						},
					},
				},
			},
		},
		{
			input: "SELECT CASE WHEN is_deleted OR is_expired THEN 1 END AS is_deleted_or_expired FROM my_table",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "my_table"},
					targets: []TargetEntry{
						{
							Exp: &CaseWhenExp{
								whenThen: []whenThenClause{
									{
										when: &BinBoolExp{
											op:    Or,
											left:  &ColSelector{col: "is_deleted"},
											right: &ColSelector{col: "is_expired"},
										},
										then: &Integer{1},
									},
								},
							},
							As: "is_deleted_or_expired",
						},
					},
				},
			},
		},
		{
			input: "SELECT CASE WHEN is_deleted OR is_expired THEN 1 END AS is_deleted_or_expired FROM my_table",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "my_table"},
					targets: []TargetEntry{
						{
							Exp: &CaseWhenExp{
								whenThen: []whenThenClause{
									{
										when: &BinBoolExp{
											op:    Or,
											left:  &ColSelector{col: "is_deleted"},
											right: &ColSelector{col: "is_expired"},
										},
										then: &Integer{1},
									},
								},
							},
							As: "is_deleted_or_expired",
						},
					},
				},
			},
		},
		{
			input: "SELECT CASE WHEN is_active THEN 1 ELSE 2 END FROM my_table",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "my_table"},
					targets: []TargetEntry{
						{
							Exp: &CaseWhenExp{
								whenThen: []whenThenClause{
									{
										when: &ColSelector{col: "is_active"},
										then: &Integer{1},
									},
								},
								elseExp: &Integer{2},
							},
						},
					},
				},
			},
		},
		{
			input: `
				SELECT product_name,
					CASE
						WHEN stock < 10 THEN 'Low stock'
						WHEN stock >= 10 AND stock <= 50 THEN 'Medium stock'
						WHEN stock > 50 THEN 'High stock'
						ELSE 'Out of stock'
					END AS stock_status
				FROM products
			`,
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "products"},
					targets: []TargetEntry{
						{
							Exp: &ColSelector{col: "product_name"},
						},
						{
							Exp: &CaseWhenExp{
								whenThen: []whenThenClause{
									{
										when: &CmpBoolExp{op: LT, left: &ColSelector{col: "stock"}, right: &Integer{10}},
										then: &Varchar{"Low stock"},
									},
									{
										when: &BinBoolExp{
											op:    And,
											left:  &CmpBoolExp{op: GE, left: &ColSelector{col: "stock"}, right: &Integer{10}},
											right: &CmpBoolExp{op: LE, left: &ColSelector{col: "stock"}, right: &Integer{50}},
										},
										then: &Varchar{"Medium stock"},
									},
									{
										when: &CmpBoolExp{op: GT, left: &ColSelector{col: "stock"}, right: &Integer{50}},
										then: &Varchar{"High stock"},
									},
								},
								elseExp: &Varchar{"Out of stock"},
							},
							As: "stock_status",
						},
					},
				},
			},
		},
		{
			input: "SELECT name !~ 'laptop.*' FROM products",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					ds: &tableRef{table: "products"},
					targets: []TargetEntry{
						{
							Exp: NewLikeBoolExp(NewColSelector("", "name"), true, NewVarchar("laptop.*")),
						},
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		res, err := ParseSQLString(tc.input)
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
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 100}, &Varchar{val: "label1"}}},
						},
					},
				},
				&UpsertIntoStmt{
					tableRef: &tableRef{table: "table2"},
					cols:     []string{"id"},
					ds: &valuesDataSource{
						rows: []*RowSpec{
							{Values: []ValueExp{&Integer{val: 10}}},
						},
					},
				},
				&CommitStmt{},
				&SelectStmt{
					distinct: false,
					targets: []TargetEntry{
						{Exp: &ColSelector{col: "id"}},
						{Exp: &ColSelector{col: "name"}},
						{Exp: &ColSelector{col: "time"}},
					},
					ds: &tableRef{table: "table1"},
					where: &BinBoolExp{
						op: And,
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
		res, err := ParseSQLString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}

func TestFloatCornerCases(t *testing.T) {
	for _, d := range []struct {
		s       string
		invalid bool
		v       ValueExp
	}{
		{"1", false, &Integer{val: 1}},
		{"1.", false, &Float64{val: 1}},
		{"1.1", false, &Float64{val: 1.1}},
		{"123.123ab1", true, nil},
		{"1aa23.1234", true, nil},
		{"123..1234", true, nil},
		{"123" + strings.Repeat("1", 10000) + ".123", true, nil},
	} {
		t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
			stmt, err := ParseSQLString("INSERT INTO t1(v) VALUES(" + d.s + ")")
			if d.invalid {
				require.Error(t, err)
				require.Contains(t, err.Error(), "syntax error")
			} else {
				require.NoError(t, err)
				require.Equal(t, []SQLStmt{
					&UpsertIntoStmt{
						isInsert: true,
						tableRef: &tableRef{
							table: "t1",
						},
						cols: []string{"v"},
						ds: &valuesDataSource{
							rows: []*RowSpec{{
								Values: []ValueExp{d.v},
							}},
						},
					},
				}, stmt)
			}
		})
	}
}

func TestGrantRevokeStmt(t *testing.T) {
	type test struct {
		text         string
		expectedStmt SQLStmt
	}

	cases := []test{
		{
			text: "GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE defaultdb TO USER immudb",
			expectedStmt: &AlterPrivilegesStmt{
				database: "defaultdb",
				user:     "immudb",
				privileges: []SQLPrivilege{
					SQLPrivilegeDelete,
					SQLPrivilegeUpdate,
					SQLPrivilegeInsert,
					SQLPrivilegeSelect,
				},
				isGrant: true,
			},
		},
		{
			text: "REVOKE SELECT, INSERT, UPDATE, DELETE ON DATABASE defaultdb TO USER immudb",
			expectedStmt: &AlterPrivilegesStmt{
				database: "defaultdb",
				user:     "immudb",
				privileges: []SQLPrivilege{
					SQLPrivilegeDelete,
					SQLPrivilegeUpdate,
					SQLPrivilegeInsert,
					SQLPrivilegeSelect,
				},
			},
		},
		{
			text: "GRANT ALL PRIVILEGES ON DATABASE defaultdb TO USER immudb",
			expectedStmt: &AlterPrivilegesStmt{
				database:   "defaultdb",
				user:       "immudb",
				privileges: allPrivileges,
				isGrant:    true,
			},
		},
		{
			text: "REVOKE ALL PRIVILEGES ON DATABASE defaultdb TO USER immudb",
			expectedStmt: &AlterPrivilegesStmt{
				database:   "defaultdb",
				user:       "immudb",
				privileges: allPrivileges,
			},
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("alter_privileges_%d", i), func(t *testing.T) {
			stmts, err := ParseSQLString(tc.text)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Equal(t, tc.expectedStmt, stmts[0])
		})
	}
}

func TestExpString(t *testing.T) {
	exps := []string{
		"(1 + 1) / (2 * 5 - 10) % 2",
		"@param LIKE 'pattern'",
		"((col1 AND (col2 < 10)) OR (@param = 3 AND (col4 = TRUE))) AND NOT (col5 = 'value' OR (2 + 2 != 4))",
		"CAST (func_call(1, 'two', 2.5) AS TIMESTAMP)",
		"col IN (TRUE, 1, 'test', 1.5)",
		"CASE WHEN in_stock THEN 'In Stock' END",
		"CASE WHEN 1 > 0 THEN 1 ELSE 0 END",
		"CASE WHEN is_active THEN 'active' WHEN is_expired THEN 'expired' ELSE 'active' END",
		"'text' LIKE 'pattern'",
		"'text' NOT LIKE 'pattern'",
	}

	for i, e := range exps {
		t.Run(fmt.Sprintf("test_expression_%d", i+1), func(t *testing.T) {
			exp, err := ParseExpFromString(e)
			require.NoError(t, err)

			parsedExp, err := ParseExpFromString(exp.String())
			require.NoError(t, err)
			require.Equal(t, exp, parsedExp)
		})
	}
}

func TestLogicOperatorPrecedence(t *testing.T) {
	type testCase struct {
		input    string
		expected string
	}

	testCases := []testCase{
		// simple precedence
		{input: "NOT true", expected: "(NOT true)"},
		{input: "true AND false OR true", expected: "((true AND false) OR true)"},
		{input: "NOT true AND false", expected: "((NOT true) AND false)"},
		{input: "NOT true OR false", expected: "((NOT true) OR false)"},

		// parentheses override precedence
		{input: "(true OR false) AND true", expected: "((true OR false) AND true)"},

		// multiple NOTs
		{input: "NOT NOT true AND false", expected: "((NOT (NOT true)) AND false)"},

		// complex nesting
		{input: "true AND (false OR (NOT false))", expected: "(true AND (false OR (NOT false)))"},
		{input: "NOT (true AND false) OR true", expected: "((NOT (true AND false)) OR true)"},

		// AND/OR with nested groups
		{input: "(true AND false) AND (true OR false)", expected: "((true AND false) AND (true OR false))"},
		{input: "(true OR false) OR (NOT (true AND false))", expected: "((true OR false) OR (NOT (true AND false)))"},

		// deep nesting
		{input: "(true AND (false OR (NOT true))) OR (NOT false)", expected: "((true AND (false OR (NOT true))) OR (NOT false))"},

		// chain of operators
		{input: "true AND false OR true AND NOT false OR true", expected: "(((true AND false) OR (true AND (NOT false))) OR true)"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			e, err := ParseExpFromString(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, e.String())
		})
	}
}
