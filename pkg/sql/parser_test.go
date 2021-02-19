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
			expectedError:  errors.New("syntax error: unexpected IDENTIFIER, expecting DATABASE or SNAPSHOT"),
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
			input: "USE SNAPSHOT SINCE '20210211 00:00:00.000'",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{since: "20210211 00:00:00.000"},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT UP TO '20210214 00:00:00.000'",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{upTo: "20210214 00:00:00.000"},
			},
			expectedError: nil,
		},
		{
			input: "USE SNAPSHOT SINCE '20210211 00:00:00.000' UP TO '20210214 00:00:00.000'",
			expectedOutput: []SQLStmt{
				&UseSnapshotStmt{since: "20210211 00:00:00.000", upTo: "20210214 00:00:00.000"},
			},
			expectedError: nil,
		},
		{
			input:          "USE SNAPSHOT SINCE UP TO '20210214 00:00:00.000'",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected UP, expecting STRING"),
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
			input: "INSERT INTO table1(id, time, title, active, compressed, payload, note) VALUES (2, TIME(), 'untitled row', TRUE, false, b'AED0393F', @param1)",
			expectedOutput: []SQLStmt{
				&InsertIntoStmt{
					table: "table1",
					cols:  []string{"id", "time", "title", "active", "compressed", "payload", "note"},
					rows: []*Row{
						{values: []Value{uint64(2), &SysFn{fn: "TIME"}, "untitled row", true, false, decodedBLOB, &Param{id: "param1"}}},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "INSERT INTO table1(id, active) VALUES (1, false), (2, true), (3, true)",
			expectedOutput: []SQLStmt{
				&InsertIntoStmt{
					table: "table1",
					cols:  []string{"id", "active"},
					rows: []*Row{
						{values: []Value{uint64(1), false}},
						{values: []Value{uint64(2), true}},
						{values: []Value{uint64(3), true}},
					},
				},
			},
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
			input: "CREATE DATABASE db1; /* some comment here */ USE DATABASE db1; CREATE /* another comment here */ TABLE table1",
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
			input: "BEGIN TRANSACTION; INSERT INTO table1 (id, label) VALUES (100, 'label1'); INSERT INTO table2 (id) VALUES (10) COMMIT;",
			expectedOutput: []SQLStmt{
				&TxStmt{
					stmts: []SQLStmt{
						&InsertIntoStmt{
							table: "table1",
							cols:  []string{"id", "label"},
							rows: []*Row{
								{values: []Value{uint64(100), "label1"}},
							},
						},
						&InsertIntoStmt{
							table: "table2",
							cols:  []string{"id"},
							rows: []*Row{
								{values: []Value{uint64(10)}},
							},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "CREATE TABLE table1; BEGIN TRANSACTION; INSERT INTO table1 (id, label) VALUES (100, 'label1'); COMMIT;",
			expectedOutput: []SQLStmt{
				&CreateTableStmt{
					table: "table1",
				},
				&TxStmt{
					stmts: []SQLStmt{
						&InsertIntoStmt{
							table: "table1",
							cols:  []string{"id", "label"},
							rows: []*Row{
								{values: []Value{uint64(100), "label1"}},
							},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input: "BEGIN TRANSACTION; CREATE TABLE table1; INSERT INTO table1 (id, label) VALUES (100, 'label1') COMMIT;",
			expectedOutput: []SQLStmt{
				&TxStmt{
					stmts: []SQLStmt{
						&CreateTableStmt{
							table: "table1",
						},
						&InsertIntoStmt{
							table: "table1",
							cols:  []string{"id", "label"},
							rows: []*Row{
								{values: []Value{uint64(100), "label1"}},
							},
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			input:          "BEGIN TRANSACTION; INSERT INTO table1 (id, label) VALUES (100, 'label1');",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected $end, expecting COMMIT"),
		},
		{
			input:          "BEGIN TRANSACTION; INSERT INTO table1 (id, label) VALUES (100, 'label1'); BEGIN TRANSACTION; CREATE TABLE table1; COMMIT; COMMIT",
			expectedOutput: nil,
			expectedError:  errors.New("syntax error: unexpected BEGIN, expecting COMMIT"),
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
					ds: &TableRef{table: "table1"},
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
					ds: &TableRef{db: "db1", table: "table1"},
					as: "t1",
				}},
			expectedError: nil,
		},
		{
			input: "SELECT t1.id, title FROM (db1.table1 AS t1)",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{table: "t1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &TableRef{db: "db1", table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT db1.table1.id, title FROM (db1.table1 AS t1)",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{db: "db1", table: "table1", col: "id"},
						&ColSelector{col: "title"},
					},
					ds: &TableRef{db: "db1", table: "table1", as: "t1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT DISTINCT id, time, name FROM table1 WHERE country = 'US' AND time <= TIME() AND name = @pname",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: true,
					selectors: []Selector{
						&ColSelector{col: "id"},
						&ColSelector{col: "time"},
						&ColSelector{col: "name"},
					},
					ds: &TableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &BinBoolExp{
							op: AND,
							left: &CmpBoolExp{
								op: EQ,
								left: &ColSelector{
									col: "country",
								},
								right: "US",
							},
							right: &CmpBoolExp{
								op: LE,
								left: &ColSelector{
									col: "time",
								},
								right: &SysFn{fn: "TIME"},
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
					ds: &TableRef{table: "table1"},
					orderBy: []*OrdCol{
						{col: &ColSelector{col: "title"}, desc: false},
						{col: &ColSelector{col: "year"}, desc: true},
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
					ds: &TableRef{table: "table1"},
					join: &JoinSpec{
						joinType: InnerJoin,
						ds:       &TableRef{table: "table2"},
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
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "name"},
						right: "John",
					},
					orderBy: []*OrdCol{
						{col: &ColSelector{col: "name"}, desc: true},
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
					ds: &TableRef{table: "table1"},
					join: &JoinSpec{
						joinType: LeftJoin,
						ds:       &TableRef{table: "table2"},
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
					where: &CmpBoolExp{
						op:    EQ,
						left:  &ColSelector{col: "name"},
						right: "John",
					},
					orderBy: []*OrdCol{
						{col: &ColSelector{col: "name"}, desc: true},
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id, title FROM (SELECT col1 AS id, col2 AS title FROM table2 OFFSET 1 LIMIT 100) LIMIT 10",
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
						ds:     &TableRef{table: "table2"},
						offset: uint64(1),
						limit:  uint64(100),
					},
					limit: uint64(10),
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
					ds: &TableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GE,
							left: &ColSelector{
								col: "time",
							},
							right: "20210101 00:00:00.000",
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "time",
							},
							right: "20210211 00:00:00.000",
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
						&AggSelector{aggFn: COUNT},
					},
					ds: &TableRef{table: "table1"},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT country, SUM(amount) FROM table1 GROUP BY country",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "country"},
						&AggColSelector{aggFn: SUM, col: "amount"},
					},
					ds: &TableRef{table: "table1"},
					groupBy: []*ColSelector{
						{col: "country"},
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
					ds: &TableRef{table: "table1"},
					where: &CmpBoolExp{
						op: GT,
						left: &ColSelector{
							col: "id",
						},
						right: uint64(0),
					},
				}},
			expectedError: nil,
		},
		{
			input: "SELECT id FROM table1 WHERE id > 0 AND id < 10",
			expectedOutput: []SQLStmt{
				&SelectStmt{
					distinct: false,
					selectors: []Selector{
						&ColSelector{col: "id"},
					},
					ds: &TableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GT,
							left: &ColSelector{
								col: "id",
							},
							right: uint64(0),
						},
						right: &CmpBoolExp{
							op: LT,
							left: &ColSelector{
								col: "id",
							},
							right: uint64(10),
						},
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
					ds: &TableRef{table: "table1"},
					where: &BinBoolExp{
						op: AND,
						left: &CmpBoolExp{
							op: GT,
							left: &ColSelector{
								col: "id",
							},
							right: uint64(0),
						},
						right: &NotBoolExp{
							exp: &CmpBoolExp{
								op: GE,
								left: &ColSelector{
									table: "table1",
									col:   "id",
								},
								right: uint64(10),
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
					ds: &TableRef{table: "table1"},
					where: &LikeBoolExp{
						col: &ColSelector{
							table: "table1",
							col:   "title",
						},
						pattern: "J%O",
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
					ds: &TableRef{table: "table1"},
					where: &BinBoolExp{
						op: OR,
						left: &BinBoolExp{
							op: AND,
							left: &CmpBoolExp{
								op: GT,
								left: &ColSelector{
									col: "id",
								},
								right: uint64(0),
							},
							right: &NotBoolExp{
								exp: &CmpBoolExp{
									op: GE,
									left: &ColSelector{
										table: "table1",
										col:   "id",
									},
									right: uint64(10),
								},
							},
						},
						right: &LikeBoolExp{
							col: &ColSelector{
								table: "table1",
								col:   "title",
							},
							pattern: "J%O",
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
					ds: &TableRef{table: "clients"},
					where: &ExistsBoolExp{
						q: &SelectStmt{
							selectors: []Selector{
								&ColSelector{col: "id"},
							},
							ds: &TableRef{table: "orders"},
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
	}

	for i, tc := range testCases {
		res, err := ParseString(tc.input)
		require.Equal(t, tc.expectedError, err, fmt.Sprintf("failed on iteration %d", i))

		if tc.expectedError == nil {
			require.Equal(t, tc.expectedOutput, res, fmt.Sprintf("failed on iteration %d", i))
		}
	}
}
