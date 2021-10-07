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

%{
package sql

import "fmt"

func setResult(l yyLexer, stmts []SQLStmt) {
    l.(*lexer).result = stmts
}
%}

%union{
    stmts []SQLStmt
    stmt SQLStmt
    colsSpec []*ColSpec
    colSpec *ColSpec
    cols []*ColSelector
    rows []*RowSpec
    row *RowSpec
    values []ValueExp
    value ValueExp
    id string
    number uint64
    str string
    boolean bool
    blob []byte
    sqlType SQLValueType
    aggFn AggregateFn
    ids []string
    col *ColSelector
    sel Selector
    sels []Selector
    distinct bool
    ds DataSource
    tableRef *tableRef
    joins []*JoinSpec
    join *JoinSpec
    joinType JoinType
    exp ValueExp
    binExp ValueExp
    err error
    ordcols []*OrdCol
    opt_ord Order
    logicOp LogicOperator
    cmpOp CmpOperator
    pparam int
}

%token CREATE USE DATABASE SNAPSHOT SINCE UP TO TABLE UNIQUE INDEX ON ALTER ADD COLUMN PRIMARY KEY
%token BEGIN TRANSACTION COMMIT
%token INSERT UPSERT INTO VALUES
%token SELECT DISTINCT FROM BEFORE TX JOIN HAVING WHERE GROUP BY LIMIT ORDER ASC DESC AS
%token NOT LIKE IF EXISTS IN
%token AUTO_INCREMENT NULL NPARAM
%token <pparam> PPARAM
%token <joinType> JOINTYPE
%token <logicOp> LOP
%token <cmpOp> CMPOP
%token <id> IDENTIFIER
%token <sqlType> TYPE
%token <number> NUMBER
%token <str> VARCHAR
%token <boolean> BOOLEAN
%token <blob> BLOB
%token <aggFn> AGGREGATE_FUNC
%token <err> ERROR

%left  ','
%right AS
%left  LOP
%right LIKE
%right NOT
%left  CMPOP
%left '+' '-'
%left '*' '/'
%left  '.'
%right STMT_SEPARATOR

%type <stmts> sql
%type <stmts> sqlstmts dstmts
%type <stmt> sqlstmt dstmt ddlstmt dmlstmt dqlstmt
%type <colsSpec> colsSpec
%type <colSpec> colSpec
%type <ids> ids one_or_more_ids opt_ids
%type <cols> cols
%type <rows> rows
%type <row> row
%type <values> values opt_values
%type <value> val
%type <sel> selector
%type <sels> opt_selectors selectors
%type <col> col
%type <distinct> opt_distinct
%type <ds> ds
%type <tableRef> tableRef
%type <number> opt_since opt_as_before
%type <joins> opt_joins joins
%type <join> join
%type <exp> exp opt_where opt_having
%type <binExp> binExp
%type <cols> opt_groupby
%type <number> opt_limit opt_max_len
%type <id> opt_as
%type <ordcols> ordcols opt_orderby
%type <opt_ord> opt_ord
%type <ids> opt_indexon
%type <boolean> opt_if_not_exists opt_auto_increment opt_not_null opt_not

%start sql
    
%%

sql: sqlstmts
{
    $$ = $1
    setResult(yylex, $1)
}

sqlstmts:
    sqlstmt opt_separator
    {
        $$ = []SQLStmt{$1}
    }
|
    dqlstmt opt_separator
    {
        $$ = []SQLStmt{$1}
    }
|
    sqlstmt STMT_SEPARATOR sqlstmts
    {
        $$ = append([]SQLStmt{$1}, $3...)
    }

opt_separator: {} | STMT_SEPARATOR

sqlstmt:
    dstmt
    {
        $$ = $1
    }
|
    BEGIN TRANSACTION dstmts COMMIT
    {
        $$ = &TxStmt{stmts: $3}
    }

dstmt: ddlstmt | dmlstmt

dstmts:
    dstmt opt_separator
    {
        $$ = []SQLStmt{$1}
    }
|
    dstmt STMT_SEPARATOR dstmts
    {
        $$ = append([]SQLStmt{$1}, $3...)
    }

ddlstmt:
    CREATE DATABASE IDENTIFIER
    {
        $$ = &CreateDatabaseStmt{DB: $3}
    }
|
    USE DATABASE IDENTIFIER
    {
        $$ = &UseDatabaseStmt{DB: $3}
    }
|
    USE SNAPSHOT opt_since opt_as_before 
    {
        $$ = &UseSnapshotStmt{sinceTx: $3, asBefore: $4}
    }
|
    CREATE TABLE opt_if_not_exists IDENTIFIER '(' colsSpec ',' PRIMARY KEY one_or_more_ids ')'
    {
        $$ = &CreateTableStmt{ifNotExists: $3, table: $4, colsSpec: $6, pkColNames: $10}
    }
|
    CREATE INDEX ON IDENTIFIER '(' ids ')'
    {
        $$ = &CreateIndexStmt{table: $4, cols: $6}
    }
|
    CREATE UNIQUE INDEX ON IDENTIFIER '(' ids ')'
    {
        $$ = &CreateIndexStmt{unique: true, table: $5, cols: $7}
    }
|
    ALTER TABLE IDENTIFIER ADD COLUMN colSpec
    {
        $$ = &AddColumnStmt{table: $3, colSpec: $6}
    }

opt_since:
    {
        $$ = 0
    }
|
    SINCE TX NUMBER
    {
        $$ = $3
    }

opt_if_not_exists:
    {
        $$ = false
    }
|
    IF NOT EXISTS
    {
        $$ = true
    }

one_or_more_ids:
    IDENTIFIER
    {
        $$ = []string{$1}
    }
|
    '(' ids ')'
    {
        $$ = $2
    }

dmlstmt:
    INSERT INTO tableRef '(' opt_ids ')' VALUES rows
    {
        $$ = &UpsertIntoStmt{isInsert: true, tableRef: $3, cols: $5, rows: $8}
    }
|
    UPSERT INTO tableRef '(' ids ')' VALUES rows
    {
        $$ = &UpsertIntoStmt{tableRef: $3, cols: $5, rows: $8}
    }

opt_ids:
    {
        $$ = nil
    }
|
    ids
    {
        $$ = $1
    }

rows:
    row
    {
        $$ = []*RowSpec{$1}
    }
|
    rows ',' row
    {
        $$ = append($1, $3)
    }

row:
    '(' opt_values ')'
    {
        $$ = &RowSpec{Values: $2}
    }

ids:
    IDENTIFIER
    {
        $$ = []string{$1}
    }
|
    ids ',' IDENTIFIER
    {
        $$ = append($1, $3)
    }

cols:
    col
    {
        $$ = []*ColSelector{$1}
    }
|
    cols ',' col
    {
        $$ = append($1, $3)
    }

opt_values:
    {
        $$ = nil
    }
|
    values
    {
        $$ = $1
    }

values:
    exp
    {
        $$ = []ValueExp{$1}
    }
|
    values ',' exp
    {
        $$ = append($1, $3)
    }

val: 
    NUMBER
    {
        $$ = &Number{val: int64($1)}
    }
|
    VARCHAR
    {
        $$ = &Varchar{val: $1}
    }
|
    BOOLEAN
    {
        $$ = &Bool{val:$1}
    }
|
    BLOB
    {
        $$ = &Blob{val: $1}
    }
|
    IDENTIFIER '(' ')'
    {
        $$ = &SysFn{fn: $1}
    }
|
    NPARAM IDENTIFIER
    {
        $$ = &Param{id: $2}
    }
|
    PPARAM
    {
        $$ = &Param{id: fmt.Sprintf("param%d", $1), pos: $1}
    }
|
    NULL
    {
        $$ = &NullValue{t: AnyType}
    }

colsSpec:
    colSpec
    {
        $$ = []*ColSpec{$1}
    }
|
    colsSpec ',' colSpec
    {
        $$ = append($1, $3)
    }

colSpec:
    IDENTIFIER TYPE opt_max_len opt_auto_increment opt_not_null
    {
        $$ = &ColSpec{colName: $1, colType: $2, maxLen: int($3), autoIncrement: $4, notNull: $5}
    }

opt_max_len:
    {
        $$ = 0
    }
|
    '[' NUMBER ']'
    {
        $$ = $2
    }

opt_auto_increment:
    {
        $$ = false
    }
|
    AUTO_INCREMENT
    {
        $$ = true
    }

opt_not_null:
    {
        $$ = false
    }
|
    NULL
    {
        $$ = false
    }
|
    NOT NULL
    {
        $$ = true
    }

dqlstmt:
    SELECT opt_distinct opt_selectors FROM ds opt_indexon opt_joins opt_where opt_groupby opt_having opt_orderby opt_limit opt_as
    {
        $$ = &SelectStmt{
                distinct: $2,
                selectors: $3,
                ds: $5,
                indexOn: $6,
                joins: $7,
                where: $8,
                groupBy: $9,
                having: $10,
                orderBy: $11,
                limit: int($12),
                as: $13,
            }
    }

opt_distinct:
    {
        $$ = false
    }
|
    DISTINCT
    {
        $$ = true
    }

opt_selectors:
    '*'
    {
        $$ = nil
    }
|
    selectors
    {
        $$ = $1
    }

selectors:
    selector opt_as
    {
        $1.setAlias($2)
        $$ = []Selector{$1}
    }
|
    selectors ',' selector opt_as
    {
        $3.setAlias($4)
        $$ = append($1, $3)
    }

selector:
    col
    {
        $$ = $1
    }
|
    AGGREGATE_FUNC '(' ')'
    {
        $$ = &AggColSelector{aggFn: $1, col: "*"}
    }
|
    AGGREGATE_FUNC '(' col ')'
    {
        $$ = &AggColSelector{aggFn: $1, db: $3.db, table: $3.table, col: $3.col}
    }

col:
    IDENTIFIER
    {
        $$ = &ColSelector{col: $1}
    }
|
    IDENTIFIER '.' IDENTIFIER
    {
        $$ = &ColSelector{table: $1, col: $3}
    }
|
    IDENTIFIER '.' IDENTIFIER '.' IDENTIFIER
    {
        $$ = &ColSelector{db: $1, table: $3, col: $5}
    }

ds:
    tableRef
    {
        $$ = $1
    }
|
    '(' tableRef opt_as_before opt_as ')'
    {
        $2.asBefore = $3
        $2.as = $4
        $$ = $2
    }
|
    '(' dqlstmt ')'
    {
        $$ = $2.(*SelectStmt)
    }

tableRef:
    IDENTIFIER
    {
        $$ = &tableRef{table: $1}
    }
|
    IDENTIFIER '.' IDENTIFIER
    {
        $$ = &tableRef{db: $1, table: $3}
    }

opt_as_before:
    {
        $$ = 0
    }
|
    BEFORE TX NUMBER
    {
        $$ = $3
    }

opt_joins:
    {
        $$ = nil
    }
|
    joins
    {
        $$ = $1
    }

joins:
    join
    {
        $$ = []*JoinSpec{$1}
    }
|
    join joins
    {
        $$ = append([]*JoinSpec{$1}, $2...)
    }

join:
    JOINTYPE JOIN ds opt_indexon ON exp
    {
        $$ = &JoinSpec{joinType: $1, ds: $3, indexOn: $4, cond: $6}
    }

opt_where:
    {
        $$ = nil
    }
|
    WHERE exp
    {
        $$ = $2
    }

opt_groupby:
    {
        $$ = nil
    }
|
    GROUP BY cols
    {
        $$ = $3
    }

opt_having:
    {
        $$ = nil
    }
|
    HAVING exp
    {
        $$ = $2
    }

opt_limit:
    {
        $$ = 0
    }
|
    LIMIT NUMBER
    {
        $$ = $2
    }

opt_orderby:
    {
        $$ = nil
    }
|
    ORDER BY ordcols
    {
        $$ = $3
    }

opt_indexon:
    {
        $$ = nil
    }
|
    USE INDEX ON one_or_more_ids
    {
        $$ = $4
    }

ordcols:
    col opt_ord
    {
        $$ = []*OrdCol{{sel: $1, order: $2}}
    }
|
    ordcols ',' col opt_ord
    {
        $$ = append($1, &OrdCol{sel: $3, order: $4})
    }

opt_ord:
    {
        $$ = AscOrder
    }
|
    ASC
    {
        $$ = AscOrder
    }
|
    DESC
    {
        $$ = DescOrder
    }

opt_as:
    {
        $$ = ""
    }
|
    AS IDENTIFIER
    {
        $$ = $2
    }

exp:
    selector
    {
        $$ = $1
    }
|
    val
    {
        $$ = $1
    }
|
    binExp
    {
        $$ = $1
    }
|
    NOT exp
    {
        $$ = &NotBoolExp{exp: $2}
    }
|
    '-' exp
    {
        $$ = &NumExp{left: &Number{val: 0}, op: SUBSOP, right: $2}
    }
|
    '(' exp ')'
    {
        $$ = $2
    }
|
    selector LIKE VARCHAR
    {
        $$ = &LikeBoolExp{sel: $1, pattern: $3}
    }
|
    EXISTS '(' dqlstmt ')'
    {
        $$ = &ExistsBoolExp{q: ($3).(*SelectStmt)}
    }
|
    selector opt_not IN '(' dqlstmt ')'
    {
        $$ = &InSubQueryExp{val: $1, notIn: $2, q: $5.(*SelectStmt)}
    }
|
    selector opt_not IN '(' values ')'
    {
        $$ = &InListExp{val: $1, notIn: $2, values: $5}
    }

opt_not:
    {
        $$ = false
    }
|
    NOT
    {
        $$ = true
    }

binExp:
    exp '+' exp
    {
        $$ = &NumExp{left: $1, op: ADDOP, right: $3}
    }
|
    exp '-' exp
    {
        $$ = &NumExp{left: $1, op: SUBSOP, right: $3}
    }
|
    exp '/' exp
    {
        $$ = &NumExp{left: $1, op: DIVOP, right: $3}
    }
|
    exp '*' exp
    {
        $$ = &NumExp{left: $1, op: MULTOP, right: $3}
    }
|
    exp LOP exp
    {
        $$ = &BinBoolExp{left: $1, op: $2, right: $3}
    }
|
    exp CMPOP exp
    {
        $$ = &CmpBoolExp{left: $1, op: $2, right: $3}
    }