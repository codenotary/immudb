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
    rows []*Row
    row *Row
    values []Value
    value Value
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
    tableRef *TableRef
    join *JoinSpec
    joinType JoinType
    boolExp BoolExp
    err error
    ordcols []*OrdCol
    opt_ord bool
    logicOp LogicOperator
    cmpOp CmpOperator
}

%token CREATE USE DATABASE SNAPSHOT SINCE UP TO TABLE INDEX ON ALTER ADD COLUMN
%token BEGIN TRANSACTION COMMIT
%token INSERT INTO VALUES
%token SELECT DISTINCT FROM JOIN HAVING WHERE GROUP BY OFFSET LIMIT ORDER ASC DESC AS
%token NOT LIKE EXISTS
%token <joinType> JOINTYPE
%token <logicOp> LOP
%token <cmpOp> CMPOP
%token <id> IDENTIFIER
%token <sqlType> TYPE
%token <number> NUMBER
%token <str> STRING
%token <boolean> BOOLEAN
%token <blob> BLOB
%token <aggFn> AGGREGATE_FUNC
%token <err> ERROR

%left  ','
%left  '.'
%left  LOP
%right NOT
%left  CMPOP
%right LIKE
%right AS
%right STMT_SEPARATOR

%type <stmts> sql
%type <stmts> sqlstmts dstmts
%type <stmt> sqlstmt dstmt ddlstmt dmlstmt dqlstmt
%type <colsSpec> colsSpec colSpecList
%type <colSpec> colSpec
%type <ids> ids
%type <cols> cols
%type <rows> rows
%type <row> row
%type <values> values
%type <value> val
%type <sel> selector
%type <sels> selectors
%type <col> col
%type <distinct> opt_distinct
%type <ds> ds
%type <tableRef> tableRef
%type <join> opt_join
%type <boolExp> boolExp opt_where opt_having
%type <cols> opt_groupby
%type <number> opt_offset opt_limit
%type <id> opt_as
%type <ordcols> ordcols opt_orderby
%type <opt_ord> opt_ord

%start sql
    
%%

sql: opt_separator sqlstmts
{
    $$ = $2
    setResult(yylex, $2)
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
    sqlstmt STMT_SEPARATOR opt_separator sqlstmts
    {
        $$ = append([]SQLStmt{$1}, $4...)
    }

opt_separator: {} | STMT_SEPARATOR opt_separator

sqlstmt:
    dstmt
|
    BEGIN TRANSACTION opt_separator dstmts COMMIT
    {
        $$ = &TxStmt{stmts: $4}
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
        $$ = &CreateDatabaseStmt{db: $3}
    }
|
    USE DATABASE IDENTIFIER
    {
        $$ = &UseDatabaseStmt{db: $3}
    }
|
    USE SNAPSHOT SINCE STRING
    {
        $$ = &UseSnapshotStmt{since: $4}
    }
|
    USE SNAPSHOT UP TO STRING
    {
        $$ = &UseSnapshotStmt{upTo: $5}
    }
|
    USE SNAPSHOT SINCE STRING UP TO STRING
    {
        $$ = &UseSnapshotStmt{since: $4, upTo: $7}
    }
|
    CREATE TABLE IDENTIFIER colsSpec
    {
        $$ = &CreateTableStmt{table: $3, colsSpec: $4}
    }
|
    CREATE INDEX ON IDENTIFIER '(' IDENTIFIER ')'
    {
        $$ = &CreateIndexStmt{table: $4, col: $6}
    }
|
    ALTER TABLE IDENTIFIER ADD COLUMN colSpec
    {
        $$ = &AddColumnStmt{table: $3, colSpec: $6}
    }
|
    ALTER TABLE IDENTIFIER ALTER COLUMN colSpec
    {
        $$ = &AlterColumnStmt{table: $3, colSpec: $6}
    }

dmlstmt:
    INSERT INTO IDENTIFIER '(' ids ')' VALUES rows
    {
        $$ = &InsertIntoStmt{table: $3, cols: $5, rows: $8}
    }

rows:
    row
    {
        $$ = []*Row{$1}
    }
|
    rows ',' row
    {
        $$ = append($1, $3)
    }

row:
    '(' values ')'
    {
        $$ = &Row{values: $2}
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

values:
    val
    {
        $$ = []Value{$1}
    }
|
    values ',' val
    {
        $$ = append($1, $3)
    }

val: 
    NUMBER
    {
        $$ = $1
    }
|
    STRING
    {
        $$ = $1
    }
|
    BOOLEAN
    {
        $$ = $1
    }
|
    BLOB
    {
        $$ = $1
    }
|
    IDENTIFIER '(' ')'
    {
        $$ = &SysFn{fn: $1}
    }
|
    '@' IDENTIFIER
    {
        $$ = &Param{id: $2}
    }

colsSpec:
    {
        $$ = nil
    }
|
    '(' ')'
    {
        $$ = nil
    }
|
    '(' colSpecList ')'
    {
        $$ = $2
    }

colSpecList:
    colSpec
    {
        $$ = []*ColSpec{$1}
    }
|
    colSpecList ',' colSpec
    {
        $$ = append($1, $3)
    }

colSpec:
    IDENTIFIER TYPE
    {
        $$ = &ColSpec{colName: $1, colType: $2}
    }

dqlstmt:
    SELECT opt_distinct selectors FROM ds opt_join opt_where opt_groupby opt_having opt_offset opt_limit opt_orderby opt_as
    {
        $$ = &SelectStmt{
                distinct: $2,
                selectors: $3,
                ds: $5,
                join: $6,
                where: $7,
                groupBy: $8,
                having: $9,
                offset: $10,
                limit: $11,
                orderBy: $12,
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

selectors:
    selector
    {
        $$ = []Selector{$1}
    }
|
    selectors ',' selector
    {
        $$ = append($1, $3)
    }

selector:
    col opt_as
    {
        $1.as = $2
        $$ = $1
    }
|
    AGGREGATE_FUNC '(' '*' ')' opt_as
    {
        $$ = &AggSelector{aggFn: $1, as: $5}
    }
|
    AGGREGATE_FUNC '(' col ')' opt_as
    {
        $$ = &AggColSelector{aggFn: $1, db: $3.db, table: $3.table, col: $3.col, as: $5}
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
    '(' tableRef opt_as ')'
    {
        $2.as = $3
        $$ = $2
    }
|
    '(' dqlstmt ')'
    {
        $$ = $2
    }

tableRef:
    IDENTIFIER
    {
        $$ = &TableRef{table: $1}
    }
|
    IDENTIFIER '.' IDENTIFIER
    {
        $$ = &TableRef{db: $1, table: $3}
    }

opt_join:
    {
        $$ = nil
    }
|
    JOINTYPE JOIN ds ON boolExp
    {
        $$ = &JoinSpec{joinType: $1, ds: $3, cond: $5}
    }

opt_where:
    {
        $$ = nil
    }
|
    WHERE boolExp
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
    HAVING boolExp
    {
        $$ = $2
    }

opt_offset:
    {
        $$ = 0
    }
|
    OFFSET NUMBER
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

ordcols:
    col opt_ord
    {
        $$ = []*OrdCol{{col: $1, desc: $2}}
    }
|
    ordcols ',' col opt_ord
    {
        $$ = append($1, &OrdCol{col: $3, desc: $4})
    }

opt_ord:
    {
        $$ = false
    }
|
    ASC
    {
        $$ = false
    }
|
    DESC
    {
        $$ = true
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

boolExp:
    col
    {
        $$ = $1
    }
|
    val
    {
        $$ = $1
    }
|
    NOT boolExp
    {
        $$ = &NotBoolExp{exp: $2}
    }
|
    '(' boolExp ')'
    {
        $$ = $2
    }
|
    col LIKE STRING
    {
        $$ = &LikeBoolExp{col: $1, pattern: $3}
    }
|
    boolExp LOP boolExp
    {
        $$ = &BinBoolExp{op: $2, left: $1, right: $3}
    }
|
    boolExp CMPOP boolExp
    {
        $$ = &CmpBoolExp{op: $2, left: $1, right: $3}
    }
|
    EXISTS '(' dqlstmt ')'
    {
        $$ = &ExistsBoolExp{q: ($3).(*SelectStmt)}
    }