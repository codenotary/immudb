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
    cols []string
    values []Value
    value Value
    id string
    number uint64
    str string
    boolean bool
    blob []byte
    sqlType SQLValueType
    aggFn AggregateFn
    sel Selector
    sels []Selector
    distinct bool
    ds DataSource
    join *InnerJoinSpec
    boolExp BoolExp
    err error
    ordcols []*OrdCol
    opt_ord bool
}

%token CREATE USE DATABASE TABLE INDEX ON ALTER ADD COLUMN
%token BEGIN END
%token INSERT INTO VALUES
%token SELECT DISTINCT FROM INNER JOIN HAVING WHERE GROUP BY OFFSET LIMIT ORDER ASC DESC AS
%token <id> IDENTIFIER
%token <sqlType> TYPE
%token <number> NUMBER
%token <str> STRING
%token <boolean> BOOLEAN
%token <blob> BLOB
%token <aggFn> AGGREGATE_FUNC
%token <err> ERROR

%left ','
%right STMT_SEPARATOR

%type <stmts> sql
%type <stmts> sqlstmts dstmts
%type <stmt> sqlstmt dstmt ddlstmt dmlstmt dqlstmt
%type <colsSpec> colsSpec colSpecList
%type <colSpec> colSpec
%type <cols> cols
%type <values> values
%type <value> val
%type <sel> selector
%type <sels> selectors
%type <distinct> opt_distinct
%type <ds> ds
%type <join> opt_join
%type <boolExp> boolExp opt_where opt_having
%type <cols> opt_groupby
%type <number> opt_offset opt_limit
%type <id> opt_as
%type <ordcols> ordcols opt_orderby
%type <opt_ord> opt_ord

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
|
    BEGIN opt_separator dstmts END
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
        $$ = &CreateDatabaseStmt{db: $3}
    }
|
    USE DATABASE IDENTIFIER
    {
        $$ = &UseDatabaseStmt{db: $3}
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
    INSERT INTO IDENTIFIER '(' cols ')' VALUES '(' values ')'
    {
        $$ = &InsertIntoStmt{table: $3, cols: $5, values: $9}
    }

cols:
    IDENTIFIER
    {
        $$ = []string{$1}
    }
|
    cols ',' IDENTIFIER
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
    IDENTIFIER opt_as
    {
        $$ = &ColSelector{col: $1, as: $2}
    }
|
    IDENTIFIER '.' IDENTIFIER opt_as
    {
        $$ = &ColSelector{ds: $1, col: $3, as: $4}
    }
|
    AGGREGATE_FUNC '(' '*' ')' opt_as
    {
        $$ = &AggSelector{aggFn: $1, as: $5}
    }
|
    AGGREGATE_FUNC '(' IDENTIFIER '.' IDENTIFIER ')' opt_as
    {
        $$ = &AggColSelector{aggFn: $1, ds: $3, col: $5, as: $7}
    }

ds:
    IDENTIFIER
    {
        $$ = &TableRef{table: $1}
    }
|
    '(' dqlstmt ')'
    {
        $$ = $2
    }

opt_join:
    {
    }
|
    INNER JOIN ds ON boolExp
    {
        $$ = &InnerJoinSpec{ds: $3, cond: $5}
    }

opt_where:
    {
    }
|
    WHERE boolExp
    {
        $$ = $2
    }

opt_groupby:
    {
    }
|
    GROUP BY cols
    {
        $$ = $3
    }

opt_having:
    {
    }
|
    HAVING boolExp
    {
        $$ = $2
    }

opt_offset:
    {
    }
|
    OFFSET NUMBER
    {
        $$ = $2
    }

opt_limit:
    {
    }
|
    LIMIT NUMBER
    {
        $$ = $2
    }

opt_orderby:
    {
    }
|
    ORDER BY ordcols
    {
        $$ = $3
    }

ordcols:
    IDENTIFIER opt_ord
    {
        $$ = []*OrdCol{{col: $1, desc: $2}}
    }
|
    ordcols ',' IDENTIFIER opt_ord
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
    IDENTIFIER '=' IDENTIFIER
    {
        $$ = &EqualBoolExp{left: $1, right: $3}
    }