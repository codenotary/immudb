/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
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
    datasource DataSource
    colsSpec []*ColSpec
    colSpec *ColSpec
    cols []*ColSelector
    rows []*RowSpec
    row *RowSpec
    values []ValueExp
    value ValueExp
    id string
    integer uint64
    float float64
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
    period period
    openPeriod *openPeriod
    periodInstant periodInstant
    joins []*JoinSpec
    join *JoinSpec
    joinType JoinType
    exp ValueExp
    binExp ValueExp
    err error
    ordcols []*OrdCol
    opt_ord bool
    logicOp LogicOperator
    cmpOp CmpOperator
    pparam int
    update *colUpdate
    updates []*colUpdate
    onConflict *OnConflictDo
    permission Permission
}

%token CREATE DROP USE DATABASE USER WITH PASSWORD READ READWRITE ADMIN SNAPSHOT HISTORY SINCE AFTER BEFORE UNTIL TX OF TIMESTAMP
%token TABLE UNIQUE INDEX ON ALTER ADD RENAME TO COLUMN PRIMARY KEY
%token BEGIN TRANSACTION COMMIT ROLLBACK
%token INSERT UPSERT INTO VALUES DELETE UPDATE SET CONFLICT DO NOTHING
%token SELECT DISTINCT FROM JOIN HAVING WHERE GROUP BY LIMIT OFFSET ORDER ASC DESC AS UNION ALL
%token NOT LIKE IF EXISTS IN IS
%token AUTO_INCREMENT NULL CAST SCAST
%token SHOW DATABASES TABLES USERS
%token <id> NPARAM
%token <pparam> PPARAM
%token <joinType> JOINTYPE
%token <logicOp> LOP
%token <cmpOp> CMPOP
%token <id> IDENTIFIER
%token <sqlType> TYPE
%token <integer> INTEGER
%token <float> FLOAT
%token <str> VARCHAR
%token <boolean> BOOLEAN
%token <blob> BLOB
%token <aggFn> AGGREGATE_FUNC
%token <err> ERROR
%token <dot> DOT

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
%left IS

%type <stmts> sql sqlstmts
%type <stmt> sqlstmt ddlstmt dmlstmt dqlstmt select_stmt
%type <colsSpec> colsSpec
%type <colSpec> colSpec
%type <ids> ids one_or_more_ids opt_ids
%type <cols> cols
%type <rows> rows
%type <row> row
%type <values> values opt_values
%type <value> val fnCall
%type <sel> selector
%type <sels> opt_selectors selectors
%type <col> col
%type <distinct> opt_distinct opt_all
%type <ds> ds
%type <tableRef> tableRef
%type <period> opt_period
%type <openPeriod> opt_period_start
%type <openPeriod> opt_period_end
%type <periodInstant> period_instant
%type <joins> opt_joins joins
%type <join> join
%type <joinType> opt_join_type
%type <exp> exp opt_where opt_having boundexp
%type <binExp> binExp
%type <cols> opt_groupby
%type <exp> opt_limit opt_offset
%type <integer> opt_max_len
%type <id> opt_as
%type <ordcols> ordcols opt_orderby
%type <opt_ord> opt_ord
%type <ids> opt_indexon
%type <boolean> opt_if_not_exists opt_auto_increment opt_not_null opt_not
%type <update> update
%type <updates> updates
%type <onConflict> opt_on_conflict
%type <permission> permission

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
    sqlstmt STMT_SEPARATOR sqlstmts
    {
        $$ = append([]SQLStmt{$1}, $3...)
    }

opt_separator: {} | STMT_SEPARATOR

sqlstmt: ddlstmt | dmlstmt | dqlstmt

ddlstmt:
    BEGIN TRANSACTION
    {
        $$ = &BeginTransactionStmt{}
    }
|
    BEGIN
    {
        $$ = &BeginTransactionStmt{}
    }
|
    COMMIT
    {
        $$ = &CommitStmt{}
    }
|
    ROLLBACK
    {
        $$ = &RollbackStmt{}
    }
|
    CREATE DATABASE opt_if_not_exists IDENTIFIER
    {
        $$ = &CreateDatabaseStmt{ifNotExists: $3, DB: $4}
    }
|
    USE IDENTIFIER
    {
        $$ = &UseDatabaseStmt{DB: $2}
    }
|
    USE DATABASE IDENTIFIER
    {
        $$ = &UseDatabaseStmt{DB: $3}
    }
|
    USE SNAPSHOT opt_period
    {
        $$ = &UseSnapshotStmt{period: $3}
    }
|
    CREATE TABLE opt_if_not_exists IDENTIFIER '(' colsSpec ',' PRIMARY KEY one_or_more_ids ')'
    {
        $$ = &CreateTableStmt{ifNotExists: $3, table: $4, colsSpec: $6, pkColNames: $10}
    }
|
    DROP TABLE IDENTIFIER
    {
        $$ = &DropTableStmt{table: $3}
    }
|
    CREATE INDEX opt_if_not_exists ON IDENTIFIER '(' ids ')'
    {
        $$ = &CreateIndexStmt{ifNotExists: $3, table: $5, cols: $7}
    }
|
    CREATE UNIQUE INDEX opt_if_not_exists ON IDENTIFIER '(' ids ')'
    {
        $$ = &CreateIndexStmt{unique: true, ifNotExists: $4, table: $6, cols: $8}
    }
|
    DROP INDEX ON IDENTIFIER '(' ids ')'
    {
        $$ = &DropIndexStmt{table: $4, cols: $6}
    }
|
    DROP INDEX IDENTIFIER DOT IDENTIFIER
    {
        $$ = &DropIndexStmt{table: $3, cols: []string{$5}}
    }
|
    ALTER TABLE IDENTIFIER ADD COLUMN colSpec
    {
        $$ = &AddColumnStmt{table: $3, colSpec: $6}
    }
|
    ALTER TABLE IDENTIFIER RENAME TO IDENTIFIER
    {
        $$ = &RenameTableStmt{oldName: $3, newName: $6}
    }
|
    ALTER TABLE IDENTIFIER RENAME COLUMN IDENTIFIER TO IDENTIFIER
    {
        $$ = &RenameColumnStmt{table: $3, oldName: $6, newName: $8}
    }
|
    ALTER TABLE IDENTIFIER DROP COLUMN IDENTIFIER
    {
        $$ = &DropColumnStmt{table: $3, colName: $6}
    }
|
    CREATE USER IDENTIFIER WITH PASSWORD VARCHAR permission
    {
        $$ = &CreateUserStmt{username: $3, password: $6, permission: $7}
    }
|
    ALTER USER IDENTIFIER WITH PASSWORD VARCHAR permission
    {
        $$ = &AlterUserStmt{username: $3, password: $6, permission: $7}
    }
|
    DROP USER IDENTIFIER
    {
        $$ = &DropUserStmt{username: $3}
    }

permission:
    {
        $$ = PermissionReadWrite
    }
|
    READ
    {
        $$ = PermissionReadOnly
    }
|
    READWRITE
    {
        $$ = PermissionReadWrite
    }
|
    ADMIN
    {
        $$ = PermissionAdmin
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
    INSERT INTO tableRef '(' opt_ids ')' VALUES rows opt_on_conflict
    {
        $$ = &UpsertIntoStmt{isInsert: true, tableRef: $3, cols: $5, rows: $8, onConflict: $9}
    }
|
    UPSERT INTO tableRef '(' ids ')' VALUES rows
    {
        $$ = &UpsertIntoStmt{tableRef: $3, cols: $5, rows: $8}
    }
|
    DELETE FROM tableRef opt_where opt_indexon opt_limit opt_offset
    {
        $$ = &DeleteFromStmt{tableRef: $3, where: $4, indexOn: $5, limit: $6, offset: $7}
    }
|
    UPDATE tableRef SET updates opt_where opt_indexon opt_limit opt_offset
    {
        $$ = &UpdateStmt{tableRef: $2, updates: $4, where: $5, indexOn: $6, limit: $7, offset: $8}
    }

opt_on_conflict:
    {
        $$ = nil
    }
|
    ON CONFLICT DO NOTHING
    {
        $$ = &OnConflictDo{}
    }

updates:
    update
    {
        $$ = []*colUpdate{$1}
    }
|
    updates  ',' update
    {
        $$ = append($1, $3)
    }

update:
    IDENTIFIER CMPOP exp
    {
        $$ = &colUpdate{col: $1, op: $2, val: $3}
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
    INTEGER
    {
        $$ = &Integer{val: int64($1)}
    }
|
    FLOAT
    {
        $$ = &Float64{val: float64($1)}
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
    CAST '(' exp AS TYPE ')'
    {
        $$ = &Cast{val: $3, t: $5}
    }
|
    fnCall
    {
        $$ = $1
    }
|
    NPARAM
    {
        $$ = &Param{id: $1}
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

fnCall:
    IDENTIFIER '(' opt_values ')'
    {
        $$ = &FnCall{fn: $1, params: $3}
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
    IDENTIFIER TYPE opt_max_len opt_not_null opt_auto_increment
    {
        $$ = &ColSpec{colName: $1, colType: $2, maxLen: int($3), notNull: $4, autoIncrement: $5}
    }

opt_max_len:
    {
        $$ = 0
    }
|
    '[' INTEGER ']'
    {
        $$ = $2
    }
|
    '(' INTEGER ')'
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
    select_stmt
    {
        $$ = $1
    }
|
    select_stmt UNION opt_all dqlstmt
    {
        $$ = &UnionStmt{
            distinct: $3,
            left: $1.(DataSource),
            right: $4.(DataSource),
        }
    }
|
    SHOW DATABASES
    {
        $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "databases"}},
        }
    }
|
    SHOW TABLES
    {
        $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "tables"}},
        }
    }
|
    SHOW TABLE IDENTIFIER
    {
        $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "table", params: []ValueExp{&Varchar{val: $3}}}},
        }
    }
|
    SHOW USERS
    {
        $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "users"}},
        }
    }

select_stmt: SELECT opt_distinct opt_selectors FROM ds opt_indexon opt_joins opt_where opt_groupby opt_having opt_orderby opt_limit opt_offset
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
                limit: $12,
                offset: $13,
            }
    }

opt_all:
    {
        $$ = true
    }
|
    ALL
    {
        $$ = false
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
    AGGREGATE_FUNC '(' '*' ')'
    {
        $$ = &AggColSelector{aggFn: $1, col: "*"}
    }
|
    AGGREGATE_FUNC '(' col ')'
    {
        $$ = &AggColSelector{aggFn: $1, table: $3.table, col: $3.col}
    }

col:
    IDENTIFIER
    {
        $$ = &ColSelector{col: $1}
    }
|
    IDENTIFIER DOT IDENTIFIER
    {
        $$ = &ColSelector{table: $1, col: $3}
    }

ds:
    tableRef opt_period opt_as
    {
        $1.period = $2
        $1.as = $3
        $$ = $1
    }
|
    '(' dqlstmt ')' opt_as
    {
        $2.(*SelectStmt).as = $4
        $$ = $2.(DataSource)
    }
|
    DATABASES '(' ')' opt_as
    {
        $$ = &FnDataSourceStmt{fnCall: &FnCall{fn: "databases"}, as: $4}
    }
|
    TABLES '(' ')' opt_as
    {
        $$ = &FnDataSourceStmt{fnCall:  &FnCall{fn: "tables"}, as: $4}
    }
|
    TABLE '(' IDENTIFIER ')'
    {
        $$ = &FnDataSourceStmt{fnCall:  &FnCall{fn: "table", params: []ValueExp{&Varchar{val: $3}}}}
    }
|
    USERS '(' ')' opt_as
    {
        $$ = &FnDataSourceStmt{fnCall:  &FnCall{fn: "users"}, as: $4}
    }
|
    fnCall opt_as
    {
        $$ = &FnDataSourceStmt{fnCall: $1.(*FnCall), as: $2}
    }
|
    '(' HISTORY OF IDENTIFIER ')' opt_as
    {
        $$ = &tableRef{table: $4, history: true, as: $6}
    }

tableRef:
    IDENTIFIER
    {
        $$ = &tableRef{table: $1}
    }

opt_period:
    opt_period_start opt_period_end
    {
        $$ = period{start: $1, end: $2}
    }

opt_period_start:
    {
        $$ = nil
    }
|
    SINCE period_instant
    {
        $$ = &openPeriod{inclusive: true, instant: $2}
    }
|
    AFTER period_instant
    {
        $$ = &openPeriod{instant: $2}
    }

opt_period_end:
    {
        $$ = nil
    }
|
    UNTIL period_instant
    {
        $$ = &openPeriod{inclusive: true, instant: $2}
    }
|
    BEFORE period_instant
    {
        $$ = &openPeriod{instant: $2}
    }

period_instant:
    TX exp
    {
        $$ = periodInstant{instantType: txInstant, exp: $2}
    }
|
    exp
    {
        $$ = periodInstant{instantType: timeInstant, exp: $1}
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
    opt_join_type JOIN ds opt_indexon ON exp
    {
        $$ = &JoinSpec{joinType: $1, ds: $3, indexOn: $4, cond: $6}
    }

opt_join_type:
    {
        $$ = InnerJoin
    }
|
    JOINTYPE
    {
        $$ = $1
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
        $$ = nil
    }
|
    LIMIT exp
    {
        $$ = $2
    }

opt_offset:
    {
        $$ = nil
    }
|
    OFFSET exp
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
    selector opt_ord
    {
        $$ = []*OrdCol{{sel: $1, descOrder: $2}}
    }
|
    ordcols ',' selector opt_ord
    {
        $$ = append($1, &OrdCol{sel: $3, descOrder: $4})
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
    IDENTIFIER
    {
        $$ = $1
    }
|
    AS IDENTIFIER
    {
        $$ = $2
    }

exp:
    boundexp
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
        $$ = &NumExp{left: &Integer{val: 0}, op: SUBSOP, right: $2}
    }
|
    boundexp opt_not LIKE exp
    {
        $$ = &LikeBoolExp{val: $1, notLike: $2, pattern: $4}
    }
|
    EXISTS '(' dqlstmt ')'
    {
        $$ = &ExistsBoolExp{q: ($3).(DataSource)}
    }
|
    boundexp opt_not IN '(' dqlstmt ')'
    {
        $$ = &InSubQueryExp{val: $1, notIn: $2, q: $5.(*SelectStmt)}
    }
|
    boundexp opt_not IN '(' values ')'
    {
        $$ = &InListExp{val: $1, notIn: $2, values: $5}
    }

boundexp:
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
    '(' exp ')'
    {
        $$ = $2
    }
|
    boundexp SCAST TYPE
    {
        $$ = &Cast{val: $1, t: $3}
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
|
    exp IS NULL
    {
        $$ = &CmpBoolExp{left: $1, op: EQ, right: &NullValue{t: AnyType}}
    }
|
    exp IS NOT NULL
    {
        $$ = &CmpBoolExp{left: $1, op: NE, right: &NullValue{t: AnyType}}
    }
