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
    keyword string
    sqlType SQLValueType
    aggFn AggregateFn
    colNames []string
    col *ColSelector
    sel Selector
    targets []TargetEntry
    jsonFields []string
    distinct bool
    ds DataSource
    tableRef *tableRef
    period period
    openPeriod *openPeriod
    periodInstant periodInstant
    joins []*JoinSpec
    join *JoinSpec
    joinType JoinType
    check CheckConstraint
    exp ValueExp
    binExp ValueExp
    err error
    ordexps []*OrdExp
    opt_ord bool
    logicOp LogicOperator
    cmpOp CmpOperator
    pparam int
    update *colUpdate
    updates []*colUpdate
    onConflict *OnConflictDo
    permission Permission
    sqlPrivilege SQLPrivilege
    sqlPrivileges []SQLPrivilege
    whenThenClauses []whenThenClause
    tableElem TableElem
    tableElems []TableElem
    timestampField TimestampFieldType
}

%token <keyword> CREATE DROP USE DATABASE USER WITH PASSWORD READ READWRITE ADMIN SNAPSHOT HISTORY SINCE AFTER BEFORE UNTIL TX OF
%token <keyword> INTEGER_TYPE BOOLEAN_TYPE VARCHAR_TYPE UUID_TYPE BLOB_TYPE TIMESTAMP_TYPE FLOAT_TYPE JSON_TYPE
%token <keyword> TABLE UNIQUE INDEX ON ALTER ADD RENAME TO COLUMN CONSTRAINT PRIMARY KEY CHECK GRANT REVOKE GRANTS FOR PRIVILEGES
%token <keyword> BEGIN TRANSACTION COMMIT ROLLBACK
%token <keyword> INSERT UPSERT INTO VALUES DELETE UPDATE SET CONFLICT DO NOTHING RETURNING
%token <keyword> SELECT DISTINCT FROM JOIN HAVING WHERE GROUP BY LIMIT OFFSET ORDER ASC DESC AS UNION ALL CASE WHEN THEN ELSE END
%token <keyword> NOT LIKE IF EXISTS IN IS
%token <keyword> AUTO_INCREMENT NULL CAST SCAST
%token <keyword> SHOW DATABASES TABLES USERS
%token <keyword> BETWEEN
%token <keyword> EXTRACT YEAR MONTH DAY HOUR MINUTE SECOND

%token <id> NPARAM
%token <pparam> PPARAM
%token <joinType> JOINTYPE
%token <logicOp> AND OR
%token <cmpOp> CMPOP
%token NOT_MATCHES_OP
%token <id> IDENTIFIER
%token <integer> INTEGER_LIT
%token <float> FLOAT_LIT
%token <str> VARCHAR_LIT
%token <boolean> BOOLEAN_LIT
%token <blob> BLOB_LIT
%token <aggFn> AGGREGATE_FUNC
%token <err> ERROR
%token <dot> DOT
%token <arrow> ARROW

%left  ','
%right AS

%nonassoc BETWEEN

%left OR
%left AND

%right NOT

%nonassoc CMPOP LIKE NOT_MATCHES_OP IS

%left '+' '-'
%left '*' '/' '%'
%left '.'

%right STMT_SEPARATOR

%type <stmts> sql sqlstmts
%type <stmt> sqlstmt ddlstmt dmlstmt dqlstmt select_stmt
%type <colSpec> colSpec
%type <colNames> col_names insert_cols one_or_more_col_names
%type <cols> cols
%type <rows> rows
%type <row> row
%type <values> values opt_values
%type <value> val fnCall
%type <sel> selector
%type <jsonFields> jsonFields
%type <col> col
%type <distinct> opt_distinct opt_all
%type <ds> ds values_or_query
%type <tableRef> tableRef
%type <period> opt_period
%type <openPeriod> opt_period_start
%type <openPeriod> opt_period_end
%type <periodInstant> period_instant
%type <joins> opt_joins joins
%type <join> join
%type <joinType> opt_join_type
%type <check> check
%type <tableElem> tableElem
%type <tableElems> tableElems
%type <exp> exp opt_exp opt_where opt_having boundexp opt_else orExp andExp cmpExp primaryBool addExp notExp
mulExp unaryExp primary
%type <cols> opt_groupby
%type <exp> opt_limit opt_offset case_when_exp
%type <targets> opt_targets targets
%type <integer> opt_max_len
%type <id> opt_as
%type <ordexps> ordexps opt_orderby
%type <opt_ord> opt_ord
%type <colNames> opt_indexon
%type <boolean> opt_if_not_exists opt_auto_increment opt_not_null opt_not opt_primary_key
%type <update> update
%type <updates> updates
%type <onConflict> opt_on_conflict
%type <permission> permission
%type <sqlPrivilege> sqlPrivilege
%type <sqlPrivileges> sqlPrivileges
%type <whenThenClauses> when_then_clauses
%type <timestampField> timestamp_field
%type <sqlType> sql_type
%type <keyword> unreserved_keyword colNameKeyword
%type <str> qualifiedName tableName col_name

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
    CREATE DATABASE IF NOT EXISTS IDENTIFIER
    {
        $$ = &CreateDatabaseStmt{ifNotExists: true, DB: $6}
    }
|
    CREATE DATABASE IDENTIFIER
    {
        $$ = &CreateDatabaseStmt{ifNotExists: false, DB: $3}
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
    CREATE TABLE IF NOT EXISTS tableName '(' tableElems ')'
    {
        $$ = newCreateTableStmt($6, $8, true)
    }
|
    CREATE TABLE tableName '(' tableElems ')'
    {
       $$ = newCreateTableStmt($3, $5, false)
    }
|
    DROP TABLE qualifiedName
    {
        $$ = &DropTableStmt{table: $3}
    }
|
    CREATE INDEX opt_if_not_exists ON tableName '(' col_names ')'
    {
        $$ = &CreateIndexStmt{ifNotExists: $3, table: $5, cols: $7}
    }
|
    CREATE UNIQUE INDEX opt_if_not_exists ON tableName '(' col_names ')'
    {
        $$ = &CreateIndexStmt{unique: true, ifNotExists: $4, table: $6, cols: $8}
    }
|
    DROP INDEX ON tableName '(' col_names ')'
    {
        $$ = &DropIndexStmt{table: $4, cols: $6}
    }
|
    DROP INDEX tableName DOT col_name
    {
        $$ = &DropIndexStmt{table: $3, cols: []string{$5}}
    }
|
    ALTER TABLE tableName ADD COLUMN colSpec
    {
        $$ = &AddColumnStmt{table: $3, colSpec: $6}
    }
|
    ALTER TABLE tableName RENAME TO tableName
    {
        $$ = &RenameTableStmt{oldName: $3, newName: $6}
    }
|
    ALTER TABLE tableName RENAME COLUMN col_name TO col_name
    {
        $$ = &RenameColumnStmt{table: $3, oldName: $6, newName: $8}
    }
|
    ALTER TABLE tableName DROP COLUMN col_name
    {
        $$ = &DropColumnStmt{table: $3, colName: $6}
    }
|
    ALTER TABLE tableName DROP CONSTRAINT IDENTIFIER
    {
        $$ = &DropConstraintStmt{table: $3, constraintName: $6}
    }
|
    CREATE USER IDENTIFIER WITH PASSWORD VARCHAR_LIT permission
    {
        $$ = &CreateUserStmt{username: $3, password: $6, permission: $7}
    }
|
    ALTER USER IDENTIFIER WITH PASSWORD VARCHAR_LIT permission
    {
        $$ = &AlterUserStmt{username: $3, password: $6, permission: $7}
    }
|
    DROP USER IDENTIFIER
    {
        $$ = &DropUserStmt{username: $3}
    }
|
    GRANT sqlPrivileges ON DATABASE qualifiedName TO USER IDENTIFIER
    {
        $$ = &AlterPrivilegesStmt{database: $5, user: $8, privileges: $2, isGrant: true}
    }
|
    REVOKE sqlPrivileges ON DATABASE qualifiedName TO USER IDENTIFIER
    {
        $$ = &AlterPrivilegesStmt{database: $5, user: $8, privileges: $2}
    }
;

sqlPrivileges:
    ALL PRIVILEGES
    {
        $$ = allPrivileges
    }
|
    sqlPrivilege
    {
        $$ = []SQLPrivilege{$1}
    }
|
    sqlPrivilege ',' sqlPrivileges
    {
        $$ = append($3, $1)
    }

sqlPrivilege:
    SELECT
    {
        $$ = SQLPrivilegeSelect
    }
|
    CREATE
    {
        $$ = SQLPrivilegeCreate
    }
|
    INSERT
    {
        $$ = SQLPrivilegeInsert
    }
|
    UPDATE
    {
        $$ = SQLPrivilegeUpdate
    }
|
    DELETE
    {
        $$ = SQLPrivilegeDelete
    }
|
    DROP
    {
        $$ = SQLPrivilegeDrop
    }
|
    ALTER
    {
        $$ = SQLPrivilegeAlter
    }
;

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
;

opt_if_not_exists:
    {
        $$ = false
    }
|
    IF NOT EXISTS
    {
        $$ = true
    }
;

dmlstmt:
    INSERT INTO tableRef insert_cols values_or_query opt_on_conflict
    {
        $$ = &UpsertIntoStmt{isInsert: true, tableRef: $3, cols: $4, ds: $5, onConflict: $6}
    }
|
    UPSERT INTO tableRef insert_cols values_or_query
    {
        $$ = &UpsertIntoStmt{tableRef: $3, cols: $4, ds: $5}
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

values_or_query:
	VALUES rows
	{
		$$ = &valuesDataSource{rows: $2}
	}
|
	dqlstmt
	{
		$$ = $1.(DataSource)
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
    INTEGER_LIT
    {
        $$ = &Integer{val: int64($1)}
    }
|
    FLOAT_LIT
    {
        $$ = &Float64{val: float64($1)}
    }
|
    VARCHAR_LIT
    {
        $$ = &Varchar{val: $1}
    }
|
    BOOLEAN_LIT
    {
        $$ = &Bool{val:$1}
    }
|
    BLOB_LIT
    {
        $$ = &Blob{val: $1}
    }
|
    CAST '(' exp AS sql_type ')'
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
;

sql_type:
    INTEGER_TYPE { $$ = IntegerType }
    | BOOLEAN_TYPE { $$ = BooleanType }
    | VARCHAR_TYPE { $$ = VarcharType }
    | UUID_TYPE { $$ = UUIDType }
    | BLOB_TYPE  { $$ = BLOBType }
    | TIMESTAMP_TYPE { $$ = TimestampType }
    | FLOAT_TYPE { $$ = Float64Type }
    | JSON_TYPE { $$ = JSONType }
;

fnCall:
    IDENTIFIER '(' opt_values ')'
    {
        $$ = &FnCall{fn: $1, params: $3}
    }

tableElems:
    tableElem
    {
        $$ = []TableElem{$1}
    }
|
    tableElems ',' tableElem
    {
        $$ = append($1, $3)
    }

tableElem:
   colSpec
   {
        $$ = $1
   }
|
    check
    {
        $$ = $1
    }
|
    PRIMARY KEY one_or_more_col_names
    {
        $$ = PrimaryKeyConstraint($3)
    }
;

colSpec:
    col_name sql_type opt_max_len opt_not_null opt_auto_increment opt_primary_key
    {
        $$ = &ColSpec{
            colName: $1, 
            colType: $2, 
            maxLen: int($3), 
            notNull: $4 || $6, 
            autoIncrement: $5,
            primaryKey: $6,
        }
    }
;

opt_primary_key:
    {
        $$ = false
    }
|
    PRIMARY KEY
    {
        $$ = true
    }
;

opt_max_len:
    {
        $$ = 0
    }
|
    '[' INTEGER_LIT ']'
    {
        $$ = $2
    }
|
    '(' INTEGER_LIT ')'
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
|
    SHOW GRANTS
    {
         $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "grants"}},
        }
    }
|
    SHOW GRANTS FOR IDENTIFIER
    {
         $$ = &SelectStmt{
            ds: &FnDataSourceStmt{fnCall: &FnCall{fn: "grants", params: []ValueExp{&Varchar{val: $4}}}},
        }
    }

select_stmt: SELECT opt_distinct opt_targets FROM ds opt_indexon opt_joins opt_where opt_groupby opt_having opt_orderby opt_limit opt_offset
    {
        $$ = &SelectStmt{
                distinct: $2,
                targets: $3,
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
|
    SELECT opt_distinct opt_targets
    {
        $$ = &SelectStmt{
            distinct: $2,
            targets: $3,
            ds: &valuesDataSource{rows: []*RowSpec{{}}},
        }
    }
;

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

opt_targets:
    '*'
    {
        $$ = nil
    }
|
    targets
    {
        $$ = $1
    }

targets:
    exp opt_as
    {
        $$ = []TargetEntry{{Exp: $1, As: $2}}
    }
|
    targets ',' exp opt_as
    {
        $$ = append($1, TargetEntry{Exp: $3, As: $4})
    }

selector:
    col
    {
        $$ = $1
    }
|
    col jsonFields
    {
        $$ = &JSONSelector{ColSelector: $1, fields: $2}
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

jsonFields:
    ARROW VARCHAR_LIT
    {
        $$ = []string{$2}
    }
|
    jsonFields ARROW VARCHAR_LIT
    {
        $$ = append($$, $3)
    }

col:
    col_name
    {
        $$ = &ColSelector{col: $1}
    }
|
    col_name DOT col_name
    {
        $$ = &ColSelector{table: $1, col: $3}
    }
;

tableName: qualifiedName;

col_name:
    qualifiedName
|
    colNameKeyword { $$ = $1 }
;

col_names:
    col_name { $$ = []string{$1} }
|
    col_names ',' col_name { $$ = append($1, $3) }  
;

one_or_more_col_names:
    col_name
    {
        $$ = []string{$1}
    }
|
    '(' col_names ')'
    {
        $$ = $2
    }
;

insert_cols:
    { $$ = nil }
|
    '(' col_names ')' { $$ = $2 }
;


colNameKeyword:
    BETWEEN
    | BLOB_TYPE
    | BOOLEAN_TYPE
    | EXISTS
    | EXTRACT
    | FLOAT_TYPE
    | INTEGER_TYPE
    | JSON_TYPE
    | TIMESTAMP_TYPE
    | VALUES
    | VARCHAR_TYPE
;

qualifiedName:
    IDENTIFIER { $$ = $1 }
    | unreserved_keyword { $$ = string($1) }
;

unreserved_keyword:
    ADMIN
    | OF
    | DROP
    | DATABASE
    | SNAPSHOT
    | INDEX
    | ALTER
    | ADD
    | RENAME
    | CONSTRAINT
    | KEY
    | GRANT
    | REVOKE
    | PRIVILEGES
    | BEGIN
    | TRANSACTION
    | COMMIT
    | ROLLBACK
    | INSERT
    | DELETE
    | UPDATE
    | CONFLICT
    | IF
    | SHOW
    | TABLES
    | YEAR
    | MONTH
    | DAY
    | HOUR
    | MINUTE
    | SECOND
    | USERS
;

ds:
    tableRef opt_period opt_as
    {
        $1.period = $2
        $1.as = $3
        $$ = $1
    }
|
    '(' VALUES rows ')'
    {
        $$ = &valuesDataSource{inferTypes: true, rows: $3}
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
    qualifiedName
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
    ORDER BY ordexps
    {
        $$ = $3
    }

opt_indexon:
    {
        $$ = nil
    }
|
    USE INDEX ON one_or_more_col_names
    {
        $$ = $4
    }
;

ordexps:
    exp opt_ord
    {
        $$ = []*OrdExp{{exp: $1, descOrder: $2}}
    }
|
    ordexps ',' exp opt_ord
    {
        $$ = append($1, &OrdExp{exp: $3, descOrder: $4})
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
    qualifiedName
    {
        $$ = $1
    }
|
    AS qualifiedName
    {
        $$ = $2
    }
;

check:
    CHECK exp
    {
        $$ = CheckConstraint{exp: $2}
    }
|
    CONSTRAINT IDENTIFIER CHECK exp
    {
        $$ = CheckConstraint{name: $2, exp: $4}
    }

opt_exp:
    {
        $$ = nil
    }
|
    exp
    {
        $$ = $1
    }
;

case_when_exp:
    CASE opt_exp when_then_clauses opt_else END
    {
        $$ = &CaseWhenExp{
            exp: $2,
            whenThen: $3,
            elseExp: $4,
        }
    }
;

when_then_clauses:
    WHEN exp THEN exp
    {
        $$ = []whenThenClause{{when: $2, then: $4}}
    }
|
    when_then_clauses WHEN exp THEN exp
    {
        $$ = append($1, whenThenClause{when: $3, then: $5})
    }
;

opt_else:
    {
        $$ = nil
    }
|
    ELSE exp
    {
        $$ = $2
    }
;

exp
    : orExp { $$ = $1 }
    ;

orExp
    : orExp OR andExp { $$ = &BinBoolExp{left: $1, op: Or, right: $3} }
    | andExp
    ;

andExp
    : andExp AND notExp { $$ = &BinBoolExp{left: $1, op: And, right: $3} }
    | notExp
    ;

notExp
    : NOT notExp { $$ = &NotBoolExp{exp: $2} }
    | cmpExp
    ;

cmpExp
    : addExp CMPOP addExp               { $$ = &CmpBoolExp{left: $1, op: $2, right: $3} }
    | addExp IS NULL                    { $$ = &CmpBoolExp{left: $1, op: EQ, right: &NullValue{t: AnyType}} }
    | addExp IS NOT NULL                { $$ = &CmpBoolExp{left: $1, op: NE, right: &NullValue{t: AnyType}} }
    | addExp BETWEEN addExp AND addExp
    {
        $$ = &BinBoolExp{
            left: &CmpBoolExp{
                left: $1,
                op: GE,
                right: $3,
            },
            op: And,
            right: &CmpBoolExp{
                left: $1,
                op: LE,
                right: $5,
            },
        }
    }
    | addExp opt_not LIKE addExp    { $$ = &LikeBoolExp{val: $1, notLike: $2, pattern: $4} }
    | addExp NOT_MATCHES_OP addExp  { $$ = &LikeBoolExp{val: $1, notLike: true, pattern: $3} }
    | primaryBool
    ;

primaryBool
    : EXISTS '(' dqlstmt ')'            { $$ = &ExistsBoolExp{q: ($3).(DataSource)} }
    | addExp opt_not IN '(' dqlstmt ')' { $$ = &InSubQueryExp{val: $1, notIn: $2, q: $5.(*SelectStmt)} }
    | addExp opt_not IN '(' values ')'  { $$ = &InListExp{val: $1, notIn: $2, values: $5} }
    | case_when_exp                     { $$ = $1 }
    | addExp
    ;

addExp
    : addExp '+' mulExp { $$ = &NumExp{left: $1, op: ADDOP, right: $3} }
    | addExp '-' mulExp { $$ = &NumExp{left: $1, op: SUBSOP, right: $3} }
    | mulExp
    ;

mulExp
    : mulExp '*' unaryExp { $$ = &NumExp{left: $1, op: MULTOP, right: $3} }
    | mulExp '/' unaryExp { $$ = &NumExp{left: $1, op: DIVOP, right: $3} }
    | mulExp '%' unaryExp { $$ = &NumExp{left: $1, op: MODOP, right: $3} }
    | unaryExp
    ;

unaryExp
    : '-' unaryExp
    {
        i, isInt := $2.(*Integer)
        if isInt {
            i.val = -i.val
            $$ = i
        } else {
            $$ = &NumExp{left: &Integer{val: 0}, op: SUBSOP, right: $2}
        }
    }
|
    primary
;

primary
    : '(' exp ')' { $$ = $2 }
    | boundexp
    ;

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
    boundexp SCAST sql_type
    {
        $$ = &Cast{val: $1, t: $3}
    }
|
    EXTRACT '(' timestamp_field FROM exp ')'
    {
        $$ = &ExtractFromTimestampExp{Field: $3, Exp: $5}
    }
;

opt_not:
    {
        $$ = false
    }
|
    NOT
    {
        $$ = true
    }
;

timestamp_field:
    YEAR   { $$ = TimestampFieldTypeYear; }
    | MONTH  { $$ = TimestampFieldTypeMonth; }
    | DAY    { $$ = TimestampFieldTypeDay; }
    | HOUR   { $$ = TimestampFieldTypeHour; }
    | MINUTE { $$ = TimestampFieldTypeMinute; }
    | SECOND { $$ = TimestampFieldTypeSecond; }
;
