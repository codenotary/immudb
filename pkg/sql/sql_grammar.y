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
    id string
    err error
}

%token CREATE USE DATABASE TABLE INDEX ON
%token <id> ID TYPE
%token <err> ERROR

%left ','
%right STMT_SEPARATOR

%type <stmts> sql
%type <stmts> sqlstmts
%type <stmt> sqlstmt
%type <colsSpec> colsSpec colSpecList
%type <colSpec> colSpec

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

sqlstmt:
    CREATE DATABASE ID
    {
        $$ = &CreateDatabaseStmt{db: $3}
    }
|   USE DATABASE ID
    {
        $$ = &UseDatabaseStmt{db: $3}
    }
|   CREATE TABLE ID colsSpec
    {
        $$ = &CreateTableStmt{table: $3, colsSpec: $4}
    }
|   CREATE INDEX ON ID '(' ID ')'
    {
        $$ = &CreateIndexStmt{table: $4, col: $6}
    }

colsSpec: 
    {
        $$ = nil
    }
|   '(' ')'
    {
        $$ = nil
    }
|   '(' colSpecList ')'
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

colSpec: ID TYPE
    {
      $$ = &ColSpec{colName: $1, colType: $2}
    }