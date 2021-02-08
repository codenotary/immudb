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
	"bytes"
	"errors"
	"io"
	"strings"
)

//go:generate go run golang.org/x/tools/cmd/goyacc -l -o sql_parser.go sql_grammar.y

type lexer struct {
	i      int
	err    error
	result []SQLStmt
}

func ParseString(sql string) ([]SQLStmt, error) {
	return Parse(strings.NewReader(sql))
}

func ParseBytes(sql []byte) ([]SQLStmt, error) {
	return Parse(bytes.NewReader(sql))
}

func Parse(r io.Reader) ([]SQLStmt, error) {
	lexer := newLexer(r)

	yyParse(lexer)

	return lexer.result, lexer.err
}

func newLexer(r io.Reader) *lexer {
	return &lexer{
		i:   0,
		err: nil,
	}
}

func (l *lexer) Lex(lval *yySymType) int {

	l.i++

	switch l.i {
	case 1:
		return CREATE
	case 2:
		return DATABASE
	}

	lval.id = "mydb1"

	return ID

}

func (l *lexer) Error(err string) {
	l.err = errors.New(err)
}
