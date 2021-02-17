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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

//go:generate go run golang.org/x/tools/cmd/goyacc -l -o sql_parser.go sql_grammar.y

var reservedWords = map[string]int{
	"CREATE":   CREATE,
	"USE":      USE,
	"DATABASE": DATABASE,
	"TABLE":    TABLE,
	"INDEX":    INDEX,
	"ON":       ON,
	"ALTER":    ALTER,
	"ADD":      ADD,
	"COLUMN":   COLUMN,
	"INSERT":   INSERT,
	"INTO":     INTO,
	"VALUES":   VALUES,
	"BEGIN":    BEGIN,
	"END":      END,
	"SELECT":   SELECT,
	"DISTINCT": DISTINCT,
	"FROM":     FROM,
	"INNER":    INNER,
	"JOIN":     JOIN,
	"HAVING":   HAVING,
	"WHERE":    WHERE,
	"GROUP":    GROUP,
	"BY":       BY,
	"OFFSET":   OFFSET,
	"LIMIT":    LIMIT,
	"ORDER":    ORDER,
	"AS":       AS,
	"ASC":      ASC,
	"DESC":     DESC,
	"NOT":      NOT,
	"LIKE":     LIKE,
}

var types = map[string]SQLValueType{
	"INTEGER":   IntegerType,
	"BOOLEAN":   BooleanType,
	"STRING":    StringType,
	"BLOB":      BLOBType,
	"TIMESTAMP": TimestampType,
}

var aggregateFns = map[string]AggregateFn{
	"COUNT": COUNT,
	"SUM":   SUM,
	"MAX":   MAX,
	"MIN":   MIN,
	"AVG":   AVG,
}

var boolValues = map[string]bool{
	"TRUE":  true,
	"FALSE": false,
}

var cmpOps = map[string]CmpOperator{
	"=":  EQ,
	"<":  LT,
	"<=": LE,
	">":  GT,
	">=": GE,
}

var logicOps = map[string]LogicOperator{
	"AND": AND,
	"OR":  OR,
}

type lexer struct {
	r      *aheadByteReader
	err    error
	result []SQLStmt
}

type aheadByteReader struct {
	nextChar byte
	nextErr  error
	r        io.ByteReader
}

func newAheadByteReader(r io.ByteReader) *aheadByteReader {
	ar := &aheadByteReader{r: r}
	ar.nextChar, ar.nextErr = r.ReadByte()
	return ar
}

func (ar *aheadByteReader) ReadByte() (byte, error) {
	defer func() {
		if ar.nextErr == nil {
			ar.nextChar, ar.nextErr = ar.r.ReadByte()
		}
	}()

	return ar.nextChar, ar.nextErr
}

func (ar *aheadByteReader) NextByte() (byte, error) {
	return ar.nextChar, ar.nextErr
}

func ParseString(sql string) ([]SQLStmt, error) {
	return Parse(strings.NewReader(sql))
}

func ParseBytes(sql []byte) ([]SQLStmt, error) {
	return Parse(bytes.NewReader(sql))
}

func Parse(r io.ByteReader) ([]SQLStmt, error) {
	lexer := newLexer(r)
	yyErrorVerbose = true

	yyParse(lexer)

	return lexer.result, lexer.err
}

func newLexer(r io.ByteReader) *lexer {
	return &lexer{
		r:   newAheadByteReader(r),
		err: nil,
	}
}

func (l *lexer) Lex(lval *yySymType) int {
	var ch byte
	var err error

	for {
		ch, err = l.r.ReadByte()
		if err == io.EOF {
			return 0
		}
		if err != nil {
			lval.err = err
			return ERROR
		}

		if !isSpace(ch) {
			break
		}
	}

	if isSeparator(ch) {
		if ch == '\r' && l.r.nextChar == '\n' {
			l.r.ReadByte()
		}
		return STMT_SEPARATOR
	}

	if isBLOBPrefix(ch) {
		if !isQuote(l.r.nextChar) {
			lval.err = fmt.Errorf("syntax error: unexpected char %c, expecting quote", l.r.nextChar)
			return ERROR
		}

		l.r.ReadByte() // consume starting quote

		tail, err := l.readString()
		if err != nil {
			lval.err = err
			return ERROR
		}

		l.r.ReadByte() // consume closing quote

		val, err := hex.DecodeString(tail)
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.blob = val
		return BLOB
	}

	if isLetter(ch) {
		tail, err := l.readWord()
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.id = fmt.Sprintf("%c%s", ch, tail)

		tid := strings.ToUpper(lval.id)

		sqlType, ok := types[tid]
		if ok {
			lval.sqlType = sqlType
			return TYPE
		}

		val, ok := boolValues[tid]
		if ok {
			lval.boolean = val
			return BOOLEAN
		}

		lop, ok := logicOps[tid]
		if ok {
			lval.logicOp = lop
			return LOP
		}

		afn, ok := aggregateFns[tid]
		if ok {
			lval.aggFn = afn
			return AGGREGATE_FUNC
		}

		tkn, ok := reservedWords[tid]
		if ok {
			return tkn
		}

		return IDENTIFIER
	}

	if isNumber(ch) {
		tail, err := l.readNumber()
		if err != nil {
			lval.err = err
			return ERROR
		}

		val, err := strconv.ParseUint(fmt.Sprintf("%c%s", ch, tail), 10, 64)
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.number = val
		return NUMBER
	}

	if isComparison(ch) {
		tail, err := l.readComparison()
		if err != nil {
			lval.err = err
			return ERROR
		}

		op := fmt.Sprintf("%c%s", ch, tail)

		cmpOp, ok := cmpOps[op]
		if !ok {
			lval.err = fmt.Errorf("Invalid comparison operator %s", op)
			return ERROR
		}

		lval.cmpOp = cmpOp
		return CMPOP
	}

	if isQuote(ch) {
		tail, err := l.readString()
		if err != nil {
			lval.err = err
			return ERROR
		}

		l.r.ReadByte() // consume closing quote

		lval.str = tail
		return STRING
	}

	return int(ch)
}

func (l *lexer) Error(err string) {
	l.err = errors.New(err)
}

func (l *lexer) readWord() (string, error) {
	return l.readWhile(func(ch byte) bool {
		return isLetter(ch) || isNumber(ch)
	})
}

func (l *lexer) readNumber() (string, error) {
	return l.readWhile(isNumber)
}

func (l *lexer) readString() (string, error) {
	return l.readWhile(func(ch byte) bool {
		return !isQuote(ch)
	})
}

func (l *lexer) readComparison() (string, error) {
	return l.readWhile(func(ch byte) bool {
		return isComparison(ch)
	})
}

func (l *lexer) readWhile(condFn func(b byte) bool) (string, error) {
	var b bytes.Buffer

	for {
		ch, err := l.r.NextByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if !condFn(ch) {
			break
		}

		ch, _ = l.r.ReadByte()
		b.WriteByte(ch)
	}

	return b.String(), nil
}

func isBLOBPrefix(ch byte) bool {
	return 'b' == ch
}

func isSeparator(ch byte) bool {
	return ';' == ch || '\r' == ch || '\n' == ch
}

func isSpace(ch byte) bool {
	return ' ' == ch
}

func isNumber(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isComparison(ch byte) bool {
	return '<' == ch || '=' == ch || '>' == ch
}

func isQuote(ch byte) bool {
	return '\'' == ch
}
