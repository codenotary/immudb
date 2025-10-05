/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

//go:generate go run golang.org/x/tools/cmd/goyacc -l -o sql_parser.go sql_grammar.y

var keywords = map[string]int{
	"CREATE":         CREATE,
	"DROP":           DROP,
	"USE":            USE,
	"DATABASE":       DATABASE,
	"SNAPSHOT":       SNAPSHOT,
	"HISTORY":        HISTORY,
	"OF":             OF,
	"SINCE":          SINCE,
	"AFTER":          AFTER,
	"BEFORE":         BEFORE,
	"UNTIL":          UNTIL,
	"TABLE":          TABLE,
	"PRIMARY":        PRIMARY,
	"KEY":            KEY,
	"UNIQUE":         UNIQUE,
	"INDEX":          INDEX,
	"ON":             ON,
	"ALTER":          ALTER,
	"ADD":            ADD,
	"RENAME":         RENAME,
	"TO":             TO,
	"COLUMN":         COLUMN,
	"INSERT":         INSERT,
	"CONFLICT":       CONFLICT,
	"DO":             DO,
	"NOTHING":        NOTHING,
	"RETURNING":      RETURNING,
	"UPSERT":         UPSERT,
	"INTO":           INTO,
	"VALUES":         VALUES,
	"UPDATE":         UPDATE,
	"SET":            SET,
	"DELETE":         DELETE,
	"BEGIN":          BEGIN,
	"TRANSACTION":    TRANSACTION,
	"COMMIT":         COMMIT,
	"ROLLBACK":       ROLLBACK,
	"SELECT":         SELECT,
	"DISTINCT":       DISTINCT,
	"FROM":           FROM,
	"UNION":          UNION,
	"ALL":            ALL,
	"TX":             TX,
	"JOIN":           JOIN,
	"HAVING":         HAVING,
	"WHERE":          WHERE,
	"GROUP":          GROUP,
	"BY":             BY,
	"LIMIT":          LIMIT,
	"OFFSET":         OFFSET,
	"ORDER":          ORDER,
	"AS":             AS,
	"ASC":            ASC,
	"DESC":           DESC,
	"AND":            AND,
	"OR":             OR,
	"NOT":            NOT,
	"LIKE":           LIKE,
	"EXISTS":         EXISTS,
	"BETWEEN":        BETWEEN,
	"IN":             IN,
	"AUTO_INCREMENT": AUTO_INCREMENT,
	"NULL":           NULL,
	"IF":             IF,
	"IS":             IS,
	"CAST":           CAST,
	"::":             SCAST,
	"SHOW":           SHOW,
	"DATABASES":      DATABASES,
	"TABLES":         TABLES,
	"USERS":          USERS,
	"USER":           USER,
	"WITH":           WITH,
	"PASSWORD":       PASSWORD,
	"READ":           READ,
	"READWRITE":      READWRITE,
	"ADMIN":          ADMIN,
	"GRANT":          GRANT,
	"REVOKE":         REVOKE,
	"GRANTS":         GRANTS,
	"FOR":            FOR,
	"PRIVILEGES":     PRIVILEGES,
	"CHECK":          CHECK,
	"CONSTRAINT":     CONSTRAINT,
	"CASE":           CASE,
	"WHEN":           WHEN,
	"THEN":           THEN,
	"ELSE":           ELSE,
	"END":            END,
	"EXTRACT":        EXTRACT,
	"INTEGER":        INTEGER_TYPE,
	"BOOLEAN":        BOOLEAN_TYPE,
	"VARCHAR":        VARCHAR_TYPE,
	"TIMESTAMP":      TIMESTAMP_TYPE,
	"FLOAT":          FLOAT_TYPE,
	"BLOB":           BLOB_TYPE,
	"UUID":           UUID_TYPE,
	"JSON":           JSON_TYPE,
	"YEAR":           YEAR,
	"MONTH":          MONTH,
	"DAY":            DAY,
	"HOUR":           HOUR,
	"MINUTE":         MINUTE,
	"SECOND":         SECOND,
}

var joinTypes = map[string]JoinType{
	"INNER": InnerJoin,
	"LEFT":  LeftJoin,
	"RIGHT": RightJoin,
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
	"!=": NE,
	"<>": NE,
	"<":  LT,
	"<=": LE,
	">":  GT,
	">=": GE,
}

var ErrEitherNamedOrUnnamedParams = errors.New("either named or unnamed params")
var ErrEitherPosOrNonPosParams = errors.New("either positional or non-positional named params")
var ErrInvalidPositionalParameter = errors.New("invalid positional parameter")

type positionalParamType int

const (
	NamedNonPositionalParamType positionalParamType = iota + 1
	NamedPositionalParamType
	UnnamedParamType
)

type lexer struct {
	r               *aheadByteReader
	err             error
	namedParamsType positionalParamType
	paramsCount     int
	result          []SQLStmt
}

type aheadByteReader struct {
	nextChar  byte
	nextErr   error
	r         io.ByteReader
	readCount int
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

	ar.readCount++

	return ar.nextChar, ar.nextErr
}

func (ar *aheadByteReader) ReadCount() int {
	return ar.readCount
}

func (ar *aheadByteReader) NextByte() (byte, error) {
	return ar.nextChar, ar.nextErr
}

func ParseSQLString(sql string) ([]SQLStmt, error) {
	return ParseSQL(strings.NewReader(sql))
}

func ParseSQL(r io.ByteReader) ([]SQLStmt, error) {
	lexer := newLexer(r)

	yyParse(lexer)

	return lexer.result, lexer.err
}

func ParseExpFromString(exp string) (ValueExp, error) {
	stmt := fmt.Sprintf("SELECT * FROM t WHERE %s", exp)

	res, err := ParseSQLString(stmt)
	if err != nil {
		return nil, err
	}

	s := res[0].(*SelectStmt)
	return s.where, nil
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

		if ch == '\t' {
			continue
		}

		if ch == '/' && l.r.nextChar == '*' {
			l.r.ReadByte()

			for {
				ch, err := l.r.ReadByte()
				if err == io.EOF {
					break
				}
				if err != nil {
					lval.err = err
					return ERROR
				}

				if ch == '*' && l.r.nextChar == '/' {
					l.r.ReadByte() // consume closing slash
					break
				}
			}

			continue
		}

		if isLineBreak(ch) {
			if ch == '\r' && l.r.nextChar == '\n' {
				l.r.ReadByte()
			}
			continue
		}

		if !isSpace(ch) {
			break
		}
	}

	if isSeparator(ch) {
		return STMT_SEPARATOR
	}

	if ch == '-' && l.r.nextChar == '>' {
		l.r.ReadByte()
		return ARROW
	}

	if isBLOBPrefix(ch) && isQuote(l.r.nextChar) {
		l.r.ReadByte() // consume starting quote

		tail, err := l.readString()
		if err != nil {
			lval.err = err
			return ERROR
		}

		val, err := hex.DecodeString(tail)
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.blob = val
		return BLOB_LIT
	}

	if isLetter(ch) {
		tail, err := l.readWord()
		if err != nil {
			lval.err = err
			return ERROR
		}

		w := fmt.Sprintf("%c%s", ch, tail)
		tid := strings.ToUpper(w)

		val, ok := boolValues[tid]
		if ok {
			lval.boolean = val
			return BOOLEAN_LIT
		}

		afn, ok := aggregateFns[tid]
		if ok {
			lval.aggFn = afn
			return AGGREGATE_FUNC
		}

		join, ok := joinTypes[tid]
		if ok {
			lval.joinType = join
			return JOINTYPE
		}

		tkn, ok := keywords[tid]
		if ok {
			lval.keyword = w
			return tkn
		}

		lval.id = strings.ToLower(w)
		return IDENTIFIER
	}

	if isDoubleQuote(ch) {
		tail, err := l.readWord()
		if err != nil {
			lval.err = err
			return ERROR
		}

		if !isDoubleQuote(l.r.nextChar) {
			lval.err = fmt.Errorf("double quote expected")
			return ERROR
		}

		l.r.ReadByte() // consume ending quote

		lval.id = strings.ToLower(tail)
		return IDENTIFIER
	}

	if isNumber(ch) {
		tail, err := l.readNumber()
		if err != nil {
			lval.err = err
			return ERROR
		}
		// looking for a float
		if isDot(l.r.nextChar) {
			l.r.ReadByte() // consume dot

			decimalPart, err := l.readNumber()
			if err != nil {
				lval.err = err
				return ERROR
			}

			val, err := strconv.ParseFloat(fmt.Sprintf("%c%s.%s", ch, tail, decimalPart), 64)
			if err != nil {
				lval.err = err
				return ERROR
			}

			lval.float = val
			return FLOAT_LIT
		}

		val, err := strconv.ParseUint(fmt.Sprintf("%c%s", ch, tail), 10, 64)
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.integer = val
		return INTEGER_LIT
	}

	if isComparison(ch) {
		tail, err := l.readComparison()
		if err != nil {
			lval.err = err
			return ERROR
		}

		op := fmt.Sprintf("%c%s", ch, tail)
		if op == "!~" {
			return NOT_MATCHES_OP
		}

		cmpOp, ok := cmpOps[op]
		if !ok {
			lval.err = fmt.Errorf("invalid comparison operator %s", op)
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

		lval.str = tail
		return VARCHAR_LIT
	}

	if ch == ':' {
		ch, err := l.r.ReadByte()
		if err != nil {
			lval.err = err
			return ERROR
		}

		if ch != ':' {
			lval.err = fmt.Errorf("colon expected")
			return ERROR
		}

		return SCAST
	}

	if ch == '@' {
		if l.namedParamsType == UnnamedParamType {
			lval.err = ErrEitherNamedOrUnnamedParams
			return ERROR
		}

		if l.namedParamsType == NamedPositionalParamType {
			lval.err = ErrEitherPosOrNonPosParams
			return ERROR
		}

		l.namedParamsType = NamedNonPositionalParamType

		ch, err := l.r.NextByte()
		if err != nil {
			lval.err = err
			return ERROR
		}

		if !isLetter(ch) {
			return ERROR
		}

		id, err := l.readWord()
		if err != nil {
			lval.err = err
			return ERROR
		}

		lval.id = strings.ToLower(id)

		return NPARAM
	}

	if ch == '$' {
		if l.namedParamsType == UnnamedParamType {
			lval.err = ErrEitherNamedOrUnnamedParams
			return ERROR
		}

		if l.namedParamsType == NamedNonPositionalParamType {
			lval.err = ErrEitherPosOrNonPosParams
			return ERROR
		}

		id, err := l.readNumber()
		if err != nil {
			lval.err = err
			return ERROR
		}

		pid, err := strconv.Atoi(id)
		if err != nil {
			lval.err = err
			return ERROR
		}

		if pid < 1 {
			lval.err = ErrInvalidPositionalParameter
			return ERROR
		}

		lval.pparam = pid

		l.namedParamsType = NamedPositionalParamType

		return PPARAM
	}

	if ch == '?' {
		if l.namedParamsType == NamedNonPositionalParamType || l.namedParamsType == NamedPositionalParamType {
			lval.err = ErrEitherNamedOrUnnamedParams
			return ERROR
		}

		l.paramsCount++
		lval.pparam = l.paramsCount

		l.namedParamsType = UnnamedParamType

		return PPARAM
	}

	if isDot(ch) {
		if isNumber(l.r.nextChar) { // looking for  a float
			decimalPart, err := l.readNumber()
			if err != nil {
				lval.err = err
				return ERROR
			}
			val, err := strconv.ParseFloat(fmt.Sprintf("%d.%s", 0, decimalPart), 64)
			if err != nil {
				lval.err = err
				return ERROR
			}
			lval.float = val
			return FLOAT_LIT
		}
		return DOT
	}

	return int(ch)
}

func (l *lexer) Error(err string) {
	l.err = fmt.Errorf("%s at position %d", err, l.r.ReadCount())
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
	var b bytes.Buffer

	for {
		ch, err := l.r.ReadByte()
		if err != nil {
			return "", err
		}

		nextCh, _ := l.r.NextByte()

		if isQuote(ch) {
			if isQuote(nextCh) {
				l.r.ReadByte() // consume escaped quote
			} else {
				break // string completely read
			}
		}

		b.WriteByte(ch)
	}

	return b.String(), nil
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
	return ch == 'x'
}

func isSeparator(ch byte) bool {
	return ch == ';'
}

func isLineBreak(ch byte) bool {
	return ch == '\r' || ch == '\n'
}

func isSpace(ch byte) bool {
	return ch == 32 || ch == 9 //SPACE or TAB
}

func isNumber(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isComparison(ch byte) bool {
	return ch == '!' || ch == '<' || ch == '=' || ch == '>' || ch == '~'
}

func isQuote(ch byte) bool {
	return ch == 0x27
}

func isDoubleQuote(ch byte) bool {
	return ch == 0x22
}

func isDot(ch byte) bool {
	return ch == '.'
}

func newCreateTableStmt(
	name string,
	elems []TableElem,
	ifNotExists bool,
) *CreateTableStmt {
	colsSpecs := make([]*ColSpec, 0, 5)
	var checks []CheckConstraint

	var pk PrimaryKeyConstraint
	for _, e := range elems {
		switch c := e.(type) {
		case *ColSpec:
			colsSpecs = append(colsSpecs, c)
		case PrimaryKeyConstraint:
			pk = c
		case CheckConstraint:
			if checks == nil {
				checks = make([]CheckConstraint, 0, 5)
			}
			checks = append(checks, c)
		}
	}

	return &CreateTableStmt{
		ifNotExists: ifNotExists,
		table:       name,
		colsSpec:    colsSpecs,
		pkColNames:  pk,
		checks:      checks,
	}
}
