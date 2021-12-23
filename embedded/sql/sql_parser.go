// Code generated by goyacc -l -o sql_parser.go sql_grammar.y. DO NOT EDIT.
package sql

import __yyfmt__ "fmt"

import "fmt"

func setResult(l yyLexer, stmts []SQLStmt) {
	l.(*lexer).result = stmts
}

type yySymType struct {
	yys        int
	stmts      []SQLStmt
	stmt       SQLStmt
	colsSpec   []*ColSpec
	colSpec    *ColSpec
	cols       []*ColSelector
	rows       []*RowSpec
	row        *RowSpec
	values     []ValueExp
	value      ValueExp
	id         string
	number     uint64
	str        string
	boolean    bool
	blob       []byte
	sqlType    SQLValueType
	aggFn      AggregateFn
	ids        []string
	col        *ColSelector
	sel        Selector
	sels       []Selector
	distinct   bool
	ds         DataSource
	tableRef   *tableRef
	period     *period
	joins      []*JoinSpec
	join       *JoinSpec
	joinType   JoinType
	exp        ValueExp
	binExp     ValueExp
	err        error
	ordcols    []*OrdCol
	opt_ord    bool
	logicOp    LogicOperator
	cmpOp      CmpOperator
	pparam     int
	update     *colUpdate
	updates    []*colUpdate
	onConflict *OnConflictDo
}

const CREATE = 57346
const USE = 57347
const DATABASE = 57348
const SNAPSHOT = 57349
const SINCE = 57350
const AFTER = 57351
const BEFORE = 57352
const UNTIL = 57353
const TX = 57354
const OF = 57355
const TIMESTAMP = 57356
const TABLE = 57357
const UNIQUE = 57358
const INDEX = 57359
const ON = 57360
const ALTER = 57361
const ADD = 57362
const COLUMN = 57363
const PRIMARY = 57364
const KEY = 57365
const BEGIN = 57366
const TRANSACTION = 57367
const COMMIT = 57368
const ROLLBACK = 57369
const INSERT = 57370
const UPSERT = 57371
const INTO = 57372
const VALUES = 57373
const DELETE = 57374
const UPDATE = 57375
const SET = 57376
const CONFLICT = 57377
const DO = 57378
const NOTHING = 57379
const SELECT = 57380
const DISTINCT = 57381
const FROM = 57382
const JOIN = 57383
const HAVING = 57384
const WHERE = 57385
const GROUP = 57386
const BY = 57387
const LIMIT = 57388
const ORDER = 57389
const ASC = 57390
const DESC = 57391
const AS = 57392
const NOT = 57393
const LIKE = 57394
const IF = 57395
const EXISTS = 57396
const IN = 57397
const IS = 57398
const AUTO_INCREMENT = 57399
const NULL = 57400
const NPARAM = 57401
const CAST = 57402
const PPARAM = 57403
const JOINTYPE = 57404
const LOP = 57405
const CMPOP = 57406
const IDENTIFIER = 57407
const TYPE = 57408
const NUMBER = 57409
const VARCHAR = 57410
const BOOLEAN = 57411
const BLOB = 57412
const AGGREGATE_FUNC = 57413
const ERROR = 57414
const STMT_SEPARATOR = 57415

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"CREATE",
	"USE",
	"DATABASE",
	"SNAPSHOT",
	"SINCE",
	"AFTER",
	"BEFORE",
	"UNTIL",
	"TX",
	"OF",
	"TIMESTAMP",
	"TABLE",
	"UNIQUE",
	"INDEX",
	"ON",
	"ALTER",
	"ADD",
	"COLUMN",
	"PRIMARY",
	"KEY",
	"BEGIN",
	"TRANSACTION",
	"COMMIT",
	"ROLLBACK",
	"INSERT",
	"UPSERT",
	"INTO",
	"VALUES",
	"DELETE",
	"UPDATE",
	"SET",
	"CONFLICT",
	"DO",
	"NOTHING",
	"SELECT",
	"DISTINCT",
	"FROM",
	"JOIN",
	"HAVING",
	"WHERE",
	"GROUP",
	"BY",
	"LIMIT",
	"ORDER",
	"ASC",
	"DESC",
	"AS",
	"NOT",
	"LIKE",
	"IF",
	"EXISTS",
	"IN",
	"IS",
	"AUTO_INCREMENT",
	"NULL",
	"NPARAM",
	"CAST",
	"PPARAM",
	"JOINTYPE",
	"LOP",
	"CMPOP",
	"IDENTIFIER",
	"TYPE",
	"NUMBER",
	"VARCHAR",
	"BOOLEAN",
	"BLOB",
	"AGGREGATE_FUNC",
	"ERROR",
	"','",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'.'",
	"STMT_SEPARATOR",
	"'('",
	"')'",
	"'['",
	"']'",
}

var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 69,
	52, 128,
	55, 128,
	-2, 117,
	-1, 174,
	41, 95,
	-2, 90,
	-1, 198,
	41, 95,
	-2, 92,
}

const yyPrivate = 57344

const yyLast = 354

var yyAct = [...]int{
	68, 275, 56, 168, 131, 215, 218, 137, 128, 211,
	98, 197, 90, 214, 6, 17, 146, 42, 248, 93,
	74, 166, 232, 207, 258, 166, 166, 252, 71, 253,
	233, 73, 67, 230, 208, 85, 83, 81, 84, 234,
	219, 166, 82, 116, 77, 78, 79, 80, 57, 167,
	114, 115, 72, 231, 55, 220, 216, 76, 19, 202,
	32, 110, 111, 113, 112, 188, 179, 178, 109, 165,
	163, 71, 119, 120, 73, 139, 187, 122, 85, 83,
	81, 84, 102, 184, 124, 82, 148, 77, 78, 79,
	80, 57, 133, 123, 121, 72, 103, 101, 130, 89,
	76, 116, 88, 180, 143, 134, 102, 51, 114, 115,
	116, 150, 151, 152, 153, 154, 155, 237, 140, 110,
	111, 113, 112, 58, 162, 274, 161, 116, 110, 111,
	113, 112, 116, 269, 142, 115, 160, 173, 58, 171,
	232, 91, 174, 164, 57, 110, 111, 113, 112, 53,
	236, 177, 113, 112, 176, 172, 175, 183, 181, 186,
	71, 166, 97, 73, 229, 136, 213, 85, 83, 81,
	84, 135, 236, 193, 82, 195, 77, 78, 79, 80,
	57, 58, 100, 182, 72, 204, 201, 57, 212, 76,
	58, 129, 66, 209, 189, 203, 191, 99, 205, 94,
	116, 147, 210, 149, 144, 141, 217, 114, 115, 221,
	222, 125, 105, 224, 95, 59, 32, 212, 110, 111,
	113, 112, 46, 41, 36, 247, 239, 200, 147, 240,
	228, 243, 244, 249, 157, 185, 116, 227, 86, 246,
	158, 156, 250, 159, 104, 38, 118, 60, 257, 276,
	277, 261, 169, 268, 256, 242, 262, 107, 108, 264,
	91, 10, 11, 255, 267, 223, 270, 37, 96, 30,
	34, 272, 273, 17, 266, 259, 12, 278, 251, 50,
	279, 7, 138, 8, 9, 13, 14, 192, 29, 15,
	16, 39, 190, 28, 20, 17, 225, 126, 87, 31,
	2, 265, 194, 106, 61, 21, 170, 40, 62, 27,
	132, 47, 48, 49, 22, 24, 23, 65, 64, 18,
	35, 44, 45, 25, 26, 235, 92, 117, 226, 245,
	260, 271, 206, 241, 70, 69, 254, 199, 198, 196,
	63, 43, 33, 54, 52, 75, 238, 263, 127, 145,
	5, 4, 3, 1,
}

var yyPact = [...]int{
	257, -1000, -1000, -21, -1000, -1000, -1000, 269, -1000, -1000,
	299, 317, 294, 263, 258, 229, 151, 231, -1000, 257,
	-1000, 159, 192, 192, 290, 158, 313, 157, 151, 151,
	151, 245, 29, 73, -1000, -1000, -1000, 150, 196, 286,
	192, -1000, -1000, 307, 20, 20, 278, 22, 19, 217,
	134, 149, 228, -1000, 89, 132, -1000, 17, 28, 16,
	190, 147, 285, -1000, 20, 20, -1000, 109, -13, 195,
	-1000, 109, 109, 14, -1000, -1000, 109, -1000, -1000, -1000,
	-1000, 13, 4, 146, -1000, -1000, -1000, 276, 126, 126,
	305, 109, 98, -1000, 101, -1000, -5, 116, -1000, -1000,
	140, 58, 139, 136, -1000, 6, 138, -1000, -1000, -13,
	109, 109, 109, 109, 109, 109, 183, 188, -1000, 71,
	76, 235, 45, 109, -11, -1000, 136, -12, 88, -1000,
	-32, 206, 289, -13, 305, 134, 109, 305, 313, 235,
	132, -1000, -14, -15, 25, 85, -1000, 117, 126, 3,
	76, 76, 180, 180, 71, 54, -1000, 177, 109, -4,
	-16, -1000, 144, -1000, -1000, 261, 131, 256, -1000, 106,
	284, 206, -1000, -13, 165, 132, -22, -1000, -1000, -1000,
	130, 163, -59, -47, 126, -1000, 71, -23, -1000, 100,
	-24, -1000, -24, -1000, -25, -1000, 217, -1000, 165, 224,
	-1000, -1000, 132, -1000, 273, -1000, 179, 97, -1000, -48,
	-28, -51, -13, -42, 99, -1000, 109, 77, -1000, -1000,
	126, 211, -1000, -5, -1000, -25, 182, -1000, 167, -65,
	-1000, -1000, 109, -1000, -1000, -1000, -24, 243, -54, 67,
	-52, 221, 209, 305, -57, -1000, -1000, -1000, -1000, -13,
	-1000, 239, -1000, -1000, 204, 109, 125, 283, -1000, 237,
	206, 208, -13, 60, -1000, 109, -1000, -1000, 125, 125,
	-13, 52, 201, -1000, 125, -1000, -1000, -1000, 201, -1000,
}

var yyPgo = [...]int{
	0, 353, 300, 352, 351, 14, 350, 349, 16, 8,
	6, 348, 347, 13, 5, 9, 346, 345, 20, 344,
	343, 2, 342, 7, 282, 17, 341, 340, 192, 339,
	11, 338, 337, 0, 12, 336, 335, 334, 333, 3,
	332, 10, 331, 330, 1, 4, 267, 329, 328, 327,
	19, 326, 325, 319,
}

var yyR1 = [...]int{
	0, 1, 2, 2, 53, 53, 3, 3, 3, 4,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 46,
	46, 10, 10, 6, 6, 6, 6, 52, 52, 51,
	51, 50, 11, 11, 13, 13, 14, 9, 9, 12,
	12, 16, 16, 15, 15, 17, 17, 17, 17, 17,
	17, 17, 17, 17, 7, 7, 8, 40, 40, 47,
	47, 48, 48, 48, 5, 22, 22, 19, 19, 20,
	20, 18, 18, 18, 21, 21, 21, 23, 23, 24,
	24, 25, 26, 26, 26, 27, 27, 27, 28, 28,
	29, 29, 30, 30, 31, 32, 32, 34, 34, 38,
	38, 35, 35, 39, 39, 43, 43, 45, 45, 42,
	42, 44, 44, 44, 41, 41, 41, 33, 33, 33,
	33, 33, 33, 33, 33, 36, 36, 36, 49, 49,
	37, 37, 37, 37, 37, 37, 37, 37,
}

var yyR2 = [...]int{
	0, 1, 2, 3, 0, 1, 1, 1, 1, 2,
	1, 1, 3, 3, 3, 11, 8, 9, 6, 0,
	3, 1, 3, 9, 8, 6, 7, 0, 4, 1,
	3, 3, 0, 1, 1, 3, 3, 1, 3, 1,
	3, 0, 1, 1, 3, 1, 1, 1, 1, 6,
	3, 2, 1, 1, 1, 3, 5, 0, 3, 0,
	1, 0, 1, 2, 12, 0, 1, 1, 1, 2,
	4, 1, 4, 4, 1, 3, 5, 3, 4, 1,
	3, 2, 0, 2, 2, 0, 2, 2, 2, 1,
	0, 1, 1, 2, 6, 0, 1, 0, 2, 0,
	3, 0, 2, 0, 2, 0, 3, 0, 4, 2,
	4, 0, 1, 1, 0, 1, 2, 1, 1, 2,
	2, 4, 4, 6, 6, 1, 1, 3, 0, 1,
	3, 3, 3, 3, 3, 3, 3, 4,
}

var yyChk = [...]int{
	-1000, -1, -2, -3, -4, -6, -5, 24, 26, 27,
	4, 5, 19, 28, 29, 32, 33, 38, -53, 79,
	25, 6, 15, 17, 16, 6, 7, 15, 30, 30,
	40, -24, 65, -22, 39, -2, 65, -46, 53, -46,
	17, 65, -25, -26, 8, 9, 65, -24, -24, -24,
	34, 78, -19, 76, -20, -18, -21, 71, 65, 65,
	51, 18, -46, -27, 11, 10, -28, 12, -33, -36,
	-37, 51, 75, 54, -18, -17, 80, 67, 68, 69,
	70, 60, 65, 59, 61, 58, -28, 20, 80, 80,
	-34, 43, -51, -50, 65, 65, 40, 73, -41, 65,
	50, 80, 78, 80, 54, 65, 18, -28, -28, -33,
	74, 75, 77, 76, 63, 64, 56, -49, 51, -33,
	-33, 80, -33, 80, 80, 65, 21, -11, -9, 65,
	-9, -45, 5, -33, -34, 73, 64, -23, -24, 80,
	-18, 65, 76, -21, 65, -7, -8, 65, 80, 65,
	-33, -33, -33, -33, -33, -33, 58, 51, 52, 55,
	-5, 81, -33, 81, -8, 81, 73, 81, -39, 46,
	17, -45, -50, -33, -45, -25, -5, -41, 81, 81,
	78, 73, 66, -9, 80, 58, -33, 80, 81, 50,
	31, 65, 31, 67, 18, -39, -29, -30, -31, -32,
	62, -41, 81, 65, 22, -8, -40, 82, 81, -9,
	-5, -15, -33, 66, -13, -14, 80, -13, -10, 65,
	80, -34, -30, 41, -41, 23, -48, 58, 51, 67,
	81, 81, 73, 81, 81, -52, 73, 18, -16, -15,
	-9, -38, 44, -23, -10, -47, 57, 58, 83, -33,
	-14, 35, 81, 81, -35, 42, 45, -45, 81, 36,
	-43, 47, -33, -12, -21, 18, 37, -39, 45, 73,
	-33, -42, -21, -21, 73, -44, 48, 49, -21, -44,
}

var yyDef = [...]int{
	0, -2, 1, 4, 6, 7, 8, 0, 10, 11,
	0, 0, 0, 0, 0, 0, 0, 65, 2, 5,
	9, 0, 19, 19, 0, 0, 82, 0, 0, 0,
	0, 0, 79, 0, 66, 3, 12, 0, 0, 0,
	19, 13, 14, 85, 0, 0, 0, 0, 0, 97,
	0, 0, 0, 67, 68, 114, 71, 0, 74, 0,
	0, 0, 0, 81, 0, 0, 83, 0, 89, -2,
	118, 0, 0, 0, 125, 126, 0, 45, 46, 47,
	48, 0, 74, 0, 52, 53, 84, 0, 32, 0,
	107, 0, 97, 29, 0, 80, 0, 0, 69, 115,
	0, 0, 0, 0, 20, 0, 0, 86, 87, 88,
	0, 0, 0, 0, 0, 0, 0, 0, 129, 119,
	120, 0, 0, 0, 0, 51, 0, 0, 33, 37,
	0, 103, 0, 98, 107, 0, 0, 107, 82, 0,
	114, 116, 0, 0, 75, 0, 54, 0, 0, 0,
	130, 131, 132, 133, 134, 135, 136, 0, 0, 0,
	0, 127, 0, 50, 18, 0, 0, 0, 25, 0,
	0, 103, 30, 31, -2, 114, 0, 70, 72, 73,
	0, 0, 57, 0, 0, 137, 121, 0, 122, 0,
	0, 38, 0, 104, 0, 26, 97, 91, -2, 0,
	96, 77, 114, 76, 0, 55, 61, 0, 16, 0,
	0, 0, 43, 0, 27, 34, 41, 24, 108, 21,
	0, 99, 93, 0, 78, 0, 59, 62, 0, 0,
	17, 123, 0, 124, 49, 23, 0, 0, 0, 42,
	0, 101, 0, 107, 0, 56, 60, 63, 58, 44,
	35, 0, 36, 22, 105, 0, 0, 0, 15, 0,
	103, 0, 102, 100, 39, 0, 28, 64, 0, 0,
	94, 106, 111, 40, 0, 109, 112, 113, 111, 110,
}

var yyTok1 = [...]int{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	80, 81, 76, 74, 73, 75, 78, 77, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 82, 3, 83,
}

var yyTok2 = [...]int{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
	62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 79,
}

var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmts = yyDollar[1].stmts
			setResult(yylex, yyDollar[1].stmts)
		}
	case 2:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.stmts = []SQLStmt{yyDollar[1].stmt}
		}
	case 3:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmts = append([]SQLStmt{yyDollar[1].stmt}, yyDollar[3].stmts...)
		}
	case 4:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
		}
	case 9:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.stmt = &BeginTransactionStmt{}
		}
	case 10:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = &CommitStmt{}
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = &RollbackStmt{}
		}
	case 12:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &CreateDatabaseStmt{DB: yyDollar[3].id}
		}
	case 13:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &UseDatabaseStmt{DB: yyDollar[3].id}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &UseSnapshotStmt{period: yyDollar[3].period}
		}
	case 15:
		yyDollar = yyS[yypt-11 : yypt+1]
		{
			yyVAL.stmt = &CreateTableStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[4].id, colsSpec: yyDollar[6].colsSpec, pkColNames: yyDollar[10].ids}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[5].id, cols: yyDollar[7].ids}
		}
	case 17:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{unique: true, ifNotExists: yyDollar[4].boolean, table: yyDollar[6].id, cols: yyDollar[8].ids}
		}
	case 18:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &AddColumnStmt{table: yyDollar[3].id, colSpec: yyDollar[6].colSpec}
		}
	case 19:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 21:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 22:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = yyDollar[2].ids
		}
	case 23:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{isInsert: true, tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows, onConflict: yyDollar[9].onConflict}
		}
	case 24:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows}
		}
	case 25:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &DeleteFromStmt{tableRef: yyDollar[3].tableRef, where: yyDollar[4].exp, indexOn: yyDollar[5].ids, limit: int(yyDollar[6].number)}
		}
	case 26:
		yyDollar = yyS[yypt-7 : yypt+1]
		{
			yyVAL.stmt = &UpdateStmt{tableRef: yyDollar[2].tableRef, updates: yyDollar[4].updates, where: yyDollar[5].exp, indexOn: yyDollar[6].ids, limit: int(yyDollar[7].number)}
		}
	case 27:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.onConflict = nil
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.onConflict = &OnConflictDo{}
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.updates = []*colUpdate{yyDollar[1].update}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.updates = append(yyDollar[1].updates, yyDollar[3].update)
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.update = &colUpdate{col: yyDollar[1].id, op: yyDollar[2].cmpOp, val: yyDollar[3].exp}
		}
	case 32:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = yyDollar[1].ids
		}
	case 34:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.rows = []*RowSpec{yyDollar[1].row}
		}
	case 35:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.rows = append(yyDollar[1].rows, yyDollar[3].row)
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.row = &RowSpec{Values: yyDollar[2].values}
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = append(yyDollar[1].ids, yyDollar[3].id)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.cols = []*ColSelector{yyDollar[1].col}
		}
	case 40:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 41:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.values = nil
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = yyDollar[1].values
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = []ValueExp{yyDollar[1].exp}
		}
	case 44:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].exp)
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Number{val: int64(yyDollar[1].number)}
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Varchar{val: yyDollar[1].str}
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Bool{val: yyDollar[1].boolean}
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Blob{val: yyDollar[1].blob}
		}
	case 49:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.value = &Cast{val: yyDollar[3].exp, t: yyDollar[5].sqlType}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.value = &SysFn{fn: yyDollar[1].id}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.value = &Param{id: yyDollar[2].id}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: fmt.Sprintf("param%d", yyDollar[1].pparam), pos: yyDollar[1].pparam}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &NullValue{t: AnyType}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.colsSpec = []*ColSpec{yyDollar[1].colSpec}
		}
	case 55:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.colsSpec = append(yyDollar[1].colsSpec, yyDollar[3].colSpec)
		}
	case 56:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.colSpec = &ColSpec{colName: yyDollar[1].id, colType: yyDollar[2].sqlType, maxLen: int(yyDollar[3].number), notNull: yyDollar[4].boolean, autoIncrement: yyDollar[5].boolean}
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 64:
		yyDollar = yyS[yypt-12 : yypt+1]
		{
			yyVAL.stmt = &SelectStmt{
				distinct:  yyDollar[2].distinct,
				selectors: yyDollar[3].sels,
				ds:        yyDollar[5].ds,
				indexOn:   yyDollar[6].ids,
				joins:     yyDollar[7].joins,
				where:     yyDollar[8].exp,
				groupBy:   yyDollar[9].cols,
				having:    yyDollar[10].exp,
				orderBy:   yyDollar[11].ordcols,
				limit:     int(yyDollar[12].number),
			}
		}
	case 65:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 67:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = nil
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = yyDollar[1].sels
		}
	case 69:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyDollar[1].sel.setAlias(yyDollar[2].id)
			yyVAL.sels = []Selector{yyDollar[1].sel}
		}
	case 70:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[3].sel.setAlias(yyDollar[4].id)
			yyVAL.sels = append(yyDollar[1].sels, yyDollar[3].sel)
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sel = yyDollar[1].col
		}
	case 72:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, col: "*"}
		}
	case 73:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, db: yyDollar[3].col.db, table: yyDollar[3].col.table, col: yyDollar[3].col.col}
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.col = &ColSelector{col: yyDollar[1].id}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.col = &ColSelector{table: yyDollar[1].id, col: yyDollar[3].id}
		}
	case 76:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.col = &ColSelector{db: yyDollar[1].id, table: yyDollar[3].id, col: yyDollar[5].id}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyDollar[1].tableRef.period = yyDollar[2].period
			yyDollar[1].tableRef.as = yyDollar[3].id
			yyVAL.ds = yyDollar[1].tableRef
		}
	case 78:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[2].stmt.(*SelectStmt).as = yyDollar[4].id
			yyVAL.ds = yyDollar[2].stmt.(DataSource)
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{table: yyDollar[1].id}
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{db: yyDollar[1].id, table: yyDollar[3].id}
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.period = &period{start: yyDollar[1].periodStart, end: yyDollar[2].periodEnd}
		}
	case 82:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.periodStart = nil
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodStart = &periodStart{inclusive: true, moment: yyDollar[2].periodMoment}
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodStart = &periodStart{moment: yyDollar[2].periodMoment}
		}
	case 85:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.periodEnd = nil
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodEnd = &periodEnd{inclusive: true, moment: yyDollar[2].periodMoment}
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodEnd = &periodEnd{moment: yyDollar[2].periodMoment}
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodMoment = &periodMomentTx{exp: yyDollar[2].exp}
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.periodMoment = &periodMomentTime{exp: yyDollar[1].exp}
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joins = nil
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = yyDollar[1].joins
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = []*JoinSpec{yyDollar[1].join}
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.joins = append([]*JoinSpec{yyDollar[1].join}, yyDollar[2].joins...)
		}
	case 94:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.join = &JoinSpec{joinType: yyDollar[1].joinType, ds: yyDollar[3].ds, indexOn: yyDollar[4].ids, cond: yyDollar[6].exp}
		}
	case 95:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joinType = InnerJoin
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joinType = yyDollar[1].joinType
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 99:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.cols = nil
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = yyDollar[3].cols
		}
	case 101:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 103:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 105:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ordcols = nil
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ordcols = yyDollar[3].ordcols
		}
	case 107:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ids = yyDollar[4].ids
		}
	case 109:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ordcols = []*OrdCol{{sel: yyDollar[1].col, descOrder: yyDollar[2].opt_ord}}
		}
	case 110:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ordcols = append(yyDollar[1].ordcols, &OrdCol{sel: yyDollar[3].col, descOrder: yyDollar[4].opt_ord})
		}
	case 111:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = true
		}
	case 114:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.id = ""
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.id = yyDollar[1].id
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.id = yyDollar[2].id
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].exp
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].binExp
		}
	case 119:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NotBoolExp{exp: yyDollar[2].exp}
		}
	case 120:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NumExp{left: &Number{val: 0}, op: SUBSOP, right: yyDollar[2].exp}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &LikeBoolExp{val: yyDollar[1].exp, notLike: yyDollar[2].boolean, pattern: yyDollar[4].exp}
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &ExistsBoolExp{q: (yyDollar[3].stmt).(*SelectStmt)}
		}
	case 123:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InSubQueryExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, q: yyDollar[5].stmt.(*SelectStmt)}
		}
	case 124:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InListExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, values: yyDollar[5].values}
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].sel
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].value
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 128:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: ADDOP, right: yyDollar[3].exp}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: SUBSOP, right: yyDollar[3].exp}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: DIVOP, right: yyDollar[3].exp}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: MULTOP, right: yyDollar[3].exp}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &BinBoolExp{left: yyDollar[1].exp, op: yyDollar[2].logicOp, right: yyDollar[3].exp}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: yyDollar[2].cmpOp, right: yyDollar[3].exp}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: EQ, right: &NullValue{t: AnyType}}
		}
	case 137:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: NE, right: &NullValue{t: AnyType}}
		}
	}
	goto yystack /* stack new state and value */
}
