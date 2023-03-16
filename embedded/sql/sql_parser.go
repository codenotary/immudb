// Code generated by goyacc -l -o sql_parser.go sql_grammar.y. DO NOT EDIT.
package sql

import __yyfmt__ "fmt"

import "fmt"

func setResult(l yyLexer, stmts []SQLStmt) {
	l.(*lexer).result = stmts
}

type yySymType struct {
	yys           int
	stmts         []SQLStmt
	stmt          SQLStmt
	datasource    DataSource
	colsSpec      []*ColSpec
	colSpec       *ColSpec
	cols          []*ColSelector
	rows          []*RowSpec
	row           *RowSpec
	values        []ValueExp
	value         ValueExp
	id            string
	number        uint64
	str           string
	boolean       bool
	blob          []byte
	sqlType       SQLValueType
	aggFn         AggregateFn
	ids           []string
	col           *ColSelector
	sel           Selector
	sels          []Selector
	distinct      bool
	ds            DataSource
	tableRef      *tableRef
	period        period
	openPeriod    *openPeriod
	periodInstant periodInstant
	joins         []*JoinSpec
	join          *JoinSpec
	joinType      JoinType
	exp           ValueExp
	binExp        ValueExp
	err           error
	ordcols       []*OrdCol
	opt_ord       bool
	logicOp       LogicOperator
	cmpOp         CmpOperator
	pparam        int
	update        *colUpdate
	updates       []*colUpdate
	onConflict    *OnConflictDo
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
const RENAME = 57363
const TO = 57364
const COLUMN = 57365
const PRIMARY = 57366
const KEY = 57367
const BEGIN = 57368
const TRANSACTION = 57369
const COMMIT = 57370
const ROLLBACK = 57371
const INSERT = 57372
const UPSERT = 57373
const INTO = 57374
const VALUES = 57375
const DELETE = 57376
const UPDATE = 57377
const SET = 57378
const CONFLICT = 57379
const DO = 57380
const NOTHING = 57381
const SELECT = 57382
const DISTINCT = 57383
const FROM = 57384
const JOIN = 57385
const HAVING = 57386
const WHERE = 57387
const GROUP = 57388
const BY = 57389
const LIMIT = 57390
const OFFSET = 57391
const ORDER = 57392
const ASC = 57393
const DESC = 57394
const AS = 57395
const UNION = 57396
const ALL = 57397
const NOT = 57398
const LIKE = 57399
const IF = 57400
const EXISTS = 57401
const IN = 57402
const IS = 57403
const AUTO_INCREMENT = 57404
const NULL = 57405
const CAST = 57406
const NPARAM = 57407
const PPARAM = 57408
const JOINTYPE = 57409
const LOP = 57410
const CMPOP = 57411
const IDENTIFIER = 57412
const TYPE = 57413
const NUMBER = 57414
const VARCHAR = 57415
const BOOLEAN = 57416
const BLOB = 57417
const AGGREGATE_FUNC = 57418
const ERROR = 57419
const STMT_SEPARATOR = 57420

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
	"RENAME",
	"TO",
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
	"OFFSET",
	"ORDER",
	"ASC",
	"DESC",
	"AS",
	"UNION",
	"ALL",
	"NOT",
	"LIKE",
	"IF",
	"EXISTS",
	"IN",
	"IS",
	"AUTO_INCREMENT",
	"NULL",
	"CAST",
	"NPARAM",
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
	-1, 74,
	57, 137,
	60, 137,
	-2, 126,
	-1, 186,
	43, 102,
	-2, 97,
	-1, 215,
	43, 102,
	-2, 99,
}

const yyPrivate = 57344

const yyLast = 373

var yyAct = [...]int{
	73, 292, 60, 180, 138, 208, 232, 236, 87, 135,
	144, 172, 214, 105, 231, 97, 173, 155, 45, 6,
	79, 265, 100, 178, 223, 203, 18, 178, 178, 178,
	274, 269, 72, 251, 268, 249, 224, 179, 237, 148,
	252, 250, 76, 219, 202, 78, 200, 192, 191, 90,
	86, 88, 89, 238, 146, 233, 91, 59, 82, 83,
	84, 85, 61, 177, 199, 109, 77, 131, 196, 131,
	157, 81, 130, 116, 102, 128, 76, 126, 127, 78,
	111, 108, 129, 90, 86, 88, 89, 96, 95, 20,
	91, 109, 82, 83, 84, 85, 61, 62, 248, 140,
	77, 291, 62, 61, 98, 81, 137, 285, 57, 123,
	228, 152, 147, 151, 255, 141, 121, 122, 159, 160,
	161, 162, 163, 164, 254, 149, 62, 117, 118, 120,
	119, 171, 174, 203, 170, 123, 193, 142, 178, 104,
	107, 62, 136, 201, 185, 230, 183, 61, 169, 186,
	175, 123, 194, 117, 118, 120, 119, 106, 121, 122,
	143, 189, 206, 190, 187, 184, 188, 195, 198, 117,
	118, 120, 119, 123, 254, 220, 71, 123, 27, 28,
	121, 122, 210, 101, 176, 122, 264, 212, 123, 156,
	158, 117, 118, 120, 119, 117, 118, 120, 119, 153,
	174, 218, 150, 112, 229, 65, 225, 63, 120, 119,
	235, 221, 34, 49, 44, 217, 227, 247, 239, 226,
	166, 156, 234, 197, 246, 92, 123, 165, 241, 240,
	263, 40, 167, 243, 174, 168, 110, 125, 64, 55,
	35, 277, 26, 293, 294, 256, 114, 115, 257, 209,
	181, 147, 261, 260, 284, 272, 259, 98, 271, 242,
	103, 266, 32, 37, 18, 273, 282, 275, 267, 53,
	207, 205, 278, 31, 30, 280, 21, 244, 133, 132,
	283, 204, 286, 93, 94, 281, 2, 289, 290, 287,
	211, 113, 76, 66, 295, 78, 182, 296, 39, 90,
	86, 88, 89, 10, 11, 43, 91, 38, 82, 83,
	84, 85, 61, 29, 145, 139, 77, 19, 12, 70,
	69, 81, 41, 42, 253, 7, 22, 8, 9, 13,
	14, 33, 99, 15, 16, 23, 25, 24, 124, 18,
	47, 48, 67, 245, 262, 50, 51, 52, 276, 288,
	222, 258, 75, 74, 270, 216, 215, 213, 68, 46,
	54, 36, 58, 56, 80, 279, 134, 154, 17, 5,
	4, 3, 1,
}

var yyPact = [...]int{
	299, -1000, -1000, 5, -1000, -1000, -1000, 249, -1000, -1000,
	320, 172, 298, 242, 241, 220, 142, 186, 222, -1000,
	299, -1000, 173, 173, 173, 288, -1000, 144, 332, 143,
	142, 142, 142, 233, -1000, 184, 27, -1000, -1000, 137,
	182, 135, 275, 173, -1000, -1000, 309, 20, 20, 263,
	3, 2, 212, 113, 224, -1000, 218, -1000, 61, 87,
	-1000, -4, 8, -1000, 177, -5, 133, 273, -1000, 20,
	20, -1000, 236, 112, 181, -1000, 236, 236, -10, -1000,
	-1000, 236, -1000, -1000, -1000, -1000, -13, -1000, -1000, -1000,
	-1000, -18, -1000, 256, 255, 72, 72, 310, 236, 59,
	-1000, 91, -1000, -31, 71, -1000, -1000, 132, 32, 129,
	-1000, 119, -15, 120, -1000, -1000, 112, 236, 236, 236,
	236, 236, 236, 164, 175, -1000, 116, 127, 224, 48,
	236, 236, 119, 114, -23, 60, -1000, -49, 202, 279,
	112, 310, 113, 236, 310, 332, 224, 87, -16, 87,
	-1000, -38, -39, -1000, 58, -1000, 81, 72, -17, 127,
	127, 165, 165, 116, 74, -1000, 160, 236, -21, -40,
	-1000, 90, -42, 55, 112, -1000, 259, 238, 92, 237,
	200, 236, 272, 202, -1000, 112, 148, 87, -43, -1000,
	-1000, -1000, -1000, 151, -63, -50, 72, -1000, 116, -14,
	-1000, 39, -1000, 236, 75, -30, -1000, -30, -1000, 236,
	112, -32, 200, 212, -1000, 148, 216, -1000, -1000, 87,
	252, -1000, 161, 26, -1000, -51, -45, -53, -46, 112,
	-1000, 96, -1000, 236, 46, 112, -1000, -1000, 72, -1000,
	210, -1000, -31, -1000, -32, 168, -1000, 123, -67, -1000,
	-1000, -1000, -1000, -1000, -30, 231, -52, -55, 214, 208,
	310, -56, -1000, -1000, -1000, -1000, -1000, 229, -1000, -1000,
	191, 236, 56, 267, -1000, 227, 202, 207, 112, 29,
	-1000, 236, -1000, 200, 56, 56, 112, -1000, 23, 192,
	-1000, 56, -1000, -1000, -1000, 192, -1000,
}

var yyPgo = [...]int{
	0, 372, 286, 371, 370, 369, 19, 368, 367, 17,
	9, 7, 366, 365, 14, 6, 16, 11, 364, 8,
	20, 363, 362, 2, 361, 360, 10, 314, 18, 359,
	358, 176, 357, 12, 356, 355, 0, 15, 354, 353,
	352, 351, 3, 5, 350, 13, 349, 348, 1, 4,
	298, 344, 343, 338, 22, 332, 324, 317,
}

var yyR1 = [...]int{
	0, 1, 2, 2, 57, 57, 3, 3, 3, 4,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	4, 4, 50, 50, 11, 11, 5, 5, 5, 5,
	56, 56, 55, 55, 54, 12, 12, 14, 14, 15,
	10, 10, 13, 13, 17, 17, 16, 16, 18, 18,
	18, 18, 18, 18, 18, 18, 18, 19, 8, 8,
	9, 44, 44, 51, 51, 52, 52, 52, 6, 6,
	7, 25, 25, 24, 24, 21, 21, 22, 22, 20,
	20, 20, 23, 23, 26, 26, 26, 27, 28, 29,
	29, 29, 30, 30, 30, 31, 31, 32, 32, 33,
	33, 34, 35, 35, 37, 37, 41, 41, 38, 38,
	42, 42, 43, 43, 47, 47, 49, 49, 46, 46,
	48, 48, 48, 45, 45, 45, 36, 36, 36, 36,
	36, 36, 36, 36, 39, 39, 39, 53, 53, 40,
	40, 40, 40, 40, 40, 40, 40,
}

var yyR2 = [...]int{
	0, 1, 2, 3, 0, 1, 1, 1, 1, 2,
	1, 1, 1, 4, 2, 3, 3, 11, 8, 9,
	6, 8, 0, 3, 1, 3, 9, 8, 7, 8,
	0, 4, 1, 3, 3, 0, 1, 1, 3, 3,
	1, 3, 1, 3, 0, 1, 1, 3, 1, 1,
	1, 1, 6, 1, 1, 1, 1, 4, 1, 3,
	5, 0, 3, 0, 1, 0, 1, 2, 1, 4,
	13, 0, 1, 0, 1, 1, 1, 2, 4, 1,
	4, 4, 1, 3, 3, 4, 2, 1, 2, 0,
	2, 2, 0, 2, 2, 2, 1, 0, 1, 1,
	2, 6, 0, 1, 0, 2, 0, 3, 0, 2,
	0, 2, 0, 2, 0, 3, 0, 4, 2, 4,
	0, 1, 1, 0, 1, 2, 1, 1, 2, 2,
	4, 4, 6, 6, 1, 1, 3, 0, 1, 3,
	3, 3, 3, 3, 3, 3, 4,
}

var yyChk = [...]int{
	-1000, -1, -2, -3, -4, -5, -6, 26, 28, 29,
	4, 5, 19, 30, 31, 34, 35, -7, 40, -57,
	84, 27, 6, 15, 17, 16, 70, 6, 7, 15,
	32, 32, 42, -27, 70, 54, -24, 41, -2, -50,
	58, -50, -50, 17, 70, -28, -29, 8, 9, 70,
	-27, -27, -27, 36, -25, 55, -21, 81, -22, -20,
	-23, 76, 70, 70, 56, 70, 18, -50, -30, 11,
	10, -31, 12, -36, -39, -40, 56, 80, 59, -20,
	-18, 85, 72, 73, 74, 75, 64, -19, 65, 66,
	63, 70, -31, 20, 21, 85, 85, -37, 45, -55,
	-54, 70, -6, 42, 78, -45, 70, 53, 85, 83,
	59, 85, 70, 18, -31, -31, -36, 79, 80, 82,
	81, 68, 69, 61, -53, 56, -36, -36, 85, -36,
	85, 85, 23, 23, -12, -10, 70, -10, -49, 5,
	-36, -37, 78, 69, -26, -27, 85, -19, 70, -20,
	70, 81, -23, 70, -8, -9, 70, 85, 70, -36,
	-36, -36, -36, -36, -36, 63, 56, 57, 60, -6,
	86, -36, -17, -16, -36, -9, 70, 86, 78, 86,
	-42, 48, 17, -49, -54, -36, -49, -28, -6, -45,
	-45, 86, 86, 78, 71, -10, 85, 63, -36, 85,
	86, 53, 86, 78, 22, 33, 70, 33, -43, 49,
	-36, 18, -42, -32, -33, -34, -35, 67, -45, 86,
	24, -9, -44, 87, 86, -10, -6, -16, 71, -36,
	70, -14, -15, 85, -14, -36, -11, 70, 85, -43,
	-37, -33, 43, -45, 25, -52, 63, 56, 72, 86,
	86, 86, 86, -56, 78, 18, -17, -10, -41, 46,
	-26, -11, -51, 62, 63, 88, -15, 37, 86, 86,
	-38, 44, 47, -49, 86, 38, -47, 50, -36, -13,
	-23, 18, 39, -42, 47, 78, -36, -43, -46, -23,
	-23, 78, -48, 51, 52, -23, -48,
}

var yyDef = [...]int{
	0, -2, 1, 4, 6, 7, 8, 10, 11, 12,
	0, 0, 0, 0, 0, 0, 0, 68, 73, 2,
	5, 9, 22, 22, 22, 0, 14, 0, 89, 0,
	0, 0, 0, 0, 87, 71, 0, 74, 3, 0,
	0, 0, 0, 22, 15, 16, 92, 0, 0, 0,
	0, 0, 104, 0, 0, 72, 0, 75, 76, 123,
	79, 0, 82, 13, 0, 0, 0, 0, 88, 0,
	0, 90, 0, 96, -2, 127, 0, 0, 0, 134,
	135, 0, 48, 49, 50, 51, 0, 53, 54, 55,
	56, 82, 91, 0, 0, 35, 0, 116, 0, 104,
	32, 0, 69, 0, 0, 77, 124, 0, 0, 0,
	23, 0, 0, 0, 93, 94, 95, 0, 0, 0,
	0, 0, 0, 0, 0, 138, 128, 129, 0, 0,
	0, 44, 0, 0, 0, 36, 40, 0, 110, 0,
	105, 116, 0, 0, 116, 89, 0, 123, 87, 123,
	125, 0, 0, 83, 0, 58, 0, 0, 0, 139,
	140, 141, 142, 143, 144, 145, 0, 0, 0, 0,
	136, 0, 0, 45, 46, 20, 0, 0, 0, 0,
	112, 0, 0, 110, 33, 34, -2, 123, 0, 86,
	78, 80, 81, 0, 61, 0, 0, 146, 130, 0,
	131, 0, 57, 0, 0, 0, 41, 0, 28, 0,
	111, 0, 112, 104, 98, -2, 0, 103, 84, 123,
	0, 59, 65, 0, 18, 0, 0, 0, 0, 47,
	21, 30, 37, 44, 27, 113, 117, 24, 0, 29,
	106, 100, 0, 85, 0, 63, 66, 0, 0, 19,
	132, 133, 52, 26, 0, 0, 0, 0, 108, 0,
	116, 0, 60, 64, 67, 62, 38, 0, 39, 25,
	114, 0, 0, 0, 17, 0, 110, 0, 109, 107,
	42, 0, 31, 112, 0, 0, 101, 70, 115, 120,
	43, 0, 118, 121, 122, 120, 119,
}

var yyTok1 = [...]int{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	85, 86, 81, 79, 78, 80, 83, 82, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 87, 3, 88,
}

var yyTok2 = [...]int{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
	62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 73, 74, 75, 76, 77, 84,
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
			yyVAL.stmt = &BeginTransactionStmt{}
		}
	case 11:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = &CommitStmt{}
		}
	case 12:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = &RollbackStmt{}
		}
	case 13:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.stmt = &CreateDatabaseStmt{ifNotExists: yyDollar[3].boolean, DB: yyDollar[4].id}
		}
	case 14:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.stmt = &UseDatabaseStmt{DB: yyDollar[2].id}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &UseDatabaseStmt{DB: yyDollar[3].id}
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &UseSnapshotStmt{period: yyDollar[3].period}
		}
	case 17:
		yyDollar = yyS[yypt-11 : yypt+1]
		{
			yyVAL.stmt = &CreateTableStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[4].id, colsSpec: yyDollar[6].colsSpec, pkColNames: yyDollar[10].ids}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[5].id, cols: yyDollar[7].ids}
		}
	case 19:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{unique: true, ifNotExists: yyDollar[4].boolean, table: yyDollar[6].id, cols: yyDollar[8].ids}
		}
	case 20:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &AddColumnStmt{table: yyDollar[3].id, colSpec: yyDollar[6].colSpec}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &RenameColumnStmt{table: yyDollar[3].id, oldName: yyDollar[6].id, newName: yyDollar[8].id}
		}
	case 22:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 23:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 24:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 25:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = yyDollar[2].ids
		}
	case 26:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{isInsert: true, tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows, onConflict: yyDollar[9].onConflict}
		}
	case 27:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows}
		}
	case 28:
		yyDollar = yyS[yypt-7 : yypt+1]
		{
			yyVAL.stmt = &DeleteFromStmt{tableRef: yyDollar[3].tableRef, where: yyDollar[4].exp, indexOn: yyDollar[5].ids, limit: yyDollar[6].exp, offset: yyDollar[7].exp}
		}
	case 29:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpdateStmt{tableRef: yyDollar[2].tableRef, updates: yyDollar[4].updates, where: yyDollar[5].exp, indexOn: yyDollar[6].ids, limit: yyDollar[7].exp, offset: yyDollar[8].exp}
		}
	case 30:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.onConflict = nil
		}
	case 31:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.onConflict = &OnConflictDo{}
		}
	case 32:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.updates = []*colUpdate{yyDollar[1].update}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.updates = append(yyDollar[1].updates, yyDollar[3].update)
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.update = &colUpdate{col: yyDollar[1].id, op: yyDollar[2].cmpOp, val: yyDollar[3].exp}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 36:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = yyDollar[1].ids
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.rows = []*RowSpec{yyDollar[1].row}
		}
	case 38:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.rows = append(yyDollar[1].rows, yyDollar[3].row)
		}
	case 39:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.row = &RowSpec{Values: yyDollar[2].values}
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = append(yyDollar[1].ids, yyDollar[3].id)
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.cols = []*ColSelector{yyDollar[1].col}
		}
	case 43:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.values = nil
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = yyDollar[1].values
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = []ValueExp{yyDollar[1].exp}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].exp)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Number{val: int64(yyDollar[1].number)}
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Varchar{val: yyDollar[1].str}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Bool{val: yyDollar[1].boolean}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Blob{val: yyDollar[1].blob}
		}
	case 52:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.value = &Cast{val: yyDollar[3].exp, t: yyDollar[5].sqlType}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = yyDollar[1].value
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: yyDollar[1].id}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: fmt.Sprintf("param%d", yyDollar[1].pparam), pos: yyDollar[1].pparam}
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &NullValue{t: AnyType}
		}
	case 57:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.value = &FnCall{fn: yyDollar[1].id, params: yyDollar[3].values}
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.colsSpec = []*ColSpec{yyDollar[1].colSpec}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.colsSpec = append(yyDollar[1].colsSpec, yyDollar[3].colSpec)
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.colSpec = &ColSpec{colName: yyDollar[1].id, colType: yyDollar[2].sqlType, maxLen: int(yyDollar[3].number), notNull: yyDollar[4].boolean, autoIncrement: yyDollar[5].boolean}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 63:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 65:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 67:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = yyDollar[1].stmt
		}
	case 69:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.stmt = &UnionStmt{
				distinct: yyDollar[3].distinct,
				left:     yyDollar[1].stmt.(DataSource),
				right:    yyDollar[4].stmt.(DataSource),
			}
		}
	case 70:
		yyDollar = yyS[yypt-13 : yypt+1]
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
				limit:     yyDollar[12].exp,
				offset:    yyDollar[13].exp,
			}
		}
	case 71:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 73:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = nil
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = yyDollar[1].sels
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyDollar[1].sel.setAlias(yyDollar[2].id)
			yyVAL.sels = []Selector{yyDollar[1].sel}
		}
	case 78:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[3].sel.setAlias(yyDollar[4].id)
			yyVAL.sels = append(yyDollar[1].sels, yyDollar[3].sel)
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sel = yyDollar[1].col
		}
	case 80:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, col: "*"}
		}
	case 81:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, db: yyDollar[3].col.db, table: yyDollar[3].col.table, col: yyDollar[3].col.col}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.col = &ColSelector{col: yyDollar[1].id}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.col = &ColSelector{table: yyDollar[1].id, col: yyDollar[3].id}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyDollar[1].tableRef.period = yyDollar[2].period
			yyDollar[1].tableRef.as = yyDollar[3].id
			yyVAL.ds = yyDollar[1].tableRef
		}
	case 85:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[2].stmt.(*SelectStmt).as = yyDollar[4].id
			yyVAL.ds = yyDollar[2].stmt.(DataSource)
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ds = &FnDataSourceStmt{fnCall: yyDollar[1].value.(*FnCall), as: yyDollar[2].id}
		}
	case 87:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{table: yyDollar[1].id}
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.period = period{start: yyDollar[1].openPeriod, end: yyDollar[2].openPeriod}
		}
	case 89:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.openPeriod = nil
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{inclusive: true, instant: yyDollar[2].periodInstant}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{instant: yyDollar[2].periodInstant}
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.openPeriod = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{inclusive: true, instant: yyDollar[2].periodInstant}
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{instant: yyDollar[2].periodInstant}
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodInstant = periodInstant{instantType: txInstant, exp: yyDollar[2].exp}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.periodInstant = periodInstant{instantType: timeInstant, exp: yyDollar[1].exp}
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joins = nil
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = yyDollar[1].joins
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = []*JoinSpec{yyDollar[1].join}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.joins = append([]*JoinSpec{yyDollar[1].join}, yyDollar[2].joins...)
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.join = &JoinSpec{joinType: yyDollar[1].joinType, ds: yyDollar[3].ds, indexOn: yyDollar[4].ids, cond: yyDollar[6].exp}
		}
	case 102:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joinType = InnerJoin
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joinType = yyDollar[1].joinType
		}
	case 104:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 106:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.cols = nil
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = yyDollar[3].cols
		}
	case 108:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 109:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 110:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 111:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 112:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 113:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 114:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ordcols = nil
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ordcols = yyDollar[3].ordcols
		}
	case 116:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ids = yyDollar[4].ids
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ordcols = []*OrdCol{{sel: yyDollar[1].col, descOrder: yyDollar[2].opt_ord}}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ordcols = append(yyDollar[1].ordcols, &OrdCol{sel: yyDollar[3].col, descOrder: yyDollar[4].opt_ord})
		}
	case 120:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = true
		}
	case 123:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.id = ""
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.id = yyDollar[1].id
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.id = yyDollar[2].id
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].exp
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].binExp
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NotBoolExp{exp: yyDollar[2].exp}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NumExp{left: &Number{val: 0}, op: SUBSOP, right: yyDollar[2].exp}
		}
	case 130:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &LikeBoolExp{val: yyDollar[1].exp, notLike: yyDollar[2].boolean, pattern: yyDollar[4].exp}
		}
	case 131:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &ExistsBoolExp{q: (yyDollar[3].stmt).(*SelectStmt)}
		}
	case 132:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InSubQueryExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, q: yyDollar[5].stmt.(*SelectStmt)}
		}
	case 133:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InListExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, values: yyDollar[5].values}
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].sel
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].value
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 137:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: ADDOP, right: yyDollar[3].exp}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: SUBSOP, right: yyDollar[3].exp}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: DIVOP, right: yyDollar[3].exp}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: MULTOP, right: yyDollar[3].exp}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &BinBoolExp{left: yyDollar[1].exp, op: yyDollar[2].logicOp, right: yyDollar[3].exp}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: yyDollar[2].cmpOp, right: yyDollar[3].exp}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: EQ, right: &NullValue{t: AnyType}}
		}
	case 146:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: NE, right: &NullValue{t: AnyType}}
		}
	}
	goto yystack /* stack new state and value */
}
