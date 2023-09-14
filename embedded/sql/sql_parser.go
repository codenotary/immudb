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
	integer       uint64
	float         float64
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
const DROP = 57347
const USE = 57348
const DATABASE = 57349
const SNAPSHOT = 57350
const SINCE = 57351
const AFTER = 57352
const BEFORE = 57353
const UNTIL = 57354
const TX = 57355
const OF = 57356
const TIMESTAMP = 57357
const TABLE = 57358
const UNIQUE = 57359
const INDEX = 57360
const ON = 57361
const ALTER = 57362
const ADD = 57363
const RENAME = 57364
const TO = 57365
const COLUMN = 57366
const PRIMARY = 57367
const KEY = 57368
const BEGIN = 57369
const TRANSACTION = 57370
const COMMIT = 57371
const ROLLBACK = 57372
const INSERT = 57373
const UPSERT = 57374
const INTO = 57375
const VALUES = 57376
const DELETE = 57377
const UPDATE = 57378
const SET = 57379
const CONFLICT = 57380
const DO = 57381
const NOTHING = 57382
const SELECT = 57383
const DISTINCT = 57384
const FROM = 57385
const JOIN = 57386
const HAVING = 57387
const WHERE = 57388
const GROUP = 57389
const BY = 57390
const LIMIT = 57391
const OFFSET = 57392
const ORDER = 57393
const ASC = 57394
const DESC = 57395
const AS = 57396
const UNION = 57397
const ALL = 57398
const NOT = 57399
const LIKE = 57400
const IF = 57401
const EXISTS = 57402
const IN = 57403
const IS = 57404
const AUTO_INCREMENT = 57405
const NULL = 57406
const CAST = 57407
const SCAST = 57408
const NPARAM = 57409
const PPARAM = 57410
const JOINTYPE = 57411
const LOP = 57412
const CMPOP = 57413
const IDENTIFIER = 57414
const TYPE = 57415
const INTEGER = 57416
const FLOAT = 57417
const VARCHAR = 57418
const BOOLEAN = 57419
const BLOB = 57420
const AGGREGATE_FUNC = 57421
const ERROR = 57422
const DOT = 57423
const STMT_SEPARATOR = 57424

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"CREATE",
	"DROP",
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
	"SCAST",
	"NPARAM",
	"PPARAM",
	"JOINTYPE",
	"LOP",
	"CMPOP",
	"IDENTIFIER",
	"TYPE",
	"INTEGER",
	"FLOAT",
	"VARCHAR",
	"BOOLEAN",
	"BLOB",
	"AGGREGATE_FUNC",
	"ERROR",
	"DOT",
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

var yyExca = [...]int16{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 79,
	58, 143,
	61, 143,
	-2, 131,
	-1, 200,
	44, 107,
	-2, 102,
	-1, 230,
	44, 107,
	-2, 104,
}

const yyPrivate = 57344

const yyLast = 394

var yyAct = [...]int16{
	78, 310, 65, 194, 149, 223, 248, 252, 93, 146,
	155, 184, 229, 113, 247, 105, 185, 108, 166, 48,
	84, 19, 282, 239, 6, 238, 192, 217, 192, 292,
	192, 192, 286, 192, 287, 268, 266, 81, 240, 218,
	83, 193, 283, 269, 96, 92, 267, 94, 95, 253,
	159, 234, 97, 131, 87, 88, 89, 90, 91, 66,
	64, 129, 130, 215, 82, 216, 254, 157, 249, 86,
	213, 131, 214, 206, 125, 126, 128, 127, 124, 129,
	130, 182, 135, 136, 110, 205, 191, 138, 131, 210,
	140, 117, 125, 126, 128, 127, 129, 130, 265, 140,
	168, 141, 139, 137, 119, 116, 104, 151, 103, 125,
	126, 128, 127, 131, 148, 131, 21, 272, 106, 163,
	158, 131, 130, 152, 309, 303, 170, 171, 172, 173,
	174, 175, 271, 160, 125, 126, 128, 127, 128, 127,
	183, 186, 125, 126, 128, 127, 67, 217, 207, 192,
	67, 187, 112, 66, 153, 199, 117, 197, 67, 62,
	200, 188, 181, 162, 76, 66, 264, 28, 29, 115,
	244, 198, 203, 208, 204, 235, 201, 180, 209, 212,
	271, 67, 202, 147, 246, 221, 109, 114, 190, 189,
	167, 169, 164, 161, 120, 99, 225, 70, 68, 37,
	54, 227, 52, 47, 154, 232, 263, 281, 134, 211,
	280, 43, 177, 262, 186, 233, 98, 133, 245, 176,
	241, 131, 167, 118, 178, 251, 236, 179, 69, 60,
	243, 38, 27, 255, 311, 312, 295, 250, 242, 122,
	123, 224, 195, 257, 256, 42, 302, 290, 259, 276,
	186, 106, 289, 258, 111, 35, 40, 19, 300, 293,
	285, 273, 58, 222, 274, 220, 77, 158, 278, 277,
	44, 45, 34, 33, 22, 260, 144, 219, 284, 143,
	142, 2, 291, 299, 226, 102, 23, 121, 71, 53,
	296, 196, 72, 298, 32, 24, 26, 25, 301, 46,
	304, 100, 101, 41, 150, 307, 308, 305, 20, 30,
	81, 31, 313, 83, 270, 314, 107, 96, 92, 132,
	94, 95, 75, 74, 261, 97, 279, 87, 88, 89,
	90, 91, 66, 81, 50, 51, 83, 82, 294, 306,
	96, 92, 86, 94, 95, 10, 12, 11, 97, 156,
	87, 88, 89, 90, 91, 66, 237, 275, 80, 79,
	82, 13, 288, 231, 230, 86, 228, 36, 7, 73,
	8, 9, 14, 15, 49, 59, 16, 17, 39, 63,
	61, 85, 19, 55, 56, 57, 297, 145, 165, 18,
	5, 4, 3, 1,
}

var yyPact = [...]int16{
	341, -1000, -1000, 28, -1000, -1000, -1000, 246, -1000, -1000,
	279, 160, 293, 278, 240, 239, 212, 127, 176, 214,
	-1000, 341, -1000, 152, 152, 152, 281, -1000, 131, 325,
	130, 270, 128, 127, 127, 127, 225, -1000, 173, 74,
	-1000, -1000, 126, 171, 125, 269, 152, -1000, -1000, 311,
	253, 253, -1000, 123, 280, 19, 17, 205, 114, 216,
	-1000, 211, -1000, 70, 115, -1000, 16, 75, -1000, 163,
	15, 122, 268, -1000, 253, 253, -1000, 276, 26, 151,
	-1000, 276, 276, 14, -1000, -1000, 276, -1000, -1000, -1000,
	-1000, -1000, 13, -1000, -1000, -1000, -1000, 10, -1000, 12,
	256, 255, 252, 111, 111, 298, 276, 72, -1000, 133,
	-1000, -22, 86, -1000, -1000, 121, 78, 120, -1000, 118,
	11, 119, -1000, -1000, 26, 276, 276, 276, 276, 276,
	276, 155, 166, 104, -1000, 51, 53, 216, -9, 276,
	276, 111, 118, 117, 116, -4, 67, -1000, -49, 193,
	273, 26, 298, 114, 276, 298, 325, 216, 115, 1,
	115, -1000, -5, -17, -1000, 66, -1000, 100, 111, 0,
	53, 53, 159, 159, 51, 59, -1000, 145, 276, -19,
	-1000, -18, -1000, 9, -25, 65, 26, -51, -1000, 254,
	-1000, 231, 113, 229, 191, 276, 265, 193, -1000, 26,
	136, 115, -39, -1000, -1000, -1000, -1000, 150, -66, -52,
	111, -1000, 51, -20, -1000, 97, -1000, 276, -1000, 112,
	-21, -1000, -21, -1000, 276, 26, -23, 191, 205, -1000,
	136, 209, -1000, -1000, 115, 249, -1000, 149, 92, 24,
	-1000, -54, -44, -55, -47, 26, -1000, 98, -1000, 276,
	50, 26, -1000, -1000, 111, -1000, 202, -1000, -22, -1000,
	-23, 147, -1000, 143, -70, -48, -1000, -1000, -1000, -1000,
	-1000, -21, 222, -58, -56, 207, 199, 298, -61, -1000,
	-1000, -1000, -1000, -1000, -1000, 220, -1000, -1000, 185, 276,
	109, 264, -1000, 218, 193, 198, 26, 43, -1000, 276,
	-1000, 191, 109, 109, 26, -1000, 42, 182, -1000, 109,
	-1000, -1000, -1000, 182, -1000,
}

var yyPgo = [...]int16{
	0, 393, 281, 392, 391, 390, 24, 389, 388, 18,
	9, 7, 387, 386, 14, 6, 16, 11, 381, 8,
	20, 380, 379, 2, 378, 375, 10, 349, 19, 374,
	369, 164, 366, 12, 364, 363, 0, 15, 362, 359,
	358, 357, 3, 5, 356, 13, 339, 338, 1, 4,
	245, 326, 324, 319, 17, 316, 314, 308,
}

var yyR1 = [...]int8{
	0, 1, 2, 2, 57, 57, 3, 3, 3, 4,
	4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
	4, 4, 4, 4, 4, 50, 50, 11, 11, 5,
	5, 5, 5, 56, 56, 55, 55, 54, 12, 12,
	14, 14, 15, 10, 10, 13, 13, 17, 17, 16,
	16, 18, 18, 18, 18, 18, 18, 18, 18, 18,
	18, 19, 8, 8, 9, 44, 44, 44, 51, 51,
	52, 52, 52, 6, 6, 7, 25, 25, 24, 24,
	21, 21, 22, 22, 20, 20, 20, 23, 23, 26,
	26, 26, 27, 28, 29, 29, 29, 30, 30, 30,
	31, 31, 32, 32, 33, 33, 34, 35, 35, 37,
	37, 41, 41, 38, 38, 42, 42, 43, 43, 47,
	47, 49, 49, 46, 46, 48, 48, 48, 45, 45,
	45, 36, 36, 36, 36, 36, 36, 36, 36, 39,
	39, 39, 39, 53, 53, 40, 40, 40, 40, 40,
	40, 40, 40,
}

var yyR2 = [...]int8{
	0, 1, 2, 3, 0, 1, 1, 1, 1, 2,
	1, 1, 1, 4, 2, 3, 3, 11, 3, 8,
	9, 7, 6, 8, 6, 0, 3, 1, 3, 9,
	8, 7, 8, 0, 4, 1, 3, 3, 0, 1,
	1, 3, 3, 1, 3, 1, 3, 0, 1, 1,
	3, 1, 1, 1, 1, 1, 6, 1, 1, 1,
	1, 4, 1, 3, 5, 0, 3, 3, 0, 1,
	0, 1, 2, 1, 4, 13, 0, 1, 0, 1,
	1, 1, 2, 4, 1, 4, 4, 1, 3, 3,
	4, 2, 1, 2, 0, 2, 2, 0, 2, 2,
	2, 1, 0, 1, 1, 2, 6, 0, 1, 0,
	2, 0, 3, 0, 2, 0, 2, 0, 2, 0,
	3, 0, 4, 2, 4, 0, 1, 1, 0, 1,
	2, 1, 1, 2, 2, 4, 4, 6, 6, 1,
	1, 3, 3, 0, 1, 3, 3, 3, 3, 3,
	3, 3, 4,
}

var yyChk = [...]int16{
	-1000, -1, -2, -3, -4, -5, -6, 27, 29, 30,
	4, 6, 5, 20, 31, 32, 35, 36, -7, 41,
	-57, 88, 28, 7, 16, 18, 17, 72, 7, 8,
	16, 18, 16, 33, 33, 43, -27, 72, 55, -24,
	42, -2, -50, 59, -50, -50, 18, 72, -28, -29,
	9, 10, 72, 19, 72, -27, -27, -27, 37, -25,
	56, -21, 85, -22, -20, -23, 79, 72, 72, 57,
	72, 19, -50, -30, 12, 11, -31, 13, -36, -39,
	-40, 57, 84, 60, -20, -18, 89, 74, 75, 76,
	77, 78, 65, -19, 67, 68, 64, 72, -31, 72,
	21, 22, 5, 89, 89, -37, 46, -55, -54, 72,
	-6, 43, 82, -45, 72, 54, 89, 81, 60, 89,
	72, 19, -31, -31, -36, 83, 84, 86, 85, 70,
	71, 62, -53, 66, 57, -36, -36, 89, -36, 89,
	89, 89, 24, 24, 24, -12, -10, 72, -10, -49,
	6, -36, -37, 82, 71, -26, -27, 89, -19, 72,
	-20, 72, 85, -23, 72, -8, -9, 72, 89, 72,
	-36, -36, -36, -36, -36, -36, 64, 57, 58, 61,
	73, -6, 90, -36, -17, -16, -36, -10, -9, 72,
	72, 90, 82, 90, -42, 49, 18, -49, -54, -36,
	-49, -28, -6, -45, -45, 90, 90, 82, 73, -10,
	89, 64, -36, 89, 90, 54, 90, 82, 90, 23,
	34, 72, 34, -43, 50, -36, 19, -42, -32, -33,
	-34, -35, 69, -45, 90, 25, -9, -44, 91, 89,
	90, -10, -6, -16, 73, -36, 72, -14, -15, 89,
	-14, -36, -11, 72, 89, -43, -37, -33, 44, -45,
	26, -52, 64, 57, 74, 74, 90, 90, 90, 90,
	-56, 82, 19, -17, -10, -41, 47, -26, -11, -51,
	63, 64, 92, 90, -15, 38, 90, 90, -38, 45,
	48, -49, 90, 39, -47, 51, -36, -13, -23, 19,
	40, -42, 48, 82, -36, -43, -46, -23, -23, 82,
	-48, 52, 53, -23, -48,
}

var yyDef = [...]int16{
	0, -2, 1, 4, 6, 7, 8, 10, 11, 12,
	0, 0, 0, 0, 0, 0, 0, 0, 73, 78,
	2, 5, 9, 25, 25, 25, 0, 14, 0, 94,
	0, 0, 0, 0, 0, 0, 0, 92, 76, 0,
	79, 3, 0, 0, 0, 0, 25, 15, 16, 97,
	0, 0, 18, 0, 0, 0, 0, 109, 0, 0,
	77, 0, 80, 81, 128, 84, 0, 87, 13, 0,
	0, 0, 0, 93, 0, 0, 95, 0, 101, -2,
	132, 0, 0, 0, 139, 140, 0, 51, 52, 53,
	54, 55, 0, 57, 58, 59, 60, 87, 96, 0,
	0, 0, 0, 38, 0, 121, 0, 109, 35, 0,
	74, 0, 0, 82, 129, 0, 0, 0, 26, 0,
	0, 0, 98, 99, 100, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 144, 133, 134, 0, 0, 0,
	47, 0, 0, 0, 0, 0, 39, 43, 0, 115,
	0, 110, 121, 0, 0, 121, 94, 0, 128, 92,
	128, 130, 0, 0, 88, 0, 62, 0, 0, 0,
	145, 146, 147, 148, 149, 150, 151, 0, 0, 0,
	142, 0, 141, 0, 0, 48, 49, 0, 22, 0,
	24, 0, 0, 0, 117, 0, 0, 115, 36, 37,
	-2, 128, 0, 91, 83, 85, 86, 0, 65, 0,
	0, 152, 135, 0, 136, 0, 61, 0, 21, 0,
	0, 44, 0, 31, 0, 116, 0, 117, 109, 103,
	-2, 0, 108, 89, 128, 0, 63, 70, 0, 0,
	19, 0, 0, 0, 0, 50, 23, 33, 40, 47,
	30, 118, 122, 27, 0, 32, 111, 105, 0, 90,
	0, 68, 71, 0, 0, 0, 20, 137, 138, 56,
	29, 0, 0, 0, 0, 113, 0, 121, 0, 64,
	69, 72, 66, 67, 41, 0, 42, 28, 119, 0,
	0, 0, 17, 0, 115, 0, 114, 112, 45, 0,
	34, 117, 0, 0, 106, 75, 120, 125, 46, 0,
	123, 126, 127, 125, 124,
}

var yyTok1 = [...]int8{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	89, 90, 85, 83, 82, 84, 87, 86, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 91, 3, 92,
}

var yyTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
	62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
	88,
}

var yyTok3 = [...]int8{
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
	base := int(yyPact[state])
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && int(yyChk[int(yyAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || int(yyExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := int(yyExca[i])
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
		token = int(yyTok1[0])
		goto out
	}
	if char < len(yyTok1) {
		token = int(yyTok1[char])
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = int(yyTok2[char-yyPrivate])
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = int(yyTok3[i+0])
		if token == char {
			token = int(yyTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(yyTok2[1]) /* unknown char */
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
	yyn = int(yyPact[yystate])
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
	yyn = int(yyAct[yyn])
	if int(yyChk[yyn]) == yytoken { /* valid shift */
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
	yyn = int(yyDef[yystate])
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && int(yyExca[xi+1]) == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = int(yyExca[xi+0])
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = int(yyExca[xi+1])
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
				yyn = int(yyPact[yyS[yyp].yys]) + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = int(yyAct[yyn]) /* simulate a shift of "error" */
					if int(yyChk[yystate]) == yyErrCode {
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

	yyp -= int(yyR2[yyn])
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = int(yyR1[yyn])
	yyg := int(yyPgo[yyn])
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = int(yyAct[yyg])
	} else {
		yystate = int(yyAct[yyj])
		if int(yyChk[yystate]) != -yyn {
			yystate = int(yyAct[yyg])
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
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &DropTableStmt{table: yyDollar[3].id}
		}
	case 19:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[5].id, cols: yyDollar[7].ids}
		}
	case 20:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{unique: true, ifNotExists: yyDollar[4].boolean, table: yyDollar[6].id, cols: yyDollar[8].ids}
		}
	case 21:
		yyDollar = yyS[yypt-7 : yypt+1]
		{
			yyVAL.stmt = &DropIndexStmt{table: yyDollar[4].id, cols: yyDollar[6].ids}
		}
	case 22:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &AddColumnStmt{table: yyDollar[3].id, colSpec: yyDollar[6].colSpec}
		}
	case 23:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &RenameColumnStmt{table: yyDollar[3].id, oldName: yyDollar[6].id, newName: yyDollar[8].id}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &DropColumnStmt{table: yyDollar[3].id, colName: yyDollar[6].id}
		}
	case 25:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 26:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 27:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 28:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = yyDollar[2].ids
		}
	case 29:
		yyDollar = yyS[yypt-9 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{isInsert: true, tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows, onConflict: yyDollar[9].onConflict}
		}
	case 30:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows}
		}
	case 31:
		yyDollar = yyS[yypt-7 : yypt+1]
		{
			yyVAL.stmt = &DeleteFromStmt{tableRef: yyDollar[3].tableRef, where: yyDollar[4].exp, indexOn: yyDollar[5].ids, limit: yyDollar[6].exp, offset: yyDollar[7].exp}
		}
	case 32:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpdateStmt{tableRef: yyDollar[2].tableRef, updates: yyDollar[4].updates, where: yyDollar[5].exp, indexOn: yyDollar[6].ids, limit: yyDollar[7].exp, offset: yyDollar[8].exp}
		}
	case 33:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.onConflict = nil
		}
	case 34:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.onConflict = &OnConflictDo{}
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.updates = []*colUpdate{yyDollar[1].update}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.updates = append(yyDollar[1].updates, yyDollar[3].update)
		}
	case 37:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.update = &colUpdate{col: yyDollar[1].id, op: yyDollar[2].cmpOp, val: yyDollar[3].exp}
		}
	case 38:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = yyDollar[1].ids
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.rows = []*RowSpec{yyDollar[1].row}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.rows = append(yyDollar[1].rows, yyDollar[3].row)
		}
	case 42:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.row = &RowSpec{Values: yyDollar[2].values}
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 44:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = append(yyDollar[1].ids, yyDollar[3].id)
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.cols = []*ColSelector{yyDollar[1].col}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 47:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.values = nil
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = yyDollar[1].values
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = []ValueExp{yyDollar[1].exp}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].exp)
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Integer{val: int64(yyDollar[1].integer)}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Float64{val: float64(yyDollar[1].float)}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Varchar{val: yyDollar[1].str}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Bool{val: yyDollar[1].boolean}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Blob{val: yyDollar[1].blob}
		}
	case 56:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.value = &Cast{val: yyDollar[3].exp, t: yyDollar[5].sqlType}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = yyDollar[1].value
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: yyDollar[1].id}
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: fmt.Sprintf("param%d", yyDollar[1].pparam), pos: yyDollar[1].pparam}
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &NullValue{t: AnyType}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.value = &FnCall{fn: yyDollar[1].id, params: yyDollar[3].values}
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.colsSpec = []*ColSpec{yyDollar[1].colSpec}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.colsSpec = append(yyDollar[1].colsSpec, yyDollar[3].colSpec)
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.colSpec = &ColSpec{colName: yyDollar[1].id, colType: yyDollar[2].sqlType, maxLen: int(yyDollar[3].integer), notNull: yyDollar[4].boolean, autoIncrement: yyDollar[5].boolean}
		}
	case 65:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.integer = 0
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.integer = yyDollar[2].integer
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.integer = yyDollar[2].integer
		}
	case 68:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 70:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = yyDollar[1].stmt
		}
	case 74:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.stmt = &UnionStmt{
				distinct: yyDollar[3].distinct,
				left:     yyDollar[1].stmt.(DataSource),
				right:    yyDollar[4].stmt.(DataSource),
			}
		}
	case 75:
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
	case 76:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = nil
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = yyDollar[1].sels
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyDollar[1].sel.setAlias(yyDollar[2].id)
			yyVAL.sels = []Selector{yyDollar[1].sel}
		}
	case 83:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[3].sel.setAlias(yyDollar[4].id)
			yyVAL.sels = append(yyDollar[1].sels, yyDollar[3].sel)
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sel = yyDollar[1].col
		}
	case 85:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, col: "*"}
		}
	case 86:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, table: yyDollar[3].col.table, col: yyDollar[3].col.col}
		}
	case 87:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.col = &ColSelector{col: yyDollar[1].id}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.col = &ColSelector{table: yyDollar[1].id, col: yyDollar[3].id}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyDollar[1].tableRef.period = yyDollar[2].period
			yyDollar[1].tableRef.as = yyDollar[3].id
			yyVAL.ds = yyDollar[1].tableRef
		}
	case 90:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[2].stmt.(*SelectStmt).as = yyDollar[4].id
			yyVAL.ds = yyDollar[2].stmt.(DataSource)
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ds = &FnDataSourceStmt{fnCall: yyDollar[1].value.(*FnCall), as: yyDollar[2].id}
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{table: yyDollar[1].id}
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.period = period{start: yyDollar[1].openPeriod, end: yyDollar[2].openPeriod}
		}
	case 94:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.openPeriod = nil
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{inclusive: true, instant: yyDollar[2].periodInstant}
		}
	case 96:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{instant: yyDollar[2].periodInstant}
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.openPeriod = nil
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{inclusive: true, instant: yyDollar[2].periodInstant}
		}
	case 99:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.openPeriod = &openPeriod{instant: yyDollar[2].periodInstant}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.periodInstant = periodInstant{instantType: txInstant, exp: yyDollar[2].exp}
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.periodInstant = periodInstant{instantType: timeInstant, exp: yyDollar[1].exp}
		}
	case 102:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joins = nil
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = yyDollar[1].joins
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = []*JoinSpec{yyDollar[1].join}
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.joins = append([]*JoinSpec{yyDollar[1].join}, yyDollar[2].joins...)
		}
	case 106:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.join = &JoinSpec{joinType: yyDollar[1].joinType, ds: yyDollar[3].ds, indexOn: yyDollar[4].ids, cond: yyDollar[6].exp}
		}
	case 107:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joinType = InnerJoin
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joinType = yyDollar[1].joinType
		}
	case 109:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 110:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 111:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.cols = nil
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = yyDollar[3].cols
		}
	case 113:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 115:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 117:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 119:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ordcols = nil
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ordcols = yyDollar[3].ordcols
		}
	case 121:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 122:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ids = yyDollar[4].ids
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ordcols = []*OrdCol{{sel: yyDollar[1].col, descOrder: yyDollar[2].opt_ord}}
		}
	case 124:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ordcols = append(yyDollar[1].ordcols, &OrdCol{sel: yyDollar[3].col, descOrder: yyDollar[4].opt_ord})
		}
	case 125:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = false
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = true
		}
	case 128:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.id = ""
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.id = yyDollar[1].id
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.id = yyDollar[2].id
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].exp
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].binExp
		}
	case 133:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NotBoolExp{exp: yyDollar[2].exp}
		}
	case 134:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NumExp{left: &Integer{val: 0}, op: SUBSOP, right: yyDollar[2].exp}
		}
	case 135:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &LikeBoolExp{val: yyDollar[1].exp, notLike: yyDollar[2].boolean, pattern: yyDollar[4].exp}
		}
	case 136:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &ExistsBoolExp{q: (yyDollar[3].stmt).(DataSource)}
		}
	case 137:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InSubQueryExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, q: yyDollar[5].stmt.(*SelectStmt)}
		}
	case 138:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.exp = &InListExp{val: yyDollar[1].exp, notIn: yyDollar[2].boolean, values: yyDollar[5].values}
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].sel
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].value
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = &Cast{val: yyDollar[1].exp, t: yyDollar[3].sqlType}
		}
	case 143:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: ADDOP, right: yyDollar[3].exp}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: SUBSOP, right: yyDollar[3].exp}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: DIVOP, right: yyDollar[3].exp}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: MULTOP, right: yyDollar[3].exp}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &BinBoolExp{left: yyDollar[1].exp, op: yyDollar[2].logicOp, right: yyDollar[3].exp}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: yyDollar[2].cmpOp, right: yyDollar[3].exp}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: EQ, right: &NullValue{t: AnyType}}
		}
	case 152:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: NE, right: &NullValue{t: AnyType}}
		}
	}
	goto yystack /* stack new state and value */
}
