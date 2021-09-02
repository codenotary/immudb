// Code generated by goyacc -l -o sql_parser.go sql_grammar.y. DO NOT EDIT.
package sql

import __yyfmt__ "fmt"

import "fmt"

func setResult(l yyLexer, stmts []SQLStmt) {
	l.(*lexer).result = stmts
}

type yySymType struct {
	yys      int
	stmts    []SQLStmt
	stmt     SQLStmt
	colsSpec []*ColSpec
	colSpec  *ColSpec
	cols     []*ColSelector
	rows     []*RowSpec
	row      *RowSpec
	values   []ValueExp
	value    ValueExp
	id       string
	number   uint64
	str      string
	boolean  bool
	blob     []byte
	sqlType  SQLValueType
	aggFn    AggregateFn
	ids      []string
	col      *ColSelector
	sel      Selector
	sels     []Selector
	distinct bool
	ds       DataSource
	tableRef *tableRef
	joins    []*JoinSpec
	join     *JoinSpec
	joinType JoinType
	boolExp  ValueExp
	binExp   ValueExp
	err      error
	ordcols  []*OrdCol
	opt_ord  Order
	logicOp  LogicOperator
	cmpOp    CmpOperator
	pparam   int
}

const CREATE = 57346
const USE = 57347
const DATABASE = 57348
const SNAPSHOT = 57349
const SINCE = 57350
const UP = 57351
const TO = 57352
const TABLE = 57353
const UNIQUE = 57354
const INDEX = 57355
const ON = 57356
const ALTER = 57357
const ADD = 57358
const COLUMN = 57359
const PRIMARY = 57360
const KEY = 57361
const BEGIN = 57362
const TRANSACTION = 57363
const COMMIT = 57364
const INSERT = 57365
const UPSERT = 57366
const INTO = 57367
const VALUES = 57368
const SELECT = 57369
const DISTINCT = 57370
const FROM = 57371
const BEFORE = 57372
const TX = 57373
const JOIN = 57374
const HAVING = 57375
const WHERE = 57376
const GROUP = 57377
const BY = 57378
const LIMIT = 57379
const ORDER = 57380
const ASC = 57381
const DESC = 57382
const AS = 57383
const NOT = 57384
const LIKE = 57385
const IF = 57386
const EXISTS = 57387
const AUTO_INCREMENT = 57388
const NULL = 57389
const NPARAM = 57390
const PPARAM = 57391
const JOINTYPE = 57392
const LOP = 57393
const CMPOP = 57394
const IDENTIFIER = 57395
const TYPE = 57396
const NUMBER = 57397
const VARCHAR = 57398
const BOOLEAN = 57399
const BLOB = 57400
const AGGREGATE_FUNC = 57401
const ERROR = 57402
const STMT_SEPARATOR = 57403

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"CREATE",
	"USE",
	"DATABASE",
	"SNAPSHOT",
	"SINCE",
	"UP",
	"TO",
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
	"INSERT",
	"UPSERT",
	"INTO",
	"VALUES",
	"SELECT",
	"DISTINCT",
	"FROM",
	"BEFORE",
	"TX",
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
	"AUTO_INCREMENT",
	"NULL",
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
}

const yyPrivate = 57344

const yyLast = 281

var yyAct = [...]int{
	89, 227, 38, 58, 130, 128, 152, 4, 151, 103,
	74, 66, 94, 132, 75, 203, 135, 110, 143, 141,
	142, 149, 40, 110, 140, 234, 136, 137, 138, 139,
	39, 222, 212, 206, 133, 162, 163, 211, 79, 134,
	55, 205, 110, 110, 50, 52, 158, 159, 161, 160,
	150, 121, 110, 193, 162, 163, 110, 195, 172, 163,
	111, 198, 117, 80, 109, 158, 159, 161, 160, 158,
	159, 161, 160, 92, 51, 61, 199, 169, 18, 100,
	230, 99, 169, 153, 98, 105, 168, 106, 84, 76,
	82, 97, 158, 159, 161, 160, 72, 70, 108, 60,
	16, 161, 160, 226, 101, 71, 61, 122, 114, 116,
	40, 210, 177, 119, 57, 129, 39, 192, 32, 40,
	145, 35, 143, 141, 142, 39, 144, 5, 180, 147,
	136, 137, 138, 139, 154, 224, 37, 176, 165, 166,
	167, 107, 87, 146, 120, 7, 90, 33, 40, 170,
	124, 118, 104, 91, 85, 81, 78, 64, 179, 62,
	185, 51, 183, 49, 186, 187, 188, 189, 190, 191,
	46, 51, 41, 77, 73, 202, 194, 196, 104, 96,
	201, 214, 175, 33, 204, 83, 43, 164, 63, 228,
	229, 182, 59, 217, 209, 157, 127, 113, 15, 156,
	213, 115, 86, 17, 68, 10, 11, 67, 56, 21,
	7, 215, 220, 221, 125, 123, 12, 30, 29, 53,
	223, 6, 173, 19, 13, 14, 10, 11, 7, 231,
	88, 232, 54, 233, 2, 69, 22, 12, 225, 171,
	65, 23, 25, 24, 218, 13, 14, 44, 45, 28,
	48, 31, 26, 27, 208, 200, 174, 42, 207, 181,
	219, 148, 216, 126, 131, 155, 112, 95, 93, 47,
	20, 36, 34, 178, 184, 197, 102, 9, 8, 3,
	1,
}

var yyPact = [...]int{
	201, -1000, -1000, 33, 11, -1000, 202, 181, -1000, -1000,
	230, 246, 238, 193, 192, -1000, 201, -1000, -1000, 222,
	57, -1000, 119, 142, 233, 235, 117, 242, 110, 108,
	108, -1000, 197, -27, 179, -1000, 53, 151, -1000, 31,
	40, -1000, 106, 146, 104, 226, -1000, 177, 173, 219,
	29, 39, 28, -1000, -1000, 222, 21, 66, -1000, 103,
	-31, 102, 22, 140, 20, 101, -1000, 171, 87, 213,
	93, 100, 93, -1000, 129, -1000, 118, 151, -1000, -1000,
	10, 38, 99, -1000, 93, 19, 86, -1000, 99, -5,
	-1000, -1000, -9, 163, -1000, 129, 169, 177, -7, -1000,
	-1000, 98, 52, -1000, 90, -18, 93, -1000, -1000, 189,
	97, 188, 161, -29, -1000, 21, 151, -1000, -1000, 125,
	-49, -1000, -19, 15, -1000, 15, 166, 159, 3, 144,
	-1000, -1000, -29, -29, -29, 18, -1000, -1000, -1000, -1000,
	9, 96, -1000, -1000, 225, -11, 203, -1000, 136, 82,
	-1000, 51, -1000, 75, 51, 153, -29, 95, -29, -29,
	-29, -29, -29, -29, 61, 7, 37, -16, 183, -12,
	-1000, -29, -1000, 8, 133, -1000, -56, 15, -28, -1000,
	14, 249, 158, 3, 50, -1000, 37, 37, -1000, -1000,
	7, 30, -1000, -1000, -32, -1000, 3, -37, -1000, 93,
	-1000, -1000, 134, -1000, -1000, -1000, 75, 156, 231, 95,
	95, -1000, -1000, -38, -1000, -1000, 151, 80, 224, 42,
	150, -1000, -1000, -1000, -1000, 12, 95, -1000, -1000, -1000,
	93, 150, -44, -1000, -1000,
}

var yyPgo = [...]int{
	0, 280, 234, 118, 279, 127, 278, 277, 7, 276,
	9, 0, 275, 274, 8, 6, 273, 4, 115, 272,
	271, 2, 270, 10, 14, 269, 11, 268, 12, 267,
	5, 266, 265, 264, 263, 262, 261, 3, 260, 259,
	1, 258, 257, 256, 255, 198,
}

var yyR1 = [...]int{
	0, 1, 2, 2, 2, 45, 45, 4, 4, 5,
	5, 3, 3, 6, 6, 6, 6, 6, 6, 6,
	25, 25, 42, 42, 12, 12, 7, 7, 14, 14,
	15, 11, 11, 13, 13, 16, 16, 17, 17, 17,
	17, 17, 17, 17, 17, 9, 9, 10, 36, 36,
	43, 43, 44, 44, 44, 8, 22, 22, 19, 19,
	20, 20, 18, 18, 18, 21, 21, 21, 23, 23,
	23, 24, 24, 26, 26, 27, 27, 28, 28, 29,
	31, 31, 34, 34, 32, 32, 35, 35, 39, 39,
	41, 41, 38, 38, 40, 40, 40, 37, 37, 30,
	30, 30, 30, 30, 30, 30, 30, 33, 33, 33,
	33, 33, 33,
}

var yyR2 = [...]int{
	0, 1, 2, 2, 3, 0, 1, 1, 4, 1,
	1, 2, 3, 3, 3, 4, 11, 7, 8, 6,
	0, 3, 0, 3, 1, 3, 8, 8, 1, 3,
	3, 1, 3, 1, 3, 1, 3, 1, 1, 1,
	1, 3, 2, 1, 1, 1, 3, 5, 0, 3,
	0, 1, 0, 1, 2, 13, 0, 1, 1, 1,
	2, 4, 1, 3, 4, 1, 3, 5, 1, 5,
	3, 1, 3, 0, 3, 0, 1, 1, 2, 5,
	0, 2, 0, 3, 0, 2, 0, 2, 0, 3,
	0, 6, 2, 4, 0, 1, 1, 0, 2, 1,
	1, 1, 2, 2, 3, 3, 4, 3, 3, 3,
	3, 3, 3,
}

var yyChk = [...]int{
	-1000, -1, -2, -4, -8, -5, 20, 27, -6, -7,
	4, 5, 15, 23, 24, -45, 67, -45, 67, 21,
	-22, 28, 6, 11, 13, 12, 6, 7, 11, 25,
	25, -2, -3, -5, -19, 64, -20, -18, -21, 59,
	53, 53, -42, 44, 14, 13, 53, -25, 8, 53,
	-24, 53, -24, 22, -45, 67, 29, 61, -37, 41,
	68, 66, 53, 42, 53, 14, -26, 30, 31, 16,
	68, 66, 68, -3, -23, -24, 68, -18, 53, 69,
	-21, 53, 68, 45, 68, 53, 31, 55, 17, -11,
	53, 53, -11, -27, -28, -29, 50, -24, -8, -37,
	69, 66, -9, -10, 53, -11, 68, 55, -10, 69,
	61, 69, -31, 34, -28, 32, -26, 69, 53, 61,
	54, 69, -11, 26, 53, 26, -34, 35, -30, -18,
	-17, -33, 42, 63, 68, 45, 55, 56, 57, 58,
	53, 48, 49, 47, -23, -37, 18, -10, -36, 70,
	69, -14, -15, 68, -14, -32, 33, 36, 62, 63,
	65, 64, 51, 52, 43, -30, -30, -30, 68, 68,
	53, 14, 69, 19, -43, 46, 55, 61, -16, -17,
	53, -39, 38, -30, -13, -21, -30, -30, -30, -30,
	-30, -30, 56, 69, -8, 69, -30, -12, 53, 68,
	-44, 47, 42, 71, -15, 69, 61, -41, 5, 36,
	61, 69, 69, -11, 47, -17, -35, 37, 13, -38,
	-21, -21, 69, -37, 55, 14, 61, -40, 39, 40,
	68, -21, -11, -40, 69,
}

var yyDef = [...]int{
	0, -2, 1, 5, 5, 7, 0, 56, 9, 10,
	0, 0, 0, 0, 0, 2, 6, 3, 6, 0,
	0, 57, 0, 22, 0, 0, 0, 20, 0, 0,
	0, 4, 0, 5, 0, 58, 59, 97, 62, 0,
	65, 13, 0, 0, 0, 0, 14, 73, 0, 0,
	0, 71, 0, 8, 11, 6, 0, 0, 60, 0,
	0, 0, 0, 0, 0, 0, 15, 0, 0, 0,
	0, 0, 0, 12, 75, 68, 0, 97, 98, 63,
	0, 66, 0, 23, 0, 0, 0, 21, 0, 0,
	31, 72, 0, 80, 76, 77, 0, 73, 0, 61,
	64, 0, 0, 45, 0, 0, 0, 74, 19, 0,
	0, 0, 82, 0, 78, 0, 97, 70, 67, 0,
	48, 17, 0, 0, 32, 0, 84, 0, 81, 99,
	100, 101, 0, 0, 0, 0, 37, 38, 39, 40,
	65, 0, 43, 44, 0, 0, 0, 46, 50, 0,
	18, 26, 28, 0, 27, 88, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 102, 103, 0, 0, 0,
	42, 0, 69, 0, 52, 51, 0, 0, 0, 35,
	0, 90, 0, 85, 83, 33, 107, 108, 109, 110,
	111, 112, 105, 104, 0, 41, 79, 0, 24, 0,
	47, 53, 0, 49, 29, 30, 0, 86, 0, 0,
	0, 106, 16, 0, 54, 36, 97, 0, 0, 89,
	94, 34, 25, 55, 87, 0, 0, 92, 95, 96,
	0, 94, 0, 93, 91,
}

var yyTok1 = [...]int{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	68, 69, 64, 62, 61, 63, 66, 65, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 70, 3, 71,
}

var yyTok2 = [...]int{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
	52, 53, 54, 55, 56, 57, 58, 59, 60, 67,
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
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.stmts = []SQLStmt{yyDollar[1].stmt}
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmts = append([]SQLStmt{yyDollar[1].stmt}, yyDollar[3].stmts...)
		}
	case 5:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
		}
	case 7:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.stmt = yyDollar[1].stmt
		}
	case 8:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.stmt = &TxStmt{stmts: yyDollar[3].stmts}
		}
	case 11:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.stmts = []SQLStmt{yyDollar[1].stmt}
		}
	case 12:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmts = append([]SQLStmt{yyDollar[1].stmt}, yyDollar[3].stmts...)
		}
	case 13:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &CreateDatabaseStmt{DB: yyDollar[3].id}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.stmt = &UseDatabaseStmt{DB: yyDollar[3].id}
		}
	case 15:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.stmt = &UseSnapshotStmt{sinceTx: yyDollar[3].number, asBefore: yyDollar[4].number}
		}
	case 16:
		yyDollar = yyS[yypt-11 : yypt+1]
		{
			yyVAL.stmt = &CreateTableStmt{ifNotExists: yyDollar[3].boolean, table: yyDollar[4].id, colsSpec: yyDollar[6].colsSpec, pkColNames: yyDollar[10].ids}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{table: yyDollar[4].id, cols: yyDollar[6].ids}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &CreateIndexStmt{unique: true, table: yyDollar[5].id, cols: yyDollar[7].ids}
		}
	case 19:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.stmt = &AddColumnStmt{table: yyDollar[3].id, colSpec: yyDollar[6].colSpec}
		}
	case 20:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 21:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[3].number
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
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{isInsert: true, tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows}
		}
	case 27:
		yyDollar = yyS[yypt-8 : yypt+1]
		{
			yyVAL.stmt = &UpsertIntoStmt{tableRef: yyDollar[3].tableRef, cols: yyDollar[5].ids, rows: yyDollar[8].rows}
		}
	case 28:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.rows = []*RowSpec{yyDollar[1].row}
		}
	case 29:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.rows = append(yyDollar[1].rows, yyDollar[3].row)
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.row = &RowSpec{Values: yyDollar[2].values}
		}
	case 31:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = append(yyDollar[1].ids, yyDollar[3].id)
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.cols = []*ColSelector{yyDollar[1].col}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = []ValueExp{yyDollar[1].value}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].value)
		}
	case 37:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Number{val: yyDollar[1].number}
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Varchar{val: yyDollar[1].str}
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Bool{val: yyDollar[1].boolean}
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Blob{val: yyDollar[1].blob}
		}
	case 41:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.value = &SysFn{fn: yyDollar[1].id}
		}
	case 42:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.value = &Param{id: yyDollar[2].id}
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: fmt.Sprintf("param%d", yyDollar[1].pparam), pos: yyDollar[1].pparam}
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &NullValue{t: AnyType}
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.colsSpec = []*ColSpec{yyDollar[1].colSpec}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.colsSpec = append(yyDollar[1].colsSpec, yyDollar[3].colSpec)
		}
	case 47:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.colSpec = &ColSpec{colName: yyDollar[1].id, colType: yyDollar[2].sqlType, maxLen: int(yyDollar[3].number), autoIncrement: yyDollar[4].boolean, notNull: yyDollar[5].boolean}
		}
	case 48:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 50:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 55:
		yyDollar = yyS[yypt-13 : yypt+1]
		{
			yyVAL.stmt = &SelectStmt{
				distinct:  yyDollar[2].distinct,
				selectors: yyDollar[3].sels,
				ds:        yyDollar[5].ds,
				joins:     yyDollar[6].joins,
				where:     yyDollar[7].boolExp,
				groupBy:   yyDollar[8].cols,
				having:    yyDollar[9].boolExp,
				orderBy:   yyDollar[10].ordcols,
				indexOn:   yyDollar[11].ids,
				limit:     yyDollar[12].number,
				as:        yyDollar[13].id,
			}
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = nil
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = yyDollar[1].sels
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyDollar[1].sel.setAlias(yyDollar[2].id)
			yyVAL.sels = []Selector{yyDollar[1].sel}
		}
	case 61:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[3].sel.setAlias(yyDollar[4].id)
			yyVAL.sels = append(yyDollar[1].sels, yyDollar[3].sel)
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sel = yyDollar[1].col
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, col: "*"}
		}
	case 64:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, db: yyDollar[3].col.db, table: yyDollar[3].col.table, col: yyDollar[3].col.col}
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.col = &ColSelector{col: yyDollar[1].id}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.col = &ColSelector{table: yyDollar[1].id, col: yyDollar[3].id}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.col = &ColSelector{db: yyDollar[1].id, table: yyDollar[3].id, col: yyDollar[5].id}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ds = yyDollar[1].tableRef
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyDollar[2].tableRef.asBefore = yyDollar[3].number
			yyDollar[2].tableRef.as = yyDollar[4].id
			yyVAL.ds = yyDollar[2].tableRef
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ds = yyDollar[2].stmt.(*SelectStmt)
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{table: yyDollar[1].id}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{db: yyDollar[1].id, table: yyDollar[3].id}
		}
	case 73:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[3].number
		}
	case 75:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joins = nil
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = yyDollar[1].joins
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = []*JoinSpec{yyDollar[1].join}
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.joins = append([]*JoinSpec{yyDollar[1].join}, yyDollar[2].joins...)
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.join = &JoinSpec{joinType: yyDollar[1].joinType, ds: yyDollar[3].ds, cond: yyDollar[5].boolExp}
		}
	case 80:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolExp = nil
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[2].boolExp
		}
	case 82:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.cols = nil
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = yyDollar[3].cols
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolExp = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[2].boolExp
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 88:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ordcols = nil
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ordcols = yyDollar[3].ordcols
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 91:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.ids = yyDollar[5].ids
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ordcols = []*OrdCol{{sel: yyDollar[1].col, order: yyDollar[2].opt_ord}}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ordcols = append(yyDollar[1].ordcols, &OrdCol{sel: yyDollar[3].col, order: yyDollar[4].opt_ord})
		}
	case 94:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.opt_ord = AscOrder
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = AscOrder
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = DescOrder
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.id = ""
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.id = yyDollar[2].id
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[1].sel
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[1].value
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[1].binExp
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolExp = &NotBoolExp{exp: yyDollar[2].boolExp}
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolExp = &NumExp{left: &Number{val: uint64(0)}, op: SUBSOP, right: yyDollar[2].boolExp}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.boolExp = yyDollar[2].boolExp
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.boolExp = &LikeBoolExp{sel: yyDollar[1].sel, pattern: yyDollar[3].str}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.boolExp = &ExistsBoolExp{q: (yyDollar[3].stmt).(*SelectStmt)}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].boolExp, op: ADDOP, right: yyDollar[3].boolExp}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].boolExp, op: SUBSOP, right: yyDollar[3].boolExp}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].boolExp, op: DIVOP, right: yyDollar[3].boolExp}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].boolExp, op: MULTOP, right: yyDollar[3].boolExp}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &BinBoolExp{left: yyDollar[1].boolExp, op: yyDollar[2].logicOp, right: yyDollar[3].boolExp}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].boolExp, op: yyDollar[2].cmpOp, right: yyDollar[3].boolExp}
		}
	}
	goto yystack /* stack new state and value */
}
