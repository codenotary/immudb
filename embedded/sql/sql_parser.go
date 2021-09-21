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
	exp      ValueExp
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

const yyLast = 279

var yyAct = [...]int{
	230, 38, 58, 94, 143, 4, 138, 160, 90, 74,
	137, 102, 112, 66, 147, 75, 194, 150, 95, 158,
	156, 157, 135, 40, 109, 155, 109, 151, 152, 153,
	154, 39, 214, 222, 136, 148, 179, 180, 215, 79,
	149, 55, 212, 109, 109, 50, 52, 175, 176, 178,
	177, 121, 110, 196, 210, 179, 180, 163, 117, 161,
	180, 61, 80, 186, 179, 180, 175, 176, 178, 177,
	175, 176, 178, 177, 162, 175, 176, 178, 177, 51,
	98, 93, 97, 108, 99, 175, 176, 178, 177, 139,
	185, 18, 96, 104, 76, 105, 84, 82, 72, 70,
	107, 60, 16, 178, 177, 40, 100, 71, 61, 229,
	116, 39, 221, 5, 122, 197, 35, 168, 119, 131,
	109, 57, 40, 209, 132, 225, 128, 167, 39, 144,
	32, 133, 106, 33, 87, 120, 140, 7, 40, 159,
	91, 187, 124, 118, 171, 103, 92, 85, 81, 78,
	37, 64, 182, 183, 184, 62, 51, 49, 46, 103,
	41, 114, 193, 51, 216, 166, 83, 192, 43, 33,
	181, 189, 190, 63, 59, 195, 202, 199, 200, 219,
	203, 204, 205, 206, 207, 208, 73, 77, 231, 232,
	220, 211, 174, 213, 142, 15, 127, 173, 129, 86,
	17, 68, 217, 67, 56, 10, 11, 21, 7, 125,
	123, 30, 29, 53, 19, 164, 12, 223, 69, 10,
	11, 224, 227, 228, 13, 14, 88, 2, 188, 54,
	12, 233, 130, 65, 234, 6, 44, 115, 13, 14,
	22, 45, 7, 28, 31, 23, 25, 24, 48, 26,
	27, 95, 191, 165, 42, 198, 226, 134, 218, 141,
	146, 172, 126, 113, 111, 47, 20, 36, 34, 145,
	169, 170, 201, 89, 101, 9, 8, 3, 1,
}

var yyPact = [...]int{
	215, -1000, -1000, 35, 24, -1000, 193, 179, -1000, -1000,
	234, 243, 232, 187, 186, -1000, 215, -1000, -1000, 201,
	52, -1000, 107, 124, 222, 228, 105, 240, 104, 103,
	103, -1000, 191, -26, 175, -1000, 60, 133, -1000, 33,
	42, -1000, 102, 131, 98, 219, -1000, 173, 170, 202,
	31, 41, 30, -1000, -1000, 201, 26, 69, -1000, 96,
	-30, 95, 29, 121, 28, 94, -1000, 168, 79, 209,
	87, 93, 87, -1000, 246, -1000, 110, 133, -1000, -1000,
	15, 40, 92, -1000, 87, 27, 77, -1000, 92, 14,
	59, -1000, -1000, -17, 111, 224, 173, -11, -1000, -1000,
	90, 57, -1000, 81, -18, 87, -1000, -1000, 184, 89,
	183, 162, -1000, 111, 166, 218, 133, -1000, -1000, 106,
	-48, -1000, -35, 21, -1000, 21, 159, -28, -1000, 26,
	6, -12, 196, -1000, 119, 72, -1000, 56, -1000, -28,
	56, 164, 156, 4, 127, -1000, -1000, -28, -28, -28,
	22, -1000, -1000, -1000, -1000, -5, 88, -1000, -1000, 214,
	-1000, -1000, 87, -1000, 6, 120, -1000, -55, 21, -16,
	54, 4, 139, -28, 85, -28, -28, -28, -28, -28,
	-28, 67, 8, 39, -15, 181, -27, -1000, -28, -37,
	-31, -1000, -1000, 117, -1000, -1000, -1000, -28, 142, 154,
	4, 51, -1000, 39, 39, -1000, -1000, 8, 23, -1000,
	-1000, -36, -1000, 13, -1000, -1000, -1000, 4, 133, 70,
	85, 85, -1000, -1000, -1000, -1000, 48, 149, -1000, 85,
	-1000, -1000, -1000, 149, -1000,
}

var yyPgo = [...]int{
	0, 278, 227, 130, 277, 113, 276, 275, 5, 274,
	11, 8, 7, 273, 272, 10, 6, 271, 270, 269,
	129, 268, 267, 1, 266, 9, 15, 265, 13, 264,
	12, 263, 4, 262, 261, 260, 259, 258, 257, 2,
	256, 255, 0, 3, 254, 253, 252, 195,
}

var yyR1 = [...]int{
	0, 1, 2, 2, 2, 47, 47, 4, 4, 5,
	5, 3, 3, 6, 6, 6, 6, 6, 6, 6,
	27, 27, 44, 44, 12, 12, 7, 7, 13, 13,
	15, 15, 16, 11, 11, 14, 14, 18, 18, 17,
	17, 19, 19, 19, 19, 19, 19, 19, 19, 9,
	9, 10, 38, 38, 45, 45, 46, 46, 46, 8,
	24, 24, 21, 21, 22, 22, 20, 20, 20, 23,
	23, 23, 25, 25, 25, 26, 26, 28, 28, 29,
	29, 30, 30, 31, 33, 33, 36, 36, 34, 34,
	37, 37, 41, 41, 43, 43, 40, 40, 42, 42,
	42, 39, 39, 32, 32, 32, 32, 32, 32, 32,
	32, 35, 35, 35, 35, 35, 35,
}

var yyR2 = [...]int{
	0, 1, 2, 2, 3, 0, 1, 1, 4, 1,
	1, 2, 3, 3, 3, 4, 11, 7, 8, 6,
	0, 3, 0, 3, 1, 3, 8, 8, 0, 1,
	1, 3, 3, 1, 3, 1, 3, 0, 1, 1,
	3, 1, 1, 1, 1, 3, 2, 1, 1, 1,
	3, 5, 0, 3, 0, 1, 0, 1, 2, 13,
	0, 1, 1, 1, 2, 4, 1, 3, 4, 1,
	3, 5, 1, 5, 3, 1, 3, 0, 3, 0,
	1, 1, 2, 6, 0, 2, 0, 3, 0, 2,
	0, 2, 0, 3, 0, 4, 2, 4, 0, 1,
	1, 0, 2, 1, 1, 1, 2, 2, 3, 3,
	4, 3, 3, 3, 3, 3, 3,
}

var yyChk = [...]int{
	-1000, -1, -2, -4, -8, -5, 20, 27, -6, -7,
	4, 5, 15, 23, 24, -47, 67, -47, 67, 21,
	-24, 28, 6, 11, 13, 12, 6, 7, 11, 25,
	25, -2, -3, -5, -21, 64, -22, -20, -23, 59,
	53, 53, -44, 44, 14, 13, 53, -27, 8, 53,
	-26, 53, -26, 22, -47, 67, 29, 61, -39, 41,
	68, 66, 53, 42, 53, 14, -28, 30, 31, 16,
	68, 66, 68, -3, -25, -26, 68, -20, 53, 69,
	-23, 53, 68, 45, 68, 53, 31, 55, 17, -13,
	-11, 53, 53, -11, -43, 5, -26, -8, -39, 69,
	66, -9, -10, 53, -11, 68, 55, -10, 69, 61,
	69, -29, -30, -31, 50, 13, -28, 69, 53, 61,
	54, 69, -11, 26, 53, 26, -33, 34, -30, 32,
	14, -39, 18, -10, -38, 70, 69, -15, -16, 68,
	-15, -36, 35, -32, -20, -19, -35, 42, 63, 68,
	45, 55, 56, 57, 58, 53, 48, 49, 47, -25,
	-12, 53, 68, 69, 19, -45, 46, 55, 61, -18,
	-17, -32, -34, 33, 36, 62, 63, 65, 64, 51,
	52, 43, -32, -32, -32, 68, 68, 53, 14, -11,
	-12, -46, 47, 42, 71, -16, 69, 61, -41, 38,
	-32, -14, -23, -32, -32, -32, -32, -32, -32, 56,
	69, -8, 69, -32, 69, 69, 47, -32, -37, 37,
	36, 61, 69, -43, -39, 55, -40, -23, -23, 61,
	-42, 39, 40, -23, -42,
}

var yyDef = [...]int{
	0, -2, 1, 5, 5, 7, 0, 60, 9, 10,
	0, 0, 0, 0, 0, 2, 6, 3, 6, 0,
	0, 61, 0, 22, 0, 0, 0, 20, 0, 0,
	0, 4, 0, 5, 0, 62, 63, 101, 66, 0,
	69, 13, 0, 0, 0, 0, 14, 77, 0, 0,
	0, 75, 0, 8, 11, 6, 0, 0, 64, 0,
	0, 0, 0, 0, 0, 0, 15, 0, 0, 0,
	28, 0, 0, 12, 94, 72, 0, 101, 102, 67,
	0, 70, 0, 23, 0, 0, 0, 21, 0, 0,
	29, 33, 76, 0, 79, 0, 77, 0, 65, 68,
	0, 0, 49, 0, 0, 0, 78, 19, 0, 0,
	0, 84, 80, 81, 0, 0, 101, 74, 71, 0,
	52, 17, 0, 0, 34, 0, 86, 0, 82, 0,
	0, 0, 0, 50, 54, 0, 18, 26, 30, 37,
	27, 88, 0, 85, 103, 104, 105, 0, 0, 0,
	0, 41, 42, 43, 44, 69, 0, 47, 48, 0,
	95, 24, 0, 73, 0, 56, 55, 0, 0, 0,
	38, 39, 92, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 106, 107, 0, 0, 0, 46, 0, 0,
	0, 51, 57, 0, 53, 31, 32, 0, 90, 0,
	89, 87, 35, 111, 112, 113, 114, 115, 116, 109,
	108, 0, 45, 94, 25, 16, 58, 40, 101, 0,
	0, 0, 110, 83, 59, 91, 93, 98, 36, 0,
	96, 99, 100, 98, 97,
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
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 29:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = yyDollar[1].ids
		}
	case 30:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.rows = []*RowSpec{yyDollar[1].row}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.rows = append(yyDollar[1].rows, yyDollar[3].row)
		}
	case 32:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.row = &RowSpec{Values: yyDollar[2].values}
		}
	case 33:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ids = []string{yyDollar[1].id}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ids = append(yyDollar[1].ids, yyDollar[3].id)
		}
	case 35:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.cols = []*ColSelector{yyDollar[1].col}
		}
	case 36:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = append(yyDollar[1].cols, yyDollar[3].col)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.values = nil
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = yyDollar[1].values
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.values = []ValueExp{yyDollar[1].exp}
		}
	case 40:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].exp)
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Number{val: int64(yyDollar[1].number)}
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Varchar{val: yyDollar[1].str}
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Bool{val: yyDollar[1].boolean}
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Blob{val: yyDollar[1].blob}
		}
	case 45:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.value = &SysFn{fn: yyDollar[1].id}
		}
	case 46:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.value = &Param{id: yyDollar[2].id}
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &Param{id: fmt.Sprintf("param%d", yyDollar[1].pparam), pos: yyDollar[1].pparam}
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.value = &NullValue{t: AnyType}
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.colsSpec = []*ColSpec{yyDollar[1].colSpec}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.colsSpec = append(yyDollar[1].colsSpec, yyDollar[3].colSpec)
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.colSpec = &ColSpec{colName: yyDollar[1].id, colType: yyDollar[2].sqlType, maxLen: int(yyDollar[3].number), autoIncrement: yyDollar[4].boolean, notNull: yyDollar[5].boolean}
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 53:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.boolean = false
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.boolean = true
		}
	case 59:
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
				limit:     yyDollar[12].number,
				as:        yyDollar[13].id,
			}
		}
	case 60:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.distinct = false
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.distinct = true
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = nil
		}
	case 63:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sels = yyDollar[1].sels
		}
	case 64:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyDollar[1].sel.setAlias(yyDollar[2].id)
			yyVAL.sels = []Selector{yyDollar[1].sel}
		}
	case 65:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyDollar[3].sel.setAlias(yyDollar[4].id)
			yyVAL.sels = append(yyDollar[1].sels, yyDollar[3].sel)
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.sel = yyDollar[1].col
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, col: "*"}
		}
	case 68:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.sel = &AggColSelector{aggFn: yyDollar[1].aggFn, db: yyDollar[3].col.db, table: yyDollar[3].col.table, col: yyDollar[3].col.col}
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.col = &ColSelector{col: yyDollar[1].id}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.col = &ColSelector{table: yyDollar[1].id, col: yyDollar[3].id}
		}
	case 71:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyVAL.col = &ColSelector{db: yyDollar[1].id, table: yyDollar[3].id, col: yyDollar[5].id}
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.ds = yyDollar[1].tableRef
		}
	case 73:
		yyDollar = yyS[yypt-5 : yypt+1]
		{
			yyDollar[2].tableRef.asBefore = yyDollar[3].number
			yyDollar[2].tableRef.as = yyDollar[4].id
			yyVAL.ds = yyDollar[2].tableRef
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ds = yyDollar[2].stmt.(*SelectStmt)
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{table: yyDollar[1].id}
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.tableRef = &tableRef{db: yyDollar[1].id, table: yyDollar[3].id}
		}
	case 77:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.number = yyDollar[3].number
		}
	case 79:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.joins = nil
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = yyDollar[1].joins
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.joins = []*JoinSpec{yyDollar[1].join}
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.joins = append([]*JoinSpec{yyDollar[1].join}, yyDollar[2].joins...)
		}
	case 83:
		yyDollar = yyS[yypt-6 : yypt+1]
		{
			yyVAL.join = &JoinSpec{joinType: yyDollar[1].joinType, ds: yyDollar[3].ds, cond: yyDollar[5].exp, indexOn: yyDollar[6].ids}
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.cols = nil
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.cols = yyDollar[3].cols
		}
	case 88:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.exp = nil
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.number = 0
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.number = yyDollar[2].number
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ordcols = nil
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.ordcols = yyDollar[3].ordcols
		}
	case 94:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.ids = nil
		}
	case 95:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ids = yyDollar[4].ids
		}
	case 96:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.ordcols = []*OrdCol{{sel: yyDollar[1].col, order: yyDollar[2].opt_ord}}
		}
	case 97:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.ordcols = append(yyDollar[1].ordcols, &OrdCol{sel: yyDollar[3].col, order: yyDollar[4].opt_ord})
		}
	case 98:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.opt_ord = AscOrder
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = AscOrder
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.opt_ord = DescOrder
		}
	case 101:
		yyDollar = yyS[yypt-0 : yypt+1]
		{
			yyVAL.id = ""
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.id = yyDollar[2].id
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].sel
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].value
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		{
			yyVAL.exp = yyDollar[1].binExp
		}
	case 106:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NotBoolExp{exp: yyDollar[2].exp}
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		{
			yyVAL.exp = &NumExp{left: &Number{val: 0}, op: SUBSOP, right: yyDollar[2].exp}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = yyDollar[2].exp
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.exp = &LikeBoolExp{sel: yyDollar[1].sel, pattern: yyDollar[3].str}
		}
	case 110:
		yyDollar = yyS[yypt-4 : yypt+1]
		{
			yyVAL.exp = &ExistsBoolExp{q: (yyDollar[3].stmt).(*SelectStmt)}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: ADDOP, right: yyDollar[3].exp}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: SUBSOP, right: yyDollar[3].exp}
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: DIVOP, right: yyDollar[3].exp}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &NumExp{left: yyDollar[1].exp, op: MULTOP, right: yyDollar[3].exp}
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &BinBoolExp{left: yyDollar[1].exp, op: yyDollar[2].logicOp, right: yyDollar[3].exp}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		{
			yyVAL.binExp = &CmpBoolExp{left: yyDollar[1].exp, op: yyDollar[2].cmpOp, right: yyDollar[3].exp}
		}
	}
	goto yystack /* stack new state and value */
}
