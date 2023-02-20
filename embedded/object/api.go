package object

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
)

const (
	catalogDatabasePrefix = "CTL.OBJ.DATABASE." // (key=CTL.OBJ.DATABASE.{dbID}, value={dbNAME})
	catalogTablePrefix    = "CTL.OBJ.TABLE."    // (key=CTL.OBJ.TABLE.{dbID}{tableID}, value={tableNAME})
	catalogColumnPrefix   = "CTL.OBJ.COLUMN."   // (key=CTL.OBJ.COLUMN.{dbID}{tableID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogIndexPrefix    = "CTL.OBJ.INDEX."    // (key=CTL.OBJ.INDEX.{dbID}{tableID}{indexID}, value={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)})

	// Old prefixes that must not be reused:
	//  `CATALOG.DATABASE.`
	//  `CATALOG.TABLE.`
	//  `CATALOG.COLUMN.`
	//  `CATALOG.INDEX.`
	//  `P.` 				primary indexes without null support
	//  `S.` 				secondary indexes without null support
	//  `U.` 				secondary unique indexes without null support
	//  `PINDEX.` 			single-column primary indexes
	//  `SINDEX.` 			single-column secondary indexes
	//  `UINDEX.` 			single-column secondary unique indexes
)

type Stmt interface {
	execAt(ctx context.Context, tx Tx, params map[string]interface{}) (Tx, error)
	inferParameters(ctx context.Context, tx Tx, params map[string]sql.SQLValueType) error
}
