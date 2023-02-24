package object

import (
	"github.com/codenotary/immudb/embedded/sql"
)

var ObjectPrefix = []byte{3}

const (
	catalogDatabasePrefix   = "CTL.OBJ.DATABASE."   // (key=CTL.OBJ.DATABASE.{dbID}, value={dbNAME})
	catalogCollectionPrefix = "CTL.OBJ.COLLECTION." // (key=CTL.OBJ.COLLECTION.{dbID}{collectionID}, value={collectionNAME})
	catalogColumnPrefix     = "CTL.OBJ.COLUMN."     // (key=CTL.OBJ.COLUMN.{dbID}{collectionID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogIndexPrefix      = "CTL.OBJ.INDEX."      // (key=CTL.OBJ.INDEX.{dbID}{collectionID}{indexID}, value={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)})
)

func DefaultOptions() *sql.Options {
	return sql.DefaultOptions().
		WithPrefix(ObjectPrefix).
		WithDatabasePrefix(catalogDatabasePrefix).
		WithTablePrefix(catalogCollectionPrefix).
		WithColumnPrefix(catalogColumnPrefix).
		WithIndexPrefix(catalogIndexPrefix)
}
