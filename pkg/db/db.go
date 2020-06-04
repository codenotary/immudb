package db

import (
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
)

type Db struct {
	Store    *store.Store
	SysStore *store.Store
	Logger   logger.Logger
	options  *DbOptions
}

func NewDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.DbName+" ", os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.DbName, op.GetSysDbDir())
	if err = os.MkdirAll(sysDbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create sys data folder: %s", err)
		return nil, err
	}
	dbDir := filepath.Join(op.DbName, op.GetDbDir())
	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create data folder: %s", err)
		return nil, err
	}
	db.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open sysstore: %s", err)
		return nil, err
	}
	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open store: %s", err)
		return nil, err
	}
	return db, nil
}
