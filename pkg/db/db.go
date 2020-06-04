/*
Copyright 2019-2020 vChain, Inc.

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
