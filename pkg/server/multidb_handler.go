/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package server

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
)

type multidbHandler struct {
	s *ImmuServer
}

func (s *ImmuServer) multidbHandler() sql.MultiDBHandler {
	return &multidbHandler{s}
}

func (h *multidbHandler) UseDatabase(ctx context.Context, db string) error {
	if auth.GetAuthTypeFromContext(ctx) != auth.SessionAuth {
		return fmt.Errorf("%w: database selection from SQL statements requires session based authentication", ErrNotSupported)
	}

	_, err := h.s.UseDatabase(ctx, &schema.Database{DatabaseName: db})
	return err
}

func (h *multidbHandler) CreateDatabase(ctx context.Context, db string, ifNotExists bool) error {
	_, err := h.s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{
		Name:        db,
		IfNotExists: ifNotExists,
	})
	return err
}

func (h *multidbHandler) ListDatabases(ctx context.Context) ([]string, error) {
	res, err := h.s.DatabaseList(ctx, nil)
	if err != nil {
		return nil, err
	}

	dbs := make([]string, len(res.Databases))

	for i, db := range res.Databases {
		dbs[i] = db.DatabaseName
	}

	return dbs, nil
}

func (h *multidbHandler) ExecPreparedStmts(ctx context.Context, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, committedTxs []*sql.SQLTx, err error) {
	db, err := h.s.getDBFromCtx(ctx, "SQLExec")
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.NewSQLTx(ctx)
	if err != nil {
		return nil, nil, err
	}

	return db.SQLExecPrepared(stmts, params, tx)
}
