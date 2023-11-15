/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

func (h *multidbHandler) ListUsers(ctx context.Context) ([]sql.User, error) {
	db, err := h.s.getDBFromCtx(ctx, "ListUsers")
	if err != nil {
		return nil, err
	}

	res, err := h.s.ListUsers(ctx, nil)
	if err != nil {
		return nil, err
	}

	users := make([]sql.User, 0, len(res.Users))

	for _, user := range res.Users {
		if !user.Active {
			continue
		}

		var hasPermission *schema.Permission

		if string(user.User) == auth.SysAdminUsername {
			hasPermission = &schema.Permission{Database: db.GetName()}
		} else {
			for _, perm := range user.Permissions {
				if perm.Database == db.GetName() {
					hasPermission = perm
					break
				}
			}
		}

		if hasPermission != nil {
			users = append(users, &User{username: string(user.User), perm: hasPermission.Permission})
		}
	}

	return users, nil
}

type User struct {
	username string
	perm     uint32
}

func (usr *User) Username() string {
	return usr.username
}

func (usr *User) Permission() uint32 {
	return usr.perm
}

func permCode(permission sql.Permission) uint32 {
	switch permission {
	case sql.PermissionReadOnly:
		{
			return 1
		}
	case sql.PermissionReadWrite:
		{
			return 2
		}
	case sql.PermissionAdmin:
		{
			return 254
		}
	}
	return 0
}

func (h *multidbHandler) CreateUser(ctx context.Context, username, password string, permission sql.Permission) error {
	db, err := h.s.getDBFromCtx(ctx, "CreateUser")
	if err != nil {
		return err
	}

	_, err = h.s.CreateUser(ctx, &schema.CreateUserRequest{
		User:       []byte(username),
		Password:   []byte(password),
		Database:   db.GetName(),
		Permission: permCode(permission),
	})

	return err
}

func (h *multidbHandler) AlterUser(ctx context.Context, username, password string, permission sql.Permission) error {
	_, user, err := h.s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return err
	}

	db, err := h.s.getDBFromCtx(ctx, "ChangePassword")
	if err != nil {
		return err
	}

	_, err = h.s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Username: username,
		Active:   true,
	})
	if err != nil {
		return err
	}

	_, err = h.s.ChangePassword(ctx, &schema.ChangePasswordRequest{
		User:        []byte(username),
		OldPassword: []byte(user.HashedPassword),
		NewPassword: []byte(password),
	})
	if err != nil {
		return err
	}

	_, err = h.s.ChangePermission(ctx, &schema.ChangePermissionRequest{
		Username:   username,
		Database:   db.GetName(),
		Action:     schema.PermissionAction_GRANT,
		Permission: permCode(permission),
	})

	return err
}

func (h *multidbHandler) DropUser(ctx context.Context, username string) error {
	_, err := h.s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Username: username,
		Active:   false,
	})
	return err
}

func (h *multidbHandler) ExecPreparedStmts(
	ctx context.Context,
	opts *sql.TxOptions,
	stmts []sql.SQLStmt,
	params map[string]interface{},
) (ntx *sql.SQLTx, committedTxs []*sql.SQLTx, err error) {

	db, err := h.s.getDBFromCtx(ctx, "SQLExec")
	if err != nil {
		return nil, nil, err
	}

	tx, err := db.NewSQLTx(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	return db.SQLExecPrepared(ctx, tx, stmts, params)
}
