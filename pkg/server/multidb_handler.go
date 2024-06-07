/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

func (h *multidbHandler) GetLoggedUser(ctx context.Context) (sql.User, error) {
	_, user, err := h.s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	db, err := h.s.getDBFromCtx(ctx, "SQLQuery")
	if err != nil {
		return nil, err
	}

	privileges := make([]sql.SQLPrivilege, len(user.SQLPrivileges))
	for i, p := range user.SQLPrivileges {
		privileges[i] = sql.SQLPrivilege(p.Privilege)
	}

	permCode := user.WhichPermission(db.GetName())
	return &User{
		username:      user.Username,
		perm:          permissionFromCode(permCode),
		sqlPrivileges: privileges,
	}, nil
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

		var perm *schema.Permission

		if string(user.User) == auth.SysAdminUsername {
			perm = &schema.Permission{Database: db.GetName()}
		} else {
			perm = findPermission(user.Permissions, db.GetName())
		}

		privileges := make([]sql.SQLPrivilege, len(user.SqlPrivileges))
		for i, p := range user.SqlPrivileges {
			privileges[i] = schema.SQLPrivilegeFromProto(p)
		}

		if perm != nil {
			users = append(users, &User{username: string(user.User), perm: permissionFromCode(perm.Permission), sqlPrivileges: privileges})
		}
	}

	return users, nil
}

func permissionFromCode(code uint32) sql.Permission {
	switch code {
	case 1:
		{
			return sql.PermissionReadOnly
		}
	case 2:
		{
			return sql.PermissionReadWrite
		}
	case 254:
		{
			return sql.PermissionAdmin
		}
	}
	return sql.PermissionSysAdmin
}

func findPermission(permissions []*schema.Permission, database string) *schema.Permission {
	for _, perm := range permissions {
		if perm.Database == database {
			return perm
		}
	}
	return nil
}

type User struct {
	username      string
	perm          sql.Permission
	sqlPrivileges []sql.SQLPrivilege
}

func (usr *User) Username() string {
	return usr.username
}

func (usr *User) Permission() sql.Permission {
	return usr.perm
}

func (usr *User) SQLPrivileges() []sql.SQLPrivilege {
	return usr.sqlPrivileges
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

func (h *multidbHandler) GrantSQLPrivileges(ctx context.Context, database, username string, privileges []sql.SQLPrivilege) error {
	return h.changeSQLPrivileges(ctx, database, username, privileges, schema.PermissionAction_GRANT)
}

func (h *multidbHandler) RevokeSQLPrivileges(ctx context.Context, database, username string, privileges []sql.SQLPrivilege) error {
	return h.changeSQLPrivileges(ctx, database, username, privileges, schema.PermissionAction_REVOKE)
}

func (h *multidbHandler) changeSQLPrivileges(ctx context.Context, database, username string, privileges []sql.SQLPrivilege, action schema.PermissionAction) error {
	ps := make([]schema.SQLPrivilege, len(privileges))
	for i, p := range privileges {
		pp, err := schema.SQLPrivilegeToProto(p)
		if err != nil {
			return err
		}
		ps[i] = pp
	}

	_, err := h.s.ChangeSQLPrivileges(ctx, &schema.ChangeSQLPrivilegesRequest{
		Action:     action,
		Username:   username,
		Database:   database,
		Privileges: ps,
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
