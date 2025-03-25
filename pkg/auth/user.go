/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package auth

import (
	"fmt"
	"regexp"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
)

// Permission per database
type Permission struct {
	Permission uint32 `json:"permission"` // permission of type auth.PermissionW
	Database   string `json:"database"`   // databases the user has access to
}

type SQLPrivilege struct {
	Privilege string `json:"privilege"` // sql privilege
	Database  string `json:"database"`  // database to which the privilege applies
}

// User ...
type User struct {
	Username       string         `json:"username"`
	HashedPassword []byte         `json:"hashedpassword"`
	Permissions    []Permission   `json:"permissions"`
	SQLPrivileges  []SQLPrivilege `json:"sqlPrivileges"`
	HasPrivileges  bool           `json:"hasPrivileges"` // needed for backward compatibility
	Active         bool           `json:"active"`
	IsSysAdmin     bool           `json:"-"`         // for the sysadmin we'll use this instead of adding all db and permissions to Permissions, to save some cpu cycles
	CreatedBy      string         `json:"createdBy"` // user which created this user
	CreatedAt      time.Time      `json:"createdat"` // time in which this user is created/updated
}

var (
	// SysAdminUsername the system admin username
	SysAdminUsername = "immudb"

	// SysAdminPassword the admin password (can be default or from command flags, config or env var)
	SysAdminPassword = SysAdminUsername
)

// SetPassword Hashes and salts the password and assigns it to hashedPassword of User
func (u *User) SetPassword(plainPassword []byte) ([]byte, error) {
	if len(plainPassword) == 0 {
		return nil, fmt.Errorf("password is empty")
	}
	hashedPassword, err := HashAndSaltPassword(plainPassword)
	if err != nil {
		return nil, err
	}
	u.HashedPassword = hashedPassword
	return plainPassword, nil
}

// ComparePasswords ...
func (u *User) ComparePasswords(plainPassword []byte) error {
	return ComparePasswords(u.HashedPassword, plainPassword)
}

const maxUsernameLen = 63

var usernameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// IsValidUsername is a function used to check username requirements
func IsValidUsername(user string) bool {
	return len(user) <= maxUsernameLen && usernameRegex.MatchString(user)
}

// HasPermission checks if user has such permission for this database
func (u *User) HasPermission(database string, permission uint32) bool {
	for _, val := range u.Permissions {
		if (val.Database == database) &&
			(val.Permission == permission) {
			return true
		}
	}
	return false
}

// HasAtLeastOnePermission checks if user has this permission for at least one database
func (u *User) HasAtLeastOnePermission(permission uint32) bool {
	for _, val := range u.Permissions {
		if val.Permission == permission {
			return true
		}
	}
	return false
}

// WhichPermission returns the permission that this user has on this database
func (u *User) WhichPermission(database string) uint32 {
	if u.IsSysAdmin {
		return PermissionSysAdmin
	}
	for _, val := range u.Permissions {
		if val.Database == database {
			return val.Permission
		}
	}
	return PermissionNone
}

// RevokePermission revoke database permission from user
func (u *User) RevokePermission(database string) bool {
	for i, val := range u.Permissions {
		if val.Database == database {
			//todo there is a more efficient way to remove elements
			u.Permissions = append(u.Permissions[:i], u.Permissions[i+1:]...)
			return true
		}
	}
	return false
}

// GrantPermission add permission to database
func (u *User) GrantPermission(database string, permission uint32) bool {
	// first remove any previous permission for this db
	u.RevokePermission(database)

	perm := Permission{Permission: permission, Database: database}
	u.Permissions = append(u.Permissions, perm)
	return true
}

// GrantSQLPrivilege grants sql privilege on the specified database
func (u *User) GrantSQLPrivileges(database string, privileges []string) bool {
	for _, p := range privileges {
		if !u.HasSQLPrivilege(database, p) {
			u.SQLPrivileges = append(u.SQLPrivileges, SQLPrivilege{Database: database, Privilege: p})
		}
	}
	return false
}

func (u *User) HasSQLPrivilege(database string, privilege string) bool {
	return u.indexOfPrivilege(database, privilege) >= 0
}

func (u *User) indexOfPrivilege(database string, privilege string) int {
	for i, p := range u.SQLPrivileges {
		if p.Database == database && p.Privilege == privilege {
			return i
		}
	}
	return -1
}

// RevokePrivilege add permission to database
func (u *User) RevokeSQLPrivileges(database string, privileges []string) bool {
	for _, p := range privileges {
		if idx := u.indexOfPrivilege(database, p); idx >= 0 {
			u.SQLPrivileges[idx] = u.SQLPrivileges[0]
			u.SQLPrivileges = u.SQLPrivileges[1:]
		}
	}
	return true
}

// SetSQLPrivileges sets user default privileges. Required to guarantee backward compatibility.
func (u *User) SetSQLPrivileges() {
	if u.HasPrivileges {
		return
	}

	for _, perm := range u.Permissions {
		privileges := sql.DefaultSQLPrivilegesForPermission(sql.PermissionFromCode(perm.Permission))
		for _, privilege := range privileges {
			u.SQLPrivileges = append(u.SQLPrivileges,
				SQLPrivilege{
					Database:  perm.Database,
					Privilege: string(privilege),
				},
			)
		}
	}
}
