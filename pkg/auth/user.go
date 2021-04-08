/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package auth

import (
	"fmt"
	"regexp"
	"time"
)

// Permission per database
type Permission struct {
	Permission uint32 `json:"permission"` //permission of type auth.PermissionW
	Database   string `json:"database"`   //databases the user has access to
}

// User ...
type User struct {
	Username       string                `json:"username"`
	HashedPassword []byte                `json:"hashedpassword"`
	Permissions    map[string]Permission `json:"permissions"`
	Active         bool                  `json:"active"`
	IsSysAdmin     bool                  `json:"-"`         //for the sysadmin we'll use this instead of adding all db and permissions to Permissions, to save some cpu cycles
	CreatedBy      string                `json:"createdBy"` //user which created this user
	CreatedAt      time.Time             `json:"createdat"` //time in which this user is created/updated
}

// SysAdminUsername the system admin username
var SysAdminUsername = "immudb"

// SysAdminPassword the admin password (can be default or from command flags, config or env var)
var SysAdminPassword = SysAdminUsername

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

// IsValidUsername is a regexp function used to check username requirements
var IsValidUsername = regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString

//HasPermission checks if user has such permission for this database
func (u *User) HasPermission(database string, permission uint32) bool {
	data, ok := u.Permissions[database]
	if ok {
		return data.Permission == permission
	}
	return false
}

//HasAtLeastOnePermission checks if user has this permission for at least one database
func (u *User) HasAtLeastOnePermission(permission uint32) bool {
	for _, val := range u.Permissions {
		if val.Permission == permission {
			return true
		}
	}
	return false
}

//WhichPermission returns the permission that this user has on this database
func (u *User) WhichPermission(database string) uint32 {
	if u.IsSysAdmin {
		return PermissionSysAdmin
	}
	data, ok := u.Permissions[database]
	if ok {
		return data.Permission
	}
	return PermissionNone
}

//RevokePermission revoke database permission from user
func (u *User) RevokePermission(database string) bool {
	_, ok := u.Permissions[database]
	if !ok {
		return false
	}
	delete(u.Permissions, database)
	return true
}

//GrantPermission add permission to database
func (u *User) GrantPermission(database string, permission uint32) bool {
	//first remove any previous permission for this db
	u.RevokePermission(database)
	u.Permissions[database] = Permission{Permission: permission, Database: database}
	return true
}
