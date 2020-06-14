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

package auth

import (
	"regexp"
)

// User ...
type User struct {
	Username       string `json:"username"`
	HashedPassword []byte `json:"-"`
	Permissions    byte   `json:"permissions"`
}

// AdminUsername the admin username
var AdminUsername = "immu"

// AdminDefaultPassword the default admin password
var AdminDefaultPassword = "immu"

// AdminPassword the admin password (can be default or from command flags, config or env var)
var AdminPassword = AdminDefaultPassword

// GenerateOrSetPassword Returns a generated or plainPassword if it not empty
// Hashes and salts the password and assigns it to hashedPassword of User
func (u *User) GenerateOrSetPassword(plainPassword []byte) ([]byte, error) {
	if len(plainPassword) == 0 {
		plainPassword = []byte(generatePassword())
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
