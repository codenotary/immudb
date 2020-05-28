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

import "regexp"

// User ...
type User struct {
	Username       string `json:"username"`
	HashedPassword []byte `json:"-"`
	Permissions    byte   `json:"permissions"`
}

var AdminUsername = "immu"

// GenerateAndSetPassword ...
func (u *User) GenerateAndSetPassword() (string, error) {
	plainPassword := generatePassword()
	hashedPassword, err := HashAndSaltPassword(plainPassword)
	if err != nil {
		return "", err
	}
	u.HashedPassword = hashedPassword
	return plainPassword, nil
}

// ComparePasswords ...
func (u *User) ComparePasswords(plainPassword []byte) error {
	return ComparePasswords(u.HashedPassword, plainPassword)
}

var IsValidUsername = regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString
