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
	"sync"
)

// User ...
type User struct {
	sync.RWMutex
	Username       string `json:"username"`
	HashedPassword []byte `json:"-"`
	Admin          bool   `json:"admin"`
}

// AdminUser is the unique existing user
// (to be changed/removed in the future, when multiple users will be supported)
var AdminUser = User{
	Username: "immu",
	Admin:    true,
}

// GenerateAndSetPassword ...
func (u *User) GenerateAndSetPassword() (string, error) {
	plainPassword, err := generatePassword()
	if err != nil {
		return "", err
	}
	hashedPassword, err := hashAndSaltPassword(plainPassword, u.Admin)
	if err != nil {
		return "", err
	}
	u.SetPassword(hashedPassword)
	return plainPassword, nil
}

func (u *User) SetPassword(hashedPassword []byte) {
	u.Lock()
	defer u.Unlock()
	u.HashedPassword = hashedPassword
}

// ComparePasswords ...
func (u *User) ComparePasswords(plainPassword string) error {
	return comparePasswords(u.HashedPassword, []byte(plainPassword))
}
