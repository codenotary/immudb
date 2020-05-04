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

// User ...
type User struct {
	Username       string `json:"username"`
	HashedPassword []byte `json:"-"`
	Admin          bool   `json:"admin"`
}

var AdminUsername = "immu"

// GenerateAndSetPassword ...
func (u *User) GenerateAndSetPassword() (string, error) {
	plainPassword := GeneratePassword()
	hashedPassword, err := HashAndSaltPassword(plainPassword)
	if err != nil {
		return "", err
	}
	u.SetPassword(hashedPassword)
	return plainPassword, nil
}

func (u *User) SetPassword(hashedPassword []byte) {
	u.HashedPassword = hashedPassword
}

// ComparePasswords ...
func (u *User) ComparePasswords(plainPassword []byte) error {
	return ComparePasswords(u.HashedPassword, plainPassword)
}
