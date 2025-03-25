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

package immuos

import (
	"os/exec"
	"os/user"
)

// User ...
type User interface {
	AddGroup(name string) error
	AddUser(usr string, group string) error
	LookupGroup(name string) (*user.Group, error)
	Lookup(username string) (*user.User, error)
}

// StandardUser ...
type StandardUser struct {
	AddGroupF    func(name string) error
	AddUserF     func(usr string, group string) error
	LookupGroupF func(name string) (*user.Group, error)
	LookupF      func(username string) (*user.User, error)
}

// NewStandardUser ...
func NewStandardUser() *StandardUser {
	return &StandardUser{
		AddGroupF:    func(name string) error { return exec.Command("groupadd", name).Run() },
		AddUserF:     func(usr string, group string) error { return exec.Command("useradd", "-g", usr, usr).Run() },
		LookupGroupF: user.LookupGroup,
		LookupF:      user.Lookup,
	}
}

// AddGroup ...
func (su *StandardUser) AddGroup(name string) error {
	return su.AddGroupF(name)
}

// AddUser ...
func (su *StandardUser) AddUser(usr string, group string) error {
	return su.AddUserF(usr, group)
}

// LookupGroup ...
func (su *StandardUser) LookupGroup(name string) (*user.Group, error) {
	return su.LookupGroupF(name)
}

// Lookup ...
func (su *StandardUser) Lookup(username string) (*user.User, error) {
	return su.LookupF(username)
}
