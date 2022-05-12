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

package immuos

import (
	"errors"
	"os/user"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandardUser(t *testing.T) {
	su := NewStandardUser()

	// AddGroup
	addGroupFOK := su.AddGroupF
	errAddGroup := errors.New("AddGroup error")
	su.AddGroupF = func(name string) error {
		return errAddGroup
	}
	require.Equal(t, errAddGroup, su.AddGroup("name"))
	su.AddGroupF = addGroupFOK

	// AddUser
	addUserFOK := su.AddUserF
	errAddUser := errors.New("AddUser error")
	su.AddUserF = func(usr string, group string) error {
		return errAddUser
	}
	require.Equal(t, errAddUser, su.AddUser("usr", "group"))
	su.AddUserF = addUserFOK

	// LookupGroup ...
	lookupGroupFOK := su.LookupGroupF
	errLookupGroup := errors.New("LookupGroup error")
	su.LookupGroupF = func(name string) (*user.Group, error) {
		return nil, errLookupGroup
	}
	_, err := su.LookupGroup("name")
	require.Equal(t, errLookupGroup, err)
	su.LookupGroupF = lookupGroupFOK

	// Lookup ...
	lookupFOK := su.LookupF
	errLookup := errors.New("Lookup error")
	su.LookupF = func(username string) (*user.User, error) {
		return nil, errLookup
	}
	_, err = su.Lookup("username")
	require.Equal(t, errLookup, err)
	su.LookupF = lookupFOK
}
