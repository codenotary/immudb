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
	"bytes"
	"testing"
)

func TestUser(t *testing.T) {
	weakPassword := []byte("weak_password")
	u := User{}
	_, err := u.SetPassword(nil)
	if err == nil {
		t.Errorf("Setpassword, fail test empty password")
	}

	p, err := u.SetPassword(weakPassword)
	if err != nil {
		t.Errorf("Error setting password %s", err)
	}
	if !bytes.Equal(p, weakPassword) {
		t.Errorf("setpassword plain passwords do not match")
	}
	err = u.ComparePasswords(weakPassword)
	if err != nil {
		t.Errorf("ComparePasswords fail %s", err)
	}

	u.GrantPermission("immudb", PermissionR)
	perm := u.WhichPermission("immudb")
	if perm != PermissionR {
		t.Errorf("WhichPermission fail")
	}

	if !u.HasPermission("immudb", PermissionR) {
		t.Errorf("HasPermission fail")
	}

	if !u.HasAtLeastOnePermission(PermissionR) {
		t.Errorf("HasAtLeastOnePermission fail")
	}

	if u.HasPermission("immudb", PermissionAdmin) {
		t.Errorf("HasPermission failed on wrong permission")
	}

	if u.HasAtLeastOnePermission(PermissionAdmin) {
		t.Errorf("HasAtLeastOnePermission fail")
	}
	u.RevokePermission("immudb")
	perm = u.WhichPermission("immudb")
	if perm == PermissionR {
		t.Errorf("RevokePermission fail")
	}
	u.IsSysAdmin = true
	if perm = u.WhichPermission("notimmudb"); perm != PermissionSysAdmin {
		t.Errorf("WhichPermission sysadmin fail")
	}
}
