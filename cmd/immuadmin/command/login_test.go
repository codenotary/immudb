/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package immuadmin

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/assert"
)

func TestCommandLine_Login(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Set arguments to execute the login command.
	cmd.SetArgs([]string{"login", auth.SysAdminUsername})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Remove the default post run function, which would always perform a logout
	// after executing the command.
	cmd.PersistentPostRun = nil

	// Execute the command.
	err := cmd.Execute()
	assert.NoError(t, err, "Executing login command should not fail.")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "logged in")
	assert.True(t, cmdl.immuClient.IsConnected(), "immuclient of commandline should be connected after login command.")
}

func TestCommandLine_Logout(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Set arguments to execute the logout command.
	cmd.SetArgs([]string{"logout"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Remove the default post run function, which would always perform a logout
	// after executing the command.
	cmd.PersistentPostRun = nil

	// Execute the command.
	err := cmd.Execute()
	assert.NoError(t, err, "executing logout command failed")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "logged out")
	assert.False(t, cmdl.immuClient.IsConnected(), "immuclient should be disconnected after logout")
}
