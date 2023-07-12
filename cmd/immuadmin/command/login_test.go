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
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/assert"
)

func TestCommandLine_Connect(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Run the help command to initialize default command line flags.
	cmd.SetArgs([]string{"help"})
	err := cmd.Execute()
	assert.NotNil(t, cmdl.immuClient, "immuclient should be initialized after help command")
	assert.NoError(t, err)

	// Check that connecting with the default parameters is successful.
	err = cmdl.connect(cmd, []string{})
	assert.NoError(t, err, "Connecting to the immudb instance should be successful with the default test commandline.")
	assert.True(t, cmdl.immuClient.IsConnected(), "immuclient of commandline should be connected.")
}

func TestCommandLine_Disconnect(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Run the help command to initialize default command line flags.
	cmd.SetArgs([]string{"help"})
	err := cmd.Execute()
	assert.NoError(t, err)

	// Connect to server.
	err = cmdl.connect(cmd, []string{})
	assert.NoError(t, err, "Connecting to the immudb instance should be successful with the default test commandline.")
	assert.True(t, cmdl.immuClient.IsConnected(), "immuclient of commandline should be connected.")

	// Disconnect form server.
	cmdl.disconnect(cmd, []string{})
	assert.False(t, cmdl.immuClient.IsConnected(), "immuclient should be disconnected after disconnect")
}

func TestCommandLine_Login(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Set arguments to execute the login command.
	cmd.SetArgs([]string{"login", auth.SysAdminUsername})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

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

	// Execute the command.
	err := cmd.Execute()
	assert.NoError(t, err, "executing logout command failed")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "logged out")
	assert.False(t, cmdl.immuClient.IsConnected(), "immuclient should be disconnected after logout")
}

func TestCommandLine_Connect_NonInteractive(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Run the help command to initialize default command line flags.
	// Set the flag to run the command in non interactive mode.
	cmd.SetArgs([]string{"help", "--non-interactive"})
	err := cmd.Execute()
	assert.NoError(t, err)

	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}

	// Connecting has to fail, as the password may not be read from the
	// passwordReader in non interactive mode.
	err = cmdl.connect(cmd, []string{})
	assert.Error(t, err, "Connecting to the database in non interactive mode without specifying a password-file should fail.")
	assert.ErrorContains(t, err, "--password-file flag", "The user should be informed about the password-file flag if the command is running interactively.")
	assert.Equal(t, 0, pwr.Counter, "No password should be read from stdin in non interactive mode.")
}

func TestCommandLine_Connect_PasswordFile(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)

	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}

	// Create a file containing the password for connecting to immudb.
	tempDir := t.TempDir()
	passwordFile := path.Join(tempDir, "immupass")

	// Run the help command to initialize default command line flags.
	// Set the flag to read the password from the file.
	cmd.SetArgs([]string{"help", "--password-file", passwordFile})

	// Execute command to initialize command line flags.
	err := cmd.Execute()
	assert.NoError(t, err)

	t.Run("not existing passwordfile", func(t *testing.T) {
		// Connecting should fail, as the password to connect to immudb
		// cannot be read from the specified file.
		err = cmdl.connect(cmd, []string{})
		assert.Error(t, err, "Connecting to an immudb instance without specifying a valid file for the --password-file flag should fail.")
		assert.Equal(t, 0, pwr.Counter, "Reading a password from stdin should not be attempted if the --password-file flag is set.")
	})

	t.Run("existing passwordfile - wrong password", func(t *testing.T) {
		// Remove the file with the password after the test.
		t.Cleanup(func() {
			os.Remove(passwordFile)
		})
		// Write an invalid password to the password file.
		err = os.WriteFile(passwordFile, []byte("invalid password"), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should fail, as the password to connect to immudb is wrong.
		err = cmdl.connect(cmd, []string{})
		assert.Error(t, err, "Connecting to an immudb instance with an invalid password should fail.")
		assert.Equal(t, 0, pwr.Counter, "Reading a password from stdin should not be attempted if the --password-file flag is set.")
	})

	t.Run("existing passwordfile - too long password", func(t *testing.T) {
		// Remove the file with the password after the test.
		t.Cleanup(func() {
			os.Remove(passwordFile)
		})
		// Write an password to the password file, which exceeds the maximum length.
		tooLongPassword := "0123456789abcdefghijklmnopqrstuvwxyz"
		err = os.WriteFile(passwordFile, []byte(tooLongPassword), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should fail, as the password to connect to immudb is wrong.
		err = cmdl.connect(cmd, []string{})
		assert.Error(t, err, "Connecting to an immudb instance with an invalid password should fail.")
		assert.Contains(t, err.Error(), "exceeds the the maximal password length", "The user should be informed that the content of the password file is too long to contain a valid password.")
		assert.Equal(t, 0, pwr.Counter, "Reading a password from stdin should not be attempted if the --password-file flag is set.")
	})

	t.Run("existing passwordfile - password with new line", func(t *testing.T) {
		// Remove the file with the password after the test and disconnect.
		t.Cleanup(func() {
			os.Remove(passwordFile)
			cmdl.disconnect(cmd, []string{})
		})
		// Write a valid password including a trailing new line to the password file.
		err = os.WriteFile(passwordFile, []byte(auth.SysAdminPassword+"\n"), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should fail, as the password to connect to immudb is wrong.
		err = cmdl.connect(cmd, []string{})
		assert.NoError(t, err, "Connecting to an immudb instance with a valid password should not fail.")
		assert.Equal(t, 0, pwr.Counter, "Reading a password from stdin should not be attempted if the --password-file flag is set.")
		assert.True(t, cmdl.immuClient.IsConnected(), "immuclient should be connected if a valid password was provided.")
	})

	t.Run("valid passwordfile", func(t *testing.T) {
		// Remove the file with the password after the test and disconnect.
		t.Cleanup(func() {
			os.Remove(passwordFile)
			cmdl.disconnect(cmd, []string{})
		})
		err = os.WriteFile(passwordFile, []byte(auth.SysAdminPassword), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should be successful, as the password to connect to immudb
		// can be read from the specified file.
		err = cmdl.connect(cmd, []string{})
		assert.NoError(t, err, "Connecting to an immudb instance with a valid password should not fail.")
		assert.Equal(t, 0, pwr.Counter, "Reading a password from stdin should not be attempted if the --password-file flag is set.")
		assert.True(t, cmdl.immuClient.IsConnected(), "immuclient should be connected if a valid password was provided.")
	})
}

func TestCommandLine_Connect_Database(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Run the help command to initialize default command line flags.
	// Set a flag to specify a non existing DB for the command.
	cmd.SetArgs([]string{"help", "--database", "invalid_db"})
	err := cmd.Execute()
	assert.NoError(t, err)

	// Connecting should be fail, as the database for which a session is opened,
	// does not exist.
	err = cmdl.connect(cmd, []string{})
	assert.Error(t, err, "Connecting to an immudb instance with a non existing database name should fail.")
	assert.ErrorIs(t, err, database.ErrDatabaseNotExists)

}
