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
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type passwordReaderMock struct {
	Counter int
}

func (pwr *passwordReaderMock) Read(msg string) ([]byte, error) {
	var pw []byte
	if pwr.Counter == 0 {
		pw = []byte(auth.SysAdminPassword)
	} else {
		pw = []byte(`Passw0rd!-`)
	}
	pwr.Counter++
	return pw, nil
}

func TestCommandLine_Connect(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	cmdl.passwordReader = &passwordReaderMock{}

	// Create command and execute it to initialize command line flags.
	cmd, _ := cmdl.NewCmd()
	cmd.Execute()

	err = cmdl.connect(cmd, []string{})
	assert.NoError(t, err)
}

func TestCommandLine_Disconnect(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	cmdl.passwordReader = &passwordReaderMock{}

	// Create command and execute it to initialize command line flags.
	cmd, _ := cmdl.NewCmd()
	cmd.Execute()

	// Connect to server.
	err = cmdl.connect(cmd, []string{})
	assert.NoError(t, err, "connect to server failed")
	connected := cmdl.immuClient.IsConnected()
	assert.True(t, connected, "client is not connected after connect")

	// Disconnect form server.
	cmdl.disconnect(cmd, []string{})
	connected = cmdl.immuClient.IsConnected()
	assert.False(t, connected, "client is connected after disconnect")
}

func TestCommandLine_Logout(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	cmdl.passwordReader = &passwordReaderMock{}

	cmd, _ := cmdl.NewCmd()
	cmdl.Register(cmd)

	// Set arguments to execute the logout command.
	cmd.SetArgs([]string{"logout"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Execute the command.
	err = cmd.Execute()
	assert.NoError(t, err, "executing logout command failed")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "logged out")

}

func TestCommandLine_Connect_NonInteractive(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	pwr := passwordReaderMock{}
	cmdl.passwordReader = &pwr
	// Set the command line to run non interactive.
	cmdl.nonInteractive = true
	cmd, _ := cmdl.NewCmd()

	// Execute command to initialize command line flags.
	cmd.Execute()

	// Connecting has to fail, as the password may not be read from the
	// passwordReader in non interactive mode.
	err = cmdl.connect(cmd, []string{})
	assert.Error(t, err, "connecting to the database in non interactive mode without specifying a password file failed")
	assert.ErrorContains(t, err, "--password-file flag", "user was not informed that the password-file flag has to be used when running interactively")
	assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
	assert.Nil(t, cmdl.immuClient, "immuclient was initialized even though no password was provided")
}

func TestCommandLine_Connect_PasswordFile(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	pwr := passwordReaderMock{}
	cmdl.passwordReader = &pwr

	// Create a file containing the password for connecting to immudb.
	passwordFile := path.Join(tempDir, "immupass")

	// Create a command.
	cmd, _ := cmdl.NewCmd()

	// Set the flag to read the password from the file.
	cmd.SetArgs([]string{"--password-file", passwordFile})

	// Execute command to initialize command line flags.
	cmd.Execute()

	t.Run("not existing passwordfile", func(t *testing.T) {
		// Connecting should fail, as the password to connect to immudb
		// cannot be read from the specified file.
		err = cmdl.connect(cmd, []string{})
		assert.Error(t, err, "connecting to database without specifying a valid file for the --password-file flag succeeded")
		assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
		assert.Nil(t, cmdl.immuClient, "immuclient was initialized even though no password was provided")
	})

	t.Run("existing passwordfile - wrong password", func(t *testing.T) {
		// Remove the file with the password after the test.
		t.Cleanup(func() {
			os.Remove(passwordFile)
			cmdl.immuClient = nil
		})
		// Write an invalid password to the password file.
		err = os.WriteFile(passwordFile, []byte("invalid password"), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should fail, as the password to connect to immudb is wrong.
		err = cmdl.connect(cmd, []string{})
		assert.Error(t, err, "connecting to database without specifying a valid password in the password file succeeded")
		assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
		assert.NotNil(t, cmdl.immuClient, "immuclient was not initialized even though a password was provided")
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
		assert.Error(t, err, "connecting to database without specifying a valid password in the password file succeeded")
		assert.Contains(t, err.Error(), "exceeds the the maximal password length")
		assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
		assert.Nil(t, cmdl.immuClient, "immuclient was initialized even though no well formatted password was provided")
	})

	t.Run("existing passwordfile - password with new line", func(t *testing.T) {
		// Remove the file with the password after the test.
		t.Cleanup(func() {
			os.Remove(passwordFile)
			cmdl.immuClient = nil
		})
		// Write a valid password including a trailing new line to the password file.
		err = os.WriteFile(passwordFile, []byte(auth.SysAdminPassword+"\n"), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should fail, as the password to connect to immudb is wrong.
		err = cmdl.connect(cmd, []string{})
		assert.NoError(t, err, "connecting to database using the --password-file flag failed")
		assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
		if !assert.NotNil(t, cmdl.immuClient, "immuclient was not initialized even though a password was provided") {
			t.FailNow()
		}
		assert.True(t, cmdl.immuClient.IsConnected(), "immuclient is not connected despite connect function not yielding an error")
	})

	t.Run("valid passwordfile", func(t *testing.T) {
		// Remove the file with the password after the test.
		t.Cleanup(func() {
			os.Remove(passwordFile)
			cmdl.immuClient = nil
		})
		err = os.WriteFile(passwordFile, []byte(auth.SysAdminPassword), fs.ModePerm)
		if err != nil {
			t.Fatalf("writing password to file %s failed: %v", passwordFile, err)
		}

		// Connecting should be successful, as the password to connect to immudb
		// can be read from the specified file.
		err = cmdl.connect(cmd, []string{})
		assert.NoError(t, err, "connecting to database using the --password-file flag failed")
		assert.Equal(t, 0, pwr.Counter, "reading password from stdin was attempted despite command running non interactively")
		if !assert.NotNil(t, cmdl.immuClient, "immuclient was not initialized even though a password was provided") {
			t.FailNow()
		}
		assert.True(t, cmdl.immuClient.IsConnected(), "immuclient is not connected despite connect function not yielding an error")
	})
}

func TestCommandLine_Connect_Database(t *testing.T) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	assert.NoError(t, err, "starting Bufconn server for immudb failed")
	defer bs.Stop()

	opts := Options().
		WithDir(tempDir).
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
		})
	cmdl := NewCommandLine()
	cmdl.options = opts
	pwr := passwordReaderMock{}
	cmdl.passwordReader = &pwr

	// Create a command.
	cmd, _ := cmdl.NewCmd()

	// Set a flag to specify a non existing DB for the command.
	cmd.SetArgs([]string{"--database", "invalid_db"})

	// Execute command to initialize command line flags.
	cmd.Execute()

	// Connecting should be fail, as the database for which a session is opened,
	// does not exist.
	err = cmdl.connect(cmd, []string{})
	assert.Error(t, err, "connecting to database without specifying a valid file for the --password-file flag succeeded")
	assert.ErrorIs(t, err, database.ErrDatabaseNotExists)
	assert.NotNil(t, cmdl.immuClient, "immuclient was not initialized")

}
