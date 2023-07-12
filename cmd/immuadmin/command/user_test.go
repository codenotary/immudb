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
	"context"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserList(t *testing.T) {
	_, cmd := newTestCommandLine(t)
	// Set arguments to execute the user list command.
	cmd.SetArgs([]string{"user", "list"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Execute the command.
	err := cmd.Execute()
	assert.NoError(t, err, "Executing user list command should not fail.")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "immudb")
}

func TestUserListErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	cl := &commandline{
		immuClient: immuClientMock,
	}

	errListUsers := errors.New("list users error")
	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return nil, errListUsers
	}
	_, err := cl.userList(nil)
	require.Equal(t, errListUsers, err)

	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return &schema.UserList{
			Users: []*schema.User{
				{
					User: []byte("immudb"),
					Permissions: []*schema.Permission{
						{Database: "*", Permission: auth.PermissionSysAdmin},
					},
					Createdby: "immudb",
					Createdat: time.Now().String(),
					Active:    true,
				},
				{
					User: []byte("user1"),
					Permissions: []*schema.Permission{
						{Database: "db2", Permission: auth.PermissionAdmin},
						{Database: "db3", Permission: auth.PermissionR},
						{Database: "db4", Permission: auth.PermissionRW},
						{Database: "db5", Permission: 999},
					},
					Createdby: "immudb",
					Createdat: time.Now().String(),
					Active:    true,
				},
			},
		}, nil
	}
	resp, err := cl.userList(nil)
	require.NoError(t, err)
	require.Contains(t, resp, "unknown: 999")
}

func TestUserChangePassword(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)
	// Set arguments to change the password of the default user.
	cmd.SetArgs([]string{"user", "changepassword", "immudb"})

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}
	// Extend the list of password returned by the password reader.
	// Changing the password of the SysAdmin required the current SysAdmin
	// password. The other two passwords are newly chosen passwords.
	pwr.Passwords = append(pwr.Passwords, auth.SysAdminPassword, "MyUser@9", "MyUser@9")

	// Execute the command.
	err := cmd.Execute()
	assert.NoError(t, err, "Executing changepassword command should not fail.")

	out, err := ioutil.ReadAll(b)
	assert.NoError(t, err)
	assert.Contains(t, string(out), "immudb's password has been changed")
}

/*
func TestUserChangePasswordErrors(t *testing.T) {
	pwReaderMock := &clienttest.PasswordReaderMock{}
	immuClientMock := &clienttest.ImmuClientMock{}
	cl := &commandline{
		passwordReader: pwReaderMock,
		immuClient:     immuClientMock,
	}

	username := "user1"
	oldPass := []byte("Oldpa$$1")

	pwReaderMock.ReadF = func(string) ([]byte, error) {
		return nil, errors.New("password read error")
	}
	_, _, err := cl.changeUserPassword(username, oldPass)
	require.Equal(t, errors.New("Error Reading Password"), err)

	pwReaderMock.ReadF = func(string) ([]byte, error) {
		return []byte("weakpass"), nil
	}
	_, _, err = cl.changeUserPassword(username, oldPass)
	require.Equal(
		t,
		errors.New("password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol"),
		err)

	pwReadCounter := 0
	goodPass1 := []byte("GoodPass1!")
	pwReaderMock.ReadF = func(string) ([]byte, error) {
		pwReadCounter++
		if pwReadCounter == 1 {
			return goodPass1, nil
		}
		return nil, errors.New("password read 2 error")
	}
	_, _, err = cl.changeUserPassword(username, oldPass)
	require.Equal(t, errors.New("Error Reading Password"), err)

	pwReadCounter = 0
	pwReaderMock.ReadF = func(string) ([]byte, error) {
		pwReadCounter++
		if pwReadCounter == 1 {
			return goodPass1, nil
		}
		return []byte("GoodPass2!"), nil
	}
	_, _, err = cl.changeUserPassword(username, oldPass)
	require.Equal(t, errors.New("Passwords don't match"), err)

	pwReaderMock.ReadF = func(string) ([]byte, error) {
		return goodPass1, nil
	}
	errChangePass := errors.New("Change password error")
	immuClientMock.ChangePasswordF = func(context.Context, []byte, []byte, []byte) error {
		return errChangePass
	}
	_, _, err = cl.changeUserPassword(username, oldPass)
	require.Equal(t, errChangePass, err)

	immuClientMock.ChangePasswordF = func(context.Context, []byte, []byte, []byte) error {
		return nil
	}
	resp, newPass, err := cl.changeUserPassword(username, oldPass)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%s's password has been changed", username), resp)
	require.Equal(t, string(goodPass1), string(newPass))
}
*/

func TestNewUser(t *testing.T) {
	cmdl, cmd := newTestCommandLine(t)

	// Set a buffer to read the command output.
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}

	t.Run("create", func(t *testing.T) {
		// Set arguments to create a new user with access to the default database.
		cmd.SetArgs([]string{"user", "create", "newuser", "read", "defaultdb"})

		// Extend the list of password returned by the password reader.
		// The two passwords are newly chosen password and the confirmation of it.
		pwr.Passwords = append(pwr.Passwords, "MyUser@9", "MyUser@9")

		// Execute the command.
		err := cmd.Execute()
		assert.NoError(t, err, "Executing create new user command should not fail.")

		out, err := ioutil.ReadAll(b)
		assert.NoError(t, err)
		assert.Contains(t, string(out), "Created user newuser")
	})

	t.Run("activate", func(t *testing.T) {
		// Reset the output buffer.
		b.Reset()

		// Reset the password reader.
		pwr.Reset()

		// Set arguments to activate the new user.
		cmd.SetArgs([]string{"user", "activate", "newuser"})

		// Execute the command.
		err := cmd.Execute()
		assert.NoError(t, err, "Executing activate user command should not fail.")

		out, err := ioutil.ReadAll(b)
		assert.NoError(t, err)
		assert.Contains(t, string(out), "User status changed successfully")
	})

	t.Run("permission", func(t *testing.T) {
		// Reset the output buffer.
		b.Reset()

		// Reset the password reader.
		pwr.Reset()

		// Set arguments to change the permissions of the new user.
		cmd.SetArgs([]string{"user", "permission", "grant", "newuser", "readwrite", "defaultdb"})

		// Execute the command.
		err := cmd.Execute()
		assert.NoError(t, err, "Executing permission grant command should not fail.")

		out, err := ioutil.ReadAll(b)
		assert.NoError(t, err)
		assert.Contains(t, string(out), "Permission changed successfully")
	})

	t.Run("deactivate", func(t *testing.T) {
		// Reset the output buffer.
		b.Reset()

		// Reset the password reader.
		pwr.Reset()

		// Set arguments to deactivate the new user again.
		cmd.SetArgs([]string{"user", "deactivate", "newuser"})

		// Execute the command.
		err := cmd.Execute()
		assert.NoError(t, err, "Executing deactivate user command should not fail.")

		out, err := ioutil.ReadAll(b)
		assert.NoError(t, err)
		assert.Contains(t, string(out), "User status changed successfully")
	})

}

/*
func TestUserCreateErrors(t *testing.T) {
	pwReaderMock := &clienttest.PasswordReaderMock{}
	immuClientMock := &clienttest.ImmuClientMock{}
	cl := &commandline{
		passwordReader: pwReaderMock,
		immuClient:     immuClientMock,
	}

	errListUsers := errors.New("list users error")
	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return nil, errListUsers
	}
	username := "user1"
	databasename := "defaultdb"
	permission := "admin"
	args := []string{username, permission, databasename}
	_, err := cl.userCreate(args)
	require.Equal(t, errListUsers, err)

	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return &schema.UserList{
			Users: []*schema.User{&schema.User{User: []byte(username)}},
		}, nil
	}
	_, err = cl.userCreate(args)
	require.Equal(t, fmt.Errorf("User %s already exists", username), err)

	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return nil, nil
	}
	errListDatabases := errors.New("list databases error")
	immuClientMock.DatabaseListF = func(context.Context) (*schema.DatabaseListResponse, error) {
		return nil, errListDatabases
	}
	_, err = cl.userCreate(args)
	require.Equal(t, errListDatabases, err)

	immuClientMock.DatabaseListF = func(context.Context) (*schema.DatabaseListResponse, error) {
		return &schema.DatabaseListResponse{
			Databases: []*schema.Database{&schema.Database{Databasename: "sysdb"}},
		}, nil
	}
	_, err = cl.userCreate(args)
	require.Equal(t, fmt.Errorf("Database %s does not exist", databasename), err)

	immuClientMock.DatabaseListF = func(context.Context) (*schema.DatabaseListResponse, error) {
		return &schema.DatabaseListResponse{
			Databases: []*schema.Database{
				&schema.Database{Databasename: "sysdb"},
				&schema.Database{Databasename: databasename},
			},
		}, nil
	}
	args[1] = "UnknownPermission"
	_, err = cl.userCreate(args)
	require.Equal(
		t,
		fmt.Errorf(
			"Permission %s not recognized: allowed permissions are read, readwrite, admin",
			args[1]), err)

	args[1] = permission
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		return nil, errors.New("password reading error")
	}
	_, err = cl.userCreate(args)
	require.Equal(t, errors.New("Error Reading Password"), err)

	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		return []byte("weakpassword"), nil
	}
	_, err = cl.userCreate(args)
	require.Equal(
		t,
		errors.New("Password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol"),
		err)

	pwReadCounter := 0
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		pwReadCounter++
		if pwReadCounter == 1 {
			return []byte("$trongPass1!"), nil
		}
		return nil, errors.New("password reading error 2")
	}
	_, err = cl.userCreate(args)
	require.Equal(t, errors.New("Error Reading Password"), err)

	pwReadCounter = 0
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		pwReadCounter++
		if pwReadCounter == 1 {
			return []byte("$trongPass1!"), nil
		}
		return []byte("$trongPass2!"), nil
	}
	_, err = cl.userCreate(args)
	require.Equal(t, errors.New("Passwords don't match"), err)

	errCreateUser := errors.New("create user error")
	immuClientMock.CreateUserF = func(context.Context, []byte, []byte, uint32, string) error {
		return errCreateUser
	}
	_, err = cl.userCreate(args)
	require.Equal(t, errCreateUser, err)

	immuClientMock.CreateUserF = func(context.Context, []byte, []byte, uint32, string) error {
		return nil
	}
	resp, err := cl.userCreate(args)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("Created user %s", username), resp)
}
*/

/*

func TestUserDeactivate(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()
	defer bs.Stop()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}

	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	cliopt := Options().WithDialOptions(dialOptions).WithPasswordReader(pr)

	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", token.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	clientb, _ = client.NewImmuClient(cliopt)
	err = clientb.CreateDatabase(ctx, &schema.Database{
		Databasename: "mydb",
	})
	require.NoError(t, err)
	err = clientb.CreateUser(ctx, []byte("myuser"), []byte("MyUser@9"), auth.PermissionAdmin, "defaultdb")
	require.NoError(t, err)
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
	}

	cmd, _ := cmdl.NewCmd()
	cmdl.user(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "deactivate", "myuser"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	usrcmd := cmd.Commands()[0]
	usrcmd.PersistentPreRunE = nil

	err = cmd.Execute()
	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "User status changed successfully")
}

func TestUserActivateErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	cl := &commandline{
		immuClient: immuClientMock,
	}

	errSetActiveUser := errors.New("set active user error")
	immuClientMock.SetActiveUserF = func(context.Context, *schema.SetActiveUserRequest) error {
		return errSetActiveUser
	}
	_, err := cl.setActiveUser([]string{"user1"}, true)
	require.Equal(t, errSetActiveUser, err)
}

func TestUserPermission(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()
	defer bs.Stop()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}

	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	cliopt := Options().WithDialOptions(dialOptions).WithPasswordReader(pr)

	clientb, _ := client.NewImmuClient(cliopt)

	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", token.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	clientb, _ = client.NewImmuClient(cliopt)
	err = clientb.CreateDatabase(ctx, &schema.Database{
		Databasename: "mydb",
	})
	require.NoError(t, err)
	err = clientb.CreateUser(ctx, []byte("myuser"), []byte("MyUser@9"), auth.PermissionAdmin, "defaultdb")
	require.NoError(t, err)
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
	}

	cmd, _ := cmdl.NewCmd()
	cmdl.user(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "permission", "grant", "myuser", "readwrite", "mydb"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	usrcmd := cmd.Commands()[0]
	usrcmd.PersistentPreRunE = nil

	err = cmd.Execute()

	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "Permission changed successfully")
}

func TestUserPermissionErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	cl := &commandline{
		immuClient: immuClientMock,
	}

	args := []string{"UnknownPermissionAction", "user1", "read", "db1"}
	_, err := cl.setUserPermission(args)
	require.Equal(
		t,
		fmt.Errorf("wrong permission action. Only grant or revoke are allowed. Provided: %s", args[0]),
		err)

	args[0] = "revoke"
	args[2] = "UnknownPermission"
	_, err = cl.setUserPermission(args)
	require.Equal(
		t,
		fmt.Errorf(
			"Permission %s not recognized: allowed permissions are read, readwrite, admin",
			args[2]),
		err)

	args[2] = "read"
	errChangePermission := errors.New("change permission error")
	immuClientMock.ChangePermissionF = func(context.Context, schema.PermissionAction, string, string, uint32) error {
		return errChangePermission
	}
	_, err = cl.setUserPermission(args)
	require.Equal(t, errChangePermission, err)
}
*/
