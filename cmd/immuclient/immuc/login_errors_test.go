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

package immuc

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
)

func TestLoginAndUserCommandsErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	passwordReaderMock := &clienttest.PasswordReaderMock{}
	homedirServiceMock := clienttest.DefaultHomedirServiceMock()
	ic := new(immuc)
	ic.ImmuClient = immuClientMock
	ic.passwordReader = passwordReaderMock
	ic.ts = tokenservice.NewTokenService().WithHds(homedirServiceMock)

	// Login errors
	passwordReadErr := errors.New("Password read error")
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return nil, passwordReadErr
	}
	args := []string{"user1"}
	_, err := ic.Login(args)
	require.Equal(t, passwordReadErr, err)
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return []byte("pass1"), nil
	}

	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, errors.New("authentication is disabled on server")
	}
	resp, err := ic.Login(args)
	require.NoError(t, err)
	require.Equal(t, "authentication is disabled on server", resp)

	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: "token1"}, nil
	}
	immuClientMock.GetOptionsF = func() *client.Options {
		return &client.Options{TokenFileName: "TestLoginErrors_token"}
	}
	errWriteFileToHomeDir := errors.New("write file to home dir error")
	homedirServiceMock.WriteFileToUserHomeDirF = func([]byte, string) error {
		return errWriteFileToHomeDir
	}
	_, err = ic.Login(args)
	require.ErrorIs(t, err, errWriteFileToHomeDir)

	homedirServiceMock.WriteFileToUserHomeDirF = func([]byte, string) error {
		return nil
	}

	// Logout errors
	errReadFileFromHomeDir := errors.New("read file from home dir error")
	homedirServiceMock.ReadFileFromUserHomeDirF = func(string) (string, error) {
		return "", errReadFileFromHomeDir
	}
	resp, err = ic.Logout(nil)
	require.NoError(t, err)
	require.Equal(t, "User not logged in.", resp)
	homedirServiceMock.ReadFileFromUserHomeDirF = func(string) (string, error) {
		return string(client.BuildToken("", "token1")), nil
	}

	errDeleteFileFromHomeDir := errors.New("delete file from home dir error")
	homedirServiceMock.DeleteFileFromUserHomeDirF = func(string) error {
		return errDeleteFileFromHomeDir
	}
	homedirServiceMock.FileExistsInUserHomeDirF = func(string) (bool, error) {
		return true, nil
	}
	_, err = ic.Logout(nil)
	require.ErrorIs(t, err, errDeleteFileFromHomeDir)

	// UserCreate errors
	resp, err = ic.UserCreate(nil)
	require.NoError(t, err)
	require.Equal(t, "incorrect number of parameters for this command. Please type 'user help' for more information", resp)

	args = []string{"user1", "readwrite", "db1"}
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return nil, passwordReadErr
	}
	resp, err = ic.UserCreate(args)
	require.NoError(t, err)
	require.Equal(t, "Error Reading Password", resp)
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return []byte("pass1"), nil
	}

	resp, err = ic.UserCreate(args)
	require.NoError(t, err)
	require.Equal(
		t,
		"password does not meet the requirements. It must contain upper and lower "+
			"case letters, digits, punctuation mark or symbol",
		resp)
	passwordReadCounter := 0
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		passwordReadCounter++
		if passwordReadCounter == 1 {
			return []byte("$omePass1"), nil
		}
		return nil, passwordReadErr
	}
	resp, err = ic.UserCreate(args)
	require.NoError(t, err)
	require.Equal(t, "Error Reading Password", resp)

	passwordReadCounter = 0
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		passwordReadCounter++
		if passwordReadCounter == 1 {
			return []byte("$omePass1"), nil
		}
		return []byte("$omePass2"), nil
	}
	resp, err = ic.UserCreate(args)
	require.NoError(t, err)
	require.Equal(t, "Passwords don't match", resp)

	// UserList errors
	errListUsers := errors.New("list users error")
	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return nil, errListUsers
	}
	_, err = ic.UserList(nil)
	require.ErrorIs(t, err, errListUsers)

	userList := &schema.UserList{
		Users: []*schema.User{
			&schema.User{
				User: []byte("user1"),
				Permissions: []*schema.Permission{
					&schema.Permission{
						Database:   "db1",
						Permission: auth.PermissionAdmin,
					},
					&schema.Permission{
						Database:   "db2",
						Permission: auth.PermissionSysAdmin,
					},
					&schema.Permission{
						Database:   "db3",
						Permission: auth.PermissionR,
					},
					&schema.Permission{
						Database:   "db4",
						Permission: auth.PermissionRW,
					},
					&schema.Permission{
						Database:   "db5",
						Permission: auth.PermissionNone,
					},
				},
				Createdby: "admin",
				Createdat: "2020-07-29",
				Active:    true,
			},
		},
	}
	immuClientMock.ListUsersF = func(context.Context) (*schema.UserList, error) {
		return userList, nil
	}
	resp, err = ic.UserList(nil)
	require.NoError(t, err)
	require.Equal(t, "permission value not recognized. Allowed permissions are read, write, admin", resp)

	userList.Users[0].Permissions = userList.Users[0].Permissions[0:4]
	resp, err = ic.UserList(nil)
	require.NoError(t, err)
	require.Contains(t, resp, "user1")
	require.Contains(t, resp, "db1")
	require.Contains(t, resp, "db2")
	require.Contains(t, resp, "db3")
	require.Contains(t, resp, "db4")

	// ChangeUserPassword errors
	_, err = ic.ChangeUserPassword(nil)
	require.EqualError(t, err, "ERROR: Not enough arguments. Use [command] --help for documentation ")

	args = []string{auth.SysAdminUsername}
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return nil, passwordReadErr
	}
	resp, err = ic.ChangeUserPassword(args)
	require.NoError(t, err)
	require.Equal(t, "Error Reading Password", resp)

	passwordReadCounter = 0
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		passwordReadCounter++
		if passwordReadCounter == 1 {
			return []byte("pass1"), nil
		}
		return nil, passwordReadErr
	}
	resp, err = ic.ChangeUserPassword(args)
	require.NoError(t, err)
	require.Equal(t, "Error Reading Password", resp)

	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		return []byte("pass1"), nil
	}
	resp, err = ic.ChangeUserPassword(args)
	require.NoError(t, err)
	require.Equal(
		t,
		"password does not meet the requirements. It must contain upper and lower "+
			"case letters, digits, punctuation mark or symbol",
		resp)

	passwordReadCounter = 0
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		passwordReadCounter++
		if passwordReadCounter < 3 {
			return []byte("$omePass1"), nil
		}
		return nil, passwordReadErr
	}
	resp, err = ic.ChangeUserPassword(args)
	require.NoError(t, err)
	require.Equal(t, "Error Reading Password", resp)

	passwordReadCounter = 0
	passwordReaderMock.ReadF = func(msg string) ([]byte, error) {
		passwordReadCounter++
		if passwordReadCounter < 3 {
			return []byte("$omePass1"), nil
		}
		return []byte("$omePass2"), nil
	}
	resp, err = ic.ChangeUserPassword(args)
	require.NoError(t, err)
	require.Equal(t, "Passwords don't match", resp)

	// SetActiveUser errors
	resp, err = ic.SetActiveUser(nil, true)
	require.NoError(t, err)
	require.Equal(
		t,
		"incorrect number of parameters for this command. Please type 'user help' for more information",
		resp)

	errSetActiveUser := errors.New("set active user error")
	immuClientMock.SetActiveUserF = func(context.Context, *schema.SetActiveUserRequest) error {
		return errSetActiveUser
	}
	_, err = ic.SetActiveUser([]string{"user1"}, true)
	require.ErrorIs(t, err, errSetActiveUser)

	// SetUserPermission errors
	resp, err = ic.SetUserPermission(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		"incorrect number of parameters for this command. Please type 'user help' for more information",
		resp)

	args = []string{"default", "user1", "readwrite", "db1"}
	resp, err = ic.SetUserPermission(args)
	require.NoError(t, err)
	require.Equal(t, "wrong permission action. Only grant or revoke are allowed", resp)

	args[0] = "revoke"
	args[2] = "default"
	resp, err = ic.SetUserPermission(args)
	require.NoError(t, err)
	require.Equal(t, "permission value not recognized. Allowed permissions are read, readwrite, admin", resp)

	args[2] = "readwrite"
	errChangePermission := errors.New("change permission error")
	immuClientMock.ChangePermissionF = func(context.Context, schema.PermissionAction, string, string, uint32) error {
		return errChangePermission
	}
	_, err = ic.SetUserPermission(args)
	require.ErrorIs(t, err, errChangePermission)
}
*/
