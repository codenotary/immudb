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

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/status"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) Login(args []string) (string, error) {
	var user []byte
	if len(args) >= 1 {
		user = []byte(args[0])
	} else if len(i.options.immudbClientOptions.Username) > 0 {
		user = []byte(i.options.immudbClientOptions.Username)
	} else {
		return "", errors.New("please specify a username")
	}

	var pass []byte
	var err error
	if len(i.options.immudbClientOptions.Password) == 0 {
		pass, err = i.options.immudbClientOptions.PasswordReader.Read("Password:")
		if err != nil {
			return "", err
		}
	} else {
		pass = []byte(i.options.immudbClientOptions.Password)
	}

	ctx := context.Background()
	response, err := i.ImmuClient.Login(ctx, user, pass)
	if err != nil {
		if strings.Contains(err.Error(), "authentication disabled") {
			return "", errors.New("authentication is disabled on server")
		}
		return "", err
	}

	i.isLoggedin = true
	successMsg := "Successfully logged in\n"
	if len(response.Warning) != 0 {
		successMsg += string(response.Warning)
	}
	return successMsg, nil
}

func (i *immuc) Logout(args []string) (string, error) {
	var err error
	i.isLoggedin = false
	err = i.ImmuClient.Logout(context.Background())
	st, ok := status.FromError(err)
	if ok && st.Message() == "not logged in" {
		return "User not logged in", nil
	}
	if err != nil {
		return "", err
	}
	return "Successfully logged out", nil
}

func (i *immuc) UserCreate(args []string) (string, error) {
	if len(args) < 3 {
		return "incorrect number of parameters for this command. Please type 'user help' for more information", nil
	}
	username := args[0]
	permission := args[1]
	databasename := args[2]

	pass, err := i.options.immudbClientOptions.PasswordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "Error Reading Password", nil
	}
	if err = auth.IsStrongPassword(string(pass)); err != nil {
		return "password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol", nil
	}
	pass2, err := i.options.immudbClientOptions.PasswordReader.Read("Confirm password:")
	if err != nil {
		return "Error Reading Password", nil
	}
	if !bytes.Equal(pass, pass2) {
		return "Passwords don't match", nil
	}
	var userpermission uint32
	switch permission {
	case "read":
		userpermission = auth.PermissionR
	case "admin":
		userpermission = auth.PermissionAdmin
	case "readwrite":
		userpermission = auth.PermissionRW
	default:
		return "permission value not recognized. Allowed permissions are read, readwrite, admin", nil
	}
	_, err = i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.CreateUser(
			context.Background(), []byte(username), pass, userpermission, databasename)
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Created user %s", username), nil
}

func (i *immuc) UserList(args []string) (string, error) {
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.ListUsers(context.Background())
	})
	if err != nil {
		return "", err
	}
	userList := response.(*schema.UserList)
	ris := "\n"
	ris += "User\tActive\tCreated By\tCreated At\t\t\t\t\tDatabase\tPermission"
	for _, val := range userList.Users {
		ris += fmt.Sprintf("%s\t%v\t%s\t\t%s\n", string(val.User), val.Active, val.Createdby, val.Createdat)
		for _, val := range val.Permissions {
			ris += fmt.Sprintf("\t\t\t\t\t\t\t\t\t\t%s\t\t", val.Database)
			switch val.Permission {
			case auth.PermissionAdmin:
				ris += "Admin\n"
			case auth.PermissionSysAdmin:
				ris += "System Admin\n"
			case auth.PermissionR:
				ris += "Read\n"
			case auth.PermissionRW:
				ris += "Read/Write\n"
			default:
				return "permission value not recognized. Allowed permissions are read, write, admin", nil
			}
		}
		ris += "\n"
	}
	return ris, nil
}

func (i *immuc) ChangeUserPassword(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
	}
	username := args[0]
	var oldpass []byte
	var err error
	if username == auth.SysAdminUsername {
		oldpass, err = i.options.immudbClientOptions.PasswordReader.Read("Old password:")
		if err != nil {
			return "Error Reading Password", nil
		}
	}
	newpass, err := i.options.immudbClientOptions.PasswordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "Error Reading Password", nil
	}
	if err = auth.IsStrongPassword(string(newpass)); err != nil {
		return "password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol", nil
	}
	pass2, err := i.options.immudbClientOptions.PasswordReader.Read("Confirm password:")
	if err != nil {
		return "Error Reading Password", nil
	}
	if !bytes.Equal(newpass, pass2) {
		return "Passwords don't match", nil
	}
	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.ChangePassword(
			context.Background(), []byte(username), oldpass, []byte(newpass))
	}); err != nil {
		return "", err
	}
	return fmt.Sprintf("Password of %s was successfully changed", username), nil
}

func (i *immuc) SetActiveUser(args []string, active bool) (string, error) {
	if len(args) < 1 {
		return "incorrect number of parameters for this command. Please type 'user help' for more information", nil
	}
	username := args[0]
	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{
			Active:   active,
			Username: username,
		})
	}); err != nil {
		return "", err
	}
	return "user status changed successfully", nil
}

func (i *immuc) SetUserPermission(args []string) (string, error) {
	if len(args) != 4 {
		return "incorrect number of parameters for this command. Please type 'user help' for more information", nil
	}
	var permissionAction schema.PermissionAction
	switch args[0] {
	case "grant":
		permissionAction = schema.PermissionAction_GRANT
	case "revoke":
		permissionAction = schema.PermissionAction_REVOKE
	default:
		return "wrong permission action. Only grant or revoke are allowed", nil
	}
	username := args[1]
	var userpermission uint32
	switch args[2] {
	case "read":
		userpermission = auth.PermissionR
	case "admin":
		userpermission = auth.PermissionAdmin
	case "readwrite":
		userpermission = auth.PermissionRW
	default:
		return "permission value not recognized. Allowed permissions are read, readwrite, admin", nil
	}

	dbname := args[3]

	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.ChangePermission(
			context.Background(), permissionAction, username, dbname, userpermission)
	}); err != nil {
		return "", err
	}
	return "permission changed successfully", nil
}
