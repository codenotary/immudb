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

package immuc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) Login(args []string) (string, error) {
	user := []byte(args[0])
	pass, err := i.passwordReader.Read("Password:")
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	response, err := i.ImmuClient.Login(ctx, user, pass)
	if err != nil {
		if strings.Contains(err.Error(), "authentication is disabled on server") {
			return "authentication is disabled on server", nil
		}
		return "", errors.New("username or password is not valid")
	}
	tokenFileName := i.ImmuClient.GetOptions().TokenFileName
	if err = i.hds.WriteFileToUserHomeDir(response.Token, tokenFileName); err != nil {
		return "", err
	}
	i.ImmuClient.GetOptions().Auth = true
	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	i.isLoggedin = true
	successMsg := "Successfully logged in."
	if len(response.Warning) != 0 {
		successMsg += fmt.Sprintf("\n%s", string(response.Warning))
	}
	return successMsg, nil
}

func (i *immuc) Logout(args []string) (string, error) {
	len, err := i.hds.ReadFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName)
	if err != nil || len == "" {
		return "User not logged in.", nil
	}
	if err := i.hds.DeleteFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName); err != nil {
		return "", err
	}
	i.isLoggedin = false
	i.ImmuClient.GetOptions().Auth = false

	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	return "Successfully logged out", nil
}
func (i *immuc) UserOperations(args []string) (string, error) {
	var command string
	if len(args) == 0 {
		command = "help"
	} else {
		command = args[0]
	}
	switch command {

	case "create":
		if len(args) < 4 {
			return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
		}
		username := args[1]
		permission := args[2]
		databasename := args[3]

		pass, err := i.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
		if err != nil {
			return "Error Reading Password", nil
		}
		if err = auth.IsStrongPassword(string(pass)); err != nil {
			return "Password does not meet the requirements. It must contain upper and lower case letter, digits, numbers, puntcuation mark or symbol.", nil
		}
		pass2, err := i.passwordReader.Read("Confirm password:")
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
			return "Permission value not recognized. Allowed permissions are read,readwrite,admin", nil
		}
		user, err := i.ImmuClient.CreateUser(context.Background(), []byte(username), pass, userpermission, databasename)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Created user %s", string(user.GetUser())), nil
	case "permission":

	}
	return "", fmt.Errorf("Wrong command. Get more information with 'user help'")
}
func (i *immuc) UserList(args []string) (string, error) {
	userlist, err := i.ImmuClient.ListUsers(context.Background())
	if err != nil {
		return "", err
	}
	fmt.Println()
	fmt.Println("User\tActive\tCreated By\tCreated At\t\t\t\t\tDatabase\tPermission")
	for _, val := range userlist.Users {
		fmt.Printf("%s\t%v\t%s\t\t%s\n", string(val.User), val.Active, val.Createdby, val.Createdat)
		for _, val := range val.Permissions {
			fmt.Printf("\t\t\t\t\t\t\t\t\t\t%s\t\t", val.Database)
			switch val.Permission {
			case auth.PermissionAdmin:
				fmt.Printf("Admin\n")
			case auth.PermissionSysAdmin:
				fmt.Printf("System Admin\n")
			case auth.PermissionR:
				fmt.Printf("Read\n")
			case auth.PermissionRW:
				fmt.Printf("Read/Write\n")
			default:
				return "Permission value not recognized. Allowed permissions are read,write,admin", nil
			}
		}
		fmt.Println()
	}
	return "", nil
}

func (i *immuc) ChangeUserPassword(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
	}
	username := args[0]
	var oldpass []byte
	var err error
	if username == auth.SysAdminUsername {
		oldpass, err = i.passwordReader.Read("Old password:")
		if err != nil {
			return "Error Reading Password", nil
		}
	}
	newpass, err := i.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "Error Reading Password", nil
	}
	if err = auth.IsStrongPassword(string(newpass)); err != nil {
		return "Password does not meet the requirements. It must contain upper and lower case letter, digits, numbers, puntcuation mark or symbol.", nil
	}
	pass2, err := i.passwordReader.Read("Confirm password:")
	if err != nil {
		return "Error Reading Password", nil
	}
	if !bytes.Equal(newpass, pass2) {
		return "Passwords don't match", nil
	}
	//old pass is not required any more
	if err := i.ImmuClient.ChangePassword(context.Background(), []byte(username), oldpass, []byte(newpass)); err != nil {
		return "", err
	}
	return fmt.Sprintf("Password of %s was changed successfuly", username), nil
}
func (i *immuc) SetActiveUser(args []string, active bool) (string, error) {
	if len(args) < 1 {
		return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
	}
	username := args[0]
	_, err := i.ImmuClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{
		Active:   active,
		Username: username,
	})
	if err != nil {
		return "", err
	}
	return "User status changed successfully", nil
}

func (i *immuc) SetUserPermission(args []string) (string, error) {
	if len(args) != 4 {
		return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
	}
	var permissionAction schema.PermissionAction
	switch args[0] {
	case "grant":
		permissionAction = schema.PermissionAction_GRANT
	case "revoke":
		permissionAction = schema.PermissionAction_REVOKE
	default:
		return "Wrong permission action. Only grant or revoke are allowed.", nil
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
		return "Permission value not recognized. Allowed permissions are read,readwrite,admin", nil
	}

	dbname := args[3]
	req := &schema.ChangePermissionRequest{
		Action:     permissionAction,
		Database:   dbname,
		Permission: userpermission,
		Username:   username,
	}
	resp, err := i.ImmuClient.ChangePermission(context.Background(), req)
	if err != nil {
		return "", err
	}
	return resp.Errormessage, nil
}
