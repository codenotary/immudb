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
	if err = client.WriteFileToUserHomeDir(response.Token, tokenFileName); err != nil {
		return "", err
	}
	i.ImmuClient.GetOptions().Auth = true
	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}
	i.isLoggedin = true

	successMsg := "Successfully logged in.\nSelect a database before for any further commands."
	if len(response.Warning) != 0 {
		successMsg += fmt.Sprintf("\n%s", string(response.Warning))
	}
	return successMsg, nil
}

func (i *immuc) Logout(args []string) (string, error) {
	len, err := client.ReadFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName)
	if err != nil || len == "" {
		return "User not logged in.", nil
	}
	client.DeleteFileFromUserHomeDir(i.ImmuClient.GetOptions().TokenFileName)
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
	case "help":
		fmt.Println("user list  -- shows all users and their details")
		fmt.Println()
		fmt.Println("user create user_name permission database_name  -- creates a user for the database, asks twice for the password")
		fmt.Println()
		fmt.Println("user changepassword username  -- asks to insert the new password twice")
		fmt.Println()
		fmt.Println("user permission grant/revoke username permission_type database_name  -- grants or revokes the permission for the database")
		fmt.Println()
		fmt.Println("user activate/deactivate username  -- activates or deactivates a user")
		fmt.Println()
		return "", nil
	case "list":
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
				case auth.PermissionW:
					fmt.Printf("Write\n")
				default:
					return "Permission value not recognized. Allowed permissions are read,write,admin", nil
				}
			}
			fmt.Println()
		}
		return "", nil
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
		case "write":
			userpermission = auth.PermissionW
		case "admin":
			userpermission = auth.PermissionAdmin
		default:
			return "Permission value not recognized. Allowed permissions are read,write,admin", nil
		}
		user, err := i.ImmuClient.CreateUser(context.Background(), []byte(username), pass, userpermission, databasename)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Created user %s", string(user.GetUser())), nil
	case "changepassword":
		if len(args) != 2 {
			return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
		}
		username := args[1]
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
		if err := i.ImmuClient.ChangePassword(context.Background(), []byte(username), []byte{}, []byte(newpass)); err != nil {
			return "", err
		}
		return fmt.Sprintf("Password of %s was changed successfuly", username), nil
	case "permission":
		if len(args) != 5 {
			return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
		}
		var permissionAction schema.PermissionAction
		switch args[1] {
		case "grant":
			permissionAction = schema.PermissionAction_GRANT
		case "revoke":
			permissionAction = schema.PermissionAction_REVOKE
		default:
			return "Wrong permission action. Only grant or revoke are allowed.", nil
		}
		username := args[2]
		var userpermission uint32
		switch args[3] {
		case "read":
			userpermission = auth.PermissionR
		case "write":
			userpermission = auth.PermissionW
		case "admin":
			userpermission = auth.PermissionAdmin
		default:
			return "Permission value not recognized. Allowed permissions are read,write,admin", nil
		}

		dbname := args[4]
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
	case "activate", "deactivate":
		if len(args) < 2 {
			return "Incorrect number of parameters for this command. Please type 'user help' for more information.", nil
		}
		username := args[1]
		var active bool
		switch args[0] {
		case "activate":
			active = true
		case "deactivate":
			active = false
		}

		_, err := i.ImmuClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{
			Active:   active,
			Username: username,
		})
		if err != nil {
			return "", err
		}
		return "User status changed successfully", nil
	}
	return "", fmt.Errorf("Wrong command. Get more information with 'user help'")
}
