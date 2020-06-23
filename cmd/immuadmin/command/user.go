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

package immuadmin

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
)

func (cl *commandline) user(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "user command",
		Short:             "Issue all user commands",
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"help", "list", "create", "permission grant", "permission revoke", "change password", "activate", "deactivate"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.UserOperations(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.MaximumNArgs(5),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) UserOperations(args []string) (string, error) {
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
		userlist, err := cl.immuClient.ListUsers(context.Background())
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
			return "Incorrect number of parameters for the command. Please type 'user help' for more information.", nil
		}
		username := args[1]
		permission := args[2]
		databasename := args[3]

		pass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
		if err != nil {
			return "Error Reading Password", nil
		}
		if err = auth.IsStrongPassword(string(pass)); err != nil {
			return "Password does not meet the requirements. It must contain upper and lower case letter, digits, numbers, puntcuation mark or symbol.", nil
		}
		pass2, err := cl.passwordReader.Read("Confirm password:")
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
		user, err := cl.immuClient.CreateUser(context.Background(), []byte(username), pass, userpermission, databasename)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Created user %s", string(user.GetUser())), nil
	case "changepassword":
		if len(args) != 2 {
			return "Incorrect number of parameters for the command. Please type 'user help' for more information.", nil
		}
		username := args[1]
		newpass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
		if err != nil {
			return "Error Reading Password", nil
		}
		if err = auth.IsStrongPassword(string(newpass)); err != nil {
			return "Password does not meet the requirements. It must contain upper and lower case letter, digits, numbers, puntcuation mark or symbol.", nil
		}
		pass2, err := cl.passwordReader.Read("Confirm password:")
		if err != nil {
			return "Error Reading Password", nil
		}
		if !bytes.Equal(newpass, pass2) {
			return "Passwords don't match", nil
		}
		//old pass is not required any more
		if err := cl.immuClient.ChangePassword(context.Background(), []byte(username), []byte{}, []byte(newpass)); err != nil {
			return "", err
		}
		return fmt.Sprintf("Password of %s was changed successfuly", username), nil
	case "permission":
		if len(args) != 5 {
			return "Incorrect number of parameters for the command. Please type 'user help' for more information.", nil
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
		resp, err := cl.immuClient.ChangePermission(context.Background(), req)
		if err != nil {
			return "", err
		}
		return resp.Errormessage, nil
	case "activate", "deactivate":
		if len(args) < 2 {
			return "Incorrect number of parameters for the command. Please type 'user help' for more information.", nil
		}
		username := args[1]
		var active bool
		switch args[0] {
		case "activate":
			active = true
		case "deactivate":
			active = false
		}

		_, err := cl.immuClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{
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

func (cl *commandline) changePassword(username string, oldPass []byte) {
	fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
	var err error
	if username == auth.SysAdminUsername && len(oldPass) <= 0 {
		oldPass, err = cl.passwordReader.Read("Old password:")
		if err != nil {
			c.QuitToStdErr(err)
		}
		_, err = cl.immuClient.Login(cl.context, []byte(username), oldPass)
		if err != nil {
			c.QuitToStdErr(err)
		}
	}
	pass, err := cl.passwordReader.Read("New password:")
	if err != nil {
		c.QuitToStdErr(err)
	}
	if err = auth.IsStrongPassword(string(pass)); err != nil {
		c.QuitToStdErr(err)
	}
	pass2, err := cl.passwordReader.Read("Confirm new password:")
	if err != nil {
		c.QuitToStdErr(err)
	}
	if !bytes.Equal(pass, pass2) {
		c.QuitToStdErr(errors.New("Passwords don't match"))
	}
	if username == auth.SysAdminUsername {
		if bytes.Equal(pass, oldPass) {
			c.QuitToStdErr(errors.New("New password is identical to the old one"))
		}
	}
	if err = cl.immuClient.ChangePassword(cl.context, []byte(username), oldPass, pass); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("Password changed for user %s\n", username)
}
