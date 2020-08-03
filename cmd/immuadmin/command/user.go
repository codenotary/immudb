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
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
)

func (cl *commandline) user(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "user command",
		Short:   "Issue all user commands",
		Aliases: []string{"u"},
	}
	userListCmd := &cobra.Command{
		Use:               "list",
		Short:             "List all users",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.userList(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MaximumNArgs(0),
	}
	userCreate := &cobra.Command{
		Use:               "create",
		Short:             "Create a new user",
		Long:              "Create a new user inside a database with permissions",
		Example:           "immuadmin user create michele read mydb",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.userCreate(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.RangeArgs(2, 3),
	}
	userChangePassword := &cobra.Command{
		Use:               "changepassword",
		Short:             "Change user password",
		Example:           "immuadmin user changepassword michele",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			username := args[0]
			var resp = ""
			var oldpass []byte
			if username == auth.SysAdminUsername {
				oldpass, err = cl.passwordReader.Read("Old password:")
				if err != nil {
					return fmt.Errorf("Error Reading Password")
				}
			}
			if resp, err = cl.changeUserPassword(username, oldpass); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userActivate := &cobra.Command{
		Use:               "activate",
		Short:             "Activate a user",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var resp = ""
			if resp, err = cl.setActiveUser(args, true); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userDeactivate := &cobra.Command{
		Use:               "deactivate",
		Short:             "Deactivate a user",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var resp = ""
			if resp, err = cl.setActiveUser(args, false); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), resp)
			}
			return err
		},
		Args: cobra.ExactArgs(1),
	}
	userPermission := &cobra.Command{
		Use:               "permission [grant|revoke] {username} [read|readwrite|admin] {database}",
		Short:             "Set user permission",
		Example:           "immuadmin user permission grant michele readwrite mydb",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if _, err = cl.setUserPermission(args); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), "Permission changed successfully")
			}
			return err
		},
		Args: cobra.ExactValidArgs(4),
	}
	ccmd.AddCommand(userListCmd)
	ccmd.AddCommand(userCreate)
	ccmd.AddCommand(userChangePassword)
	ccmd.AddCommand(userActivate)
	ccmd.AddCommand(userDeactivate)
	ccmd.AddCommand(userPermission)
	cmd.AddCommand(ccmd)
}

func (cl *commandline) changeUserPassword(username string, oldpassword []byte) (string, error) {
	newpass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "Error Reading Password", nil
	}
	if err = auth.IsStrongPassword(string(newpass)); err != nil {
		return "", fmt.Errorf("password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol")
	}
	pass2, err := cl.passwordReader.Read("Confirm password:")
	if err != nil {
		return "", fmt.Errorf("Error Reading Password")
	}
	if !bytes.Equal(newpass, pass2) {
		return "", fmt.Errorf("Passwords don't match")
	}
	if err := cl.immuClient.ChangePassword(context.Background(), []byte(username), oldpassword, []byte(newpass)); err != nil {
		return "", err
	}
	return fmt.Sprintf("Password of %s was changed successfuly", username), nil
}
func (cl *commandline) userList(args []string) (string, error) {
	userlist, err := cl.immuClient.ListUsers(context.Background())
	if err != nil {
		return "", err
	}
	var users string
	users += fmt.Sprintf("\nUser\tActive\tCreated By\tCreated At\t\t\t\t\tDatabase\tPermission\n")
	for _, val := range userlist.Users {
		users += fmt.Sprintf("%s\t%v\t%s\t\t%v\n", string(val.User), val.Active, val.Createdby, val.Createdat)
		for _, val := range val.Permissions {
			users += fmt.Sprintf("\t\t\t\t\t\t\t\t\t\t%s\t\t", val.Database)
			switch val.Permission {
			case auth.PermissionAdmin:
				users += fmt.Sprintf("Admin\n")
			case auth.PermissionSysAdmin:
				users += fmt.Sprintf("System Admin\n")
			case auth.PermissionR:
				users += fmt.Sprintf("Read\n")
			case auth.PermissionRW:
				users += fmt.Sprintf("Read/Write\n")
			default:
				return "", fmt.Errorf("permission value not recognized. Allowed permissions are read, readwrite, admin")
			}
		}
		users += fmt.Sprintf("\n")
	}
	return users, nil
}
func (cl *commandline) userCreate(args []string) (string, error) {
	username := args[0]
	permission := args[1]
	var databasename string
	if len(args) == 3 {
		databasename = args[2]
	}

	pass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		return "", fmt.Errorf("Error Reading Password")
	}
	if err = auth.IsStrongPassword(string(pass)); err != nil {
		return "", fmt.Errorf("password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol")
	}
	pass2, err := cl.passwordReader.Read("Confirm password:")
	if err != nil {
		return "", fmt.Errorf("Error Reading Password")
	}
	if !bytes.Equal(pass, pass2) {
		return "", fmt.Errorf("Passwords don't match")
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
		return "", fmt.Errorf("permission value not recognized. Allowed permissions are read, readwrite, admin")
	}
	user, err := cl.immuClient.CreateUser(context.Background(), []byte(username), pass, userpermission, databasename)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Created user %s", string(user.GetUser())), nil
}

func (cl *commandline) setActiveUser(args []string, active bool) (string, error) {
	username := args[0]
	_, err := cl.immuClient.SetActiveUser(context.Background(), &schema.SetActiveUserRequest{
		Active:   active,
		Username: username,
	})
	if err != nil {
		return "", err
	}
	return "User status changed successfully", nil
}

func (cl *commandline) setUserPermission(args []string) (resp string, err error) {
	var permissionAction schema.PermissionAction
	switch args[0] {
	case "grant":
		permissionAction = schema.PermissionAction_GRANT
	case "revoke":
		permissionAction = schema.PermissionAction_REVOKE
	default:
		return "", fmt.Errorf("wrong permission action. Only grant or revoke are allowed. Provided: %s", args[0])
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
		return "", fmt.Errorf("permission value not recognized. Allowed permissions are read, readwrite, admin. Provided: %s", args[2])
	}
	dbname := args[3]

	return "", cl.immuClient.ChangePermission(context.Background(), permissionAction, username, dbname, userpermission)
}
