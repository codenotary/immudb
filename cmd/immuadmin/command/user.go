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
	"errors"
	"fmt"
	"strings"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
)

func (cl *commandline) user(parentCmd *cobra.Command, parentCmdName string) {
	cmdName := "user"
	fullCmdName := parentCmdName + " " + cmdName
	usage := cmdName + " list|create|change-password|set-permission|delete [username] [read|readwrite]"
	usages := map[string][]string{
		"list":            []string{"List users", fullCmdName + " list"},
		"create":          []string{"Create a new user", fullCmdName + " create username read|readwrite"},
		"change-password": []string{"Change password", fullCmdName + " change-password username"},
		"set-permission":  []string{"Set permission", fullCmdName + " set-permission username read|readwrite"},
		"delete":          []string{"Delete user", fullCmdName + " delete username"},
	}
	validCommands := map[string]struct{}{
		"list":            struct{}{},
		"create":          struct{}{},
		"change-password": struct{}{},
		"set-permission":  struct{}{},
		"delete":          struct{}{},
	}
	requiredArgs := c.RequiredArgs{
		Cmd:    fullCmdName,
		Usage:  parentCmdName + " " + usage,
		Usages: usages,
	}
	ccmd := &cobra.Command{
		Use:               usage,
		Short:             "Perform various user-related operations: list, create, delete, change password, set permissions",
		Example:           c.UsageSprintf(usages),
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			action, err := requiredArgs.Require(args, 0, "a user action", "a valid user action", validCommands, "")
			if err != nil {
				c.QuitToStdErr(err)
			}
			switch action {
			case "list":
				cl.listUsers()
			case "create", "change-password", "set-permission", "delete":
				username, err := requiredArgs.Require(args, 1, "a username", "", map[string]struct{}{}, action)
				if err != nil {
					c.QuitToStdErr(err)
				}
				switch action {
				case "create", "set-permission":
					permissions, err := requiredArgs.Require(
						args,
						2,
						"user permissions",
						"some valid user permissions",
						map[string]struct{}{"read": struct{}{}, "readwrite": struct{}{}},
						action)
					if err != nil {
						c.QuitToStdErr(err)
					}
					switch action {
					case "create":
						cl.createUser(username, permissions)
					default:
						cl.setPermissions(username, permissions)
					}
				case "change-password":
					cl.changePassword(username)
				case "delete":
					cl.deleteUser(username)
				}
			}
			return nil
		},
	}
	parentCmd.AddCommand(ccmd)
}

func (cl *commandline) listUsers() {
	usersList, err := cl.immuClient.ListUsers(cl.context)
	if err != nil {
		c.QuitWithUserError(err)
	}
	if len(usersList.Items) <= 0 {
		fmt.Printf("No users found")
	}
	fmt.Println(len(usersList.Items), "user(s):")
	c.PrintTable(
		[]string{"Username", "Role", "Permissions"},
		len(usersList.Items),
		func(i int) []string {
			row := make([]string, 3)
			u := string(auth.TrimPermissionSuffix(usersList.Items[i].GetKey()))
			permission := auth.GetPermissionFromSuffix(usersList.Items[i].GetKey())
			row[0] = u
			if permission == auth.Permissions.Admin {
				row[1] = "admin"
				row[2] = "admin"
			} else {
				row[1] = "client"
				switch permission {
				case auth.Permissions.R:
					row[2] = "read"
				case auth.Permissions.RW:
					row[2] = "readwrite"
				default:
					row[2] = fmt.Sprintf("unknown: %d", permission)
				}
			}
			return row
		},
	)
}

func (cl *commandline) createUser(username string, permissions string) {
	fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
	pass, err := cl.passwordReader.Read(fmt.Sprintf("Choose a password for %s:", username))
	if err != nil {
		c.QuitToStdErr(err)
	}
	if err = auth.IsStrongPassword(string(pass)); err != nil {
		c.QuitToStdErr(errors.New("Password does not meet the requirements"))
	}
	pass2, err := cl.passwordReader.Read("Confirm password:")
	if err != nil {
		c.QuitToStdErr(err)
	}
	if !bytes.Equal(pass, pass2) {
		c.QuitToStdErr(errors.New("Passwords don't match"))
	}
	var permission []byte
	switch permissions {
	case "readwrite":
		permission = []byte{auth.Permissions.RW}
	default:
		permission = []byte{auth.Permissions.R}
	}
	_, err = cl.immuClient.CreateUser(cl.context, []byte(username), pass, permission)
	if err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("User %s created\n", username)
}

func (cl *commandline) setPermissions(username string, permissions string) {
	var permission []byte
	switch permissions {
	case "readwrite":
		permission = []byte{auth.Permissions.RW}
	default:
		permission = []byte{auth.Permissions.R}
	}
	if err := cl.immuClient.SetPermission(
		cl.context, []byte(username), permission); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("Permissions updated for user %s. "+
		"They will be in effect once the user logs out and in again.\n",
		username)
}

func (cl *commandline) changePassword(username string) {
	fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
	var err error
	oldPass := []byte{}
	if username == auth.AdminUsername {
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
	if username == auth.AdminUsername {
		if bytes.Equal(pass, oldPass) {
			c.QuitToStdErr(errors.New("New password is identical to the old one"))
		}
	}
	if err = cl.immuClient.ChangePassword(cl.context, []byte(username), oldPass, pass); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("Password changed for user %s\n", username)
}

func (cl *commandline) deleteUser(username string) {
	fmt.Printf("Are you sure you want to delete user %s? [y/N]: ", username)
	answer, err := c.ReadFromTerminalYN("N")
	if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
		c.QuitToStdErr("Canceled")
	}
	if err := cl.immuClient.DeleteUser(cl.context, []byte(username)); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("User %s has been deleted\n", username)
}
