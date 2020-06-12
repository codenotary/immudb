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
	usage := cmdName + " list|create|change-password|set-permission|deactivate [username] [read|readwrite]"
	usages := map[string][]string{
		"list":            {"List users", fullCmdName + " list"},
		"create":          {"Create a new user", fullCmdName + " create username read|readwrite"},
		"change-password": {"Change password", fullCmdName + " change-password username"},
		"set-permission":  {"Set permission", fullCmdName + " set-permission username read|readwrite"},
		"deactivate":      {"Deactivate user", fullCmdName + " deactivate username"},
	}
	validCommands := map[string]struct{}{
		"list":            {},
		"create":          {},
		"change-password": {},
		"set-permission":  {},
		"deactivate":      {},
	}
	requiredArgs := c.RequiredArgs{
		Cmd:    fullCmdName,
		Usage:  parentCmdName + " " + usage,
		Usages: usages,
	}
	ccmd := &cobra.Command{
		Use:               usage,
		Short:             "Perform various user-related operations: list, create, deactivate, change password, set permissions",
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
			case "create", "change-password", "set-permission", "deactivate":
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
						map[string]struct{}{"read": {}, "readwrite": {}},
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
					cl.changePassword(username, nil)
				case "deactivate":
					cl.deactivateUser(username)
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
	if len(usersList.Users) <= 0 {
		fmt.Printf("No users found")
	}
	fmt.Println(len(usersList.Users), "user(s):")
	c.PrintTable(
		[]string{"Username", "Role", "Permissions"},
		len(usersList.Users),
		func(i int) []string {
			row := make([]string, 3)
			permission := usersList.Users[i].GetPermissions()[0]
			row[0] = string(usersList.Users[i].GetUser())
			if permission == auth.PermissionAdmin {
				row[1] = "admin"
				row[2] = "admin"
			} else {
				row[1] = "client"
				switch permission {
				case auth.PermissionR:
					row[2] = "read"
				case auth.PermissionRW:
					row[2] = "readwrite"
				case auth.PermissionNone:
					row[2] = "deactivated"
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
		permission = []byte{auth.PermissionRW}
	default:
		permission = []byte{auth.PermissionR}
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
		permission = []byte{auth.PermissionRW}
	default:
		permission = []byte{auth.PermissionR}
	}

	user, err := cl.immuClient.GetUser(cl.context, []byte(username))
	if err != nil {
		c.QuitWithUserError(err)
	}
	if user.Permissions[0] == auth.PermissionNone {
		fmt.Printf(
			"User %s is deactivated. Are you sure you want to activate it back? [y/N]: ",
			username)
		answer, err := c.ReadFromTerminalYN("N")
		if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
			c.QuitToStdErr("Canceled")
		}
	}

	if err := cl.immuClient.SetPermission(
		cl.context, []byte(username), permission); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("Permissions updated for user %s\n", username)
}

func (cl *commandline) changePassword(username string, oldPass []byte) {
	fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
	var err error
	if username == auth.AdminUsername && len(oldPass) <= 0 {
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

func (cl *commandline) deactivateUser(username string) {
	fmt.Printf("Are you sure you want to deactivate user %s? [y/N]: ", username)
	answer, err := c.ReadFromTerminalYN("N")
	if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
		c.QuitToStdErr("Canceled")
	}
	if err := cl.immuClient.DeactivateUser(cl.context, []byte(username)); err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("User %s has been deactivated\n", username)
}
