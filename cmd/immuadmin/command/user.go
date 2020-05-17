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
	usage := cmdName + " list|create|change-password|delete [username]"
	usages := map[string][]string{
		"list":            []string{"List users", fullCmdName + " list"},
		"create":          []string{"Create a new user", fullCmdName + " create username"},
		"change-password": []string{"Change password", fullCmdName + " change-password username"},
		"delete":          []string{"Delete user", fullCmdName + " delete username"},
	}
	requiredArgs := c.RequiredArgs{
		Cmd:    fullCmdName,
		Usage:  parentCmdName + " " + usage,
		Usages: usages,
	}
	ccmd := &cobra.Command{
		Use:               usage,
		Short:             "Perform various user-related operations: list, create, delete, change password",
		Example:           c.UsageSprintf(usages),
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			action, err := requiredArgs.Require(args, 0, "a user action", "")
			if err != nil {
				c.QuitToStdErr(err)
			}
			switch action {
			case "list":
				cl.listUsers()
			case "create", "change-password", "delete":
				username, err := requiredArgs.Require(args, 1, "a username", action)
				if err != nil {
					c.QuitToStdErr(err)
				}
				switch action {
				case "create":
					cl.createUser(username)
				case "change-password":
					cl.changePassword(username)
				case "delete":
					cl.deleteUser(username)
				}
			default:
				_, err := requiredArgs.Require(args, 0, "a valid user action", action)
				if err != nil {
					c.QuitToStdErr(err)
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
	fmt.Println(len(usersList.Items), "users:")
	var sb strings.Builder
	c.PrintTable(
		[]string{"Username", "Role", "Permissions"},
		len(usersList.Items),
		func(i int, colSep string) string {
			u := string(usersList.Items[i].GetKey())
			sb.WriteString(u)
			sb.WriteString(colSep)
			if u != auth.AdminUsername {
				sb.WriteString("client")
				sb.WriteString(colSep)
				sb.WriteString("read/write")
			} else {
				sb.WriteString("admin")
				sb.WriteString(colSep)
				sb.WriteString("admin")
			}
			sb.WriteString(colSep)
			s := sb.String()
			sb.Reset()
			return s
		},
	)
}

func (cl *commandline) createUser(username string) {
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
	_, err = cl.immuClient.CreateUser(cl.context, []byte(username), pass)
	if err != nil {
		c.QuitWithUserError(err)
	}
	fmt.Printf("User %s created\n", username)
}

func (cl *commandline) changePassword(username string) {
	fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
	oldPass, err := cl.passwordReader.Read("Old password:")
	if err != nil {
		c.QuitToStdErr(err)
	}
	_, err = cl.immuClient.Login(cl.context, []byte(username), oldPass)
	if err != nil {
		c.QuitToStdErr(err)
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
	if bytes.Equal(pass, oldPass) {
		c.QuitToStdErr(errors.New("New password is identical to the old one"))
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
