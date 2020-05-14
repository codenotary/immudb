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

func (cl *commandline) user(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "user create|change-password|delete username [password]",
		Short: "Perform various user-related operations: create, delete, change password",
		Example: `  Create a new user:
    ./immuadmin user create username
  Change password:
    ./immuadmin user change-password username
  Delete user:
    ./immuadmin user delete username`,
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			action := args[0]
			username := []byte(args[1])
			ctx := cl.context
			switch action {
			case "create":
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
				_, err = cl.immuClient.CreateUser(ctx, username, pass)
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("User %s created\n", username)
				return nil
			case "change-password":
				fmt.Println("NOTE:", auth.PasswordRequirementsMsg+".")
				oldPass, err := cl.passwordReader.Read("Old password:")
				if err != nil {
					c.QuitToStdErr(err)
				}
				if len(oldPass) <= 0 {
					c.QuitToStdErr(fmt.Errorf("Old password can not be empty"))
				}
				_, err = cl.immuClient.Login(ctx, username, oldPass)
				if err != nil {
					c.QuitToStdErr(fmt.Errorf("Wrong password"))
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
				if err = cl.immuClient.ChangePassword(ctx, username, oldPass, pass); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("Password changed for user %s\n", string(username))
				return nil
			case "delete":
				var answer string
				fmt.Printf("Are you sure you want to delete user %s? [y/N]: ", username)
				if _, err := fmt.Scanln(&answer); err != nil ||
					!(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
					c.QuitToStdErr("Canceled")
				}
				if err := cl.immuClient.DeleteUser(ctx, username); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("User %s has been deleted\n", string(username))
				return nil
			}
			c.QuitToStdErr(errors.New("Please specify one of the following actions: create, change-password or delete"))
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
