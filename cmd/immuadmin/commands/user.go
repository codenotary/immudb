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

package commands

import (
	"bytes"
	"errors"
	"fmt"

	c "github.com/codenotary/immudb/cmd"
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
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			action := args[0]
			username := []byte(args[1])
			ctx := cl.context
			switch action {
			case "create":
				pass, err := cl.passwordReader.Read("Password:")
				if err != nil {
					c.QuitWithUserError(err)
				}
				pass2, err := cl.passwordReader.Read("Confirm password:")
				if err != nil {
					c.QuitWithUserError(err)
				}
				if !bytes.Equal(pass, pass2) {
					c.QuitWithUserError(errors.New("Passwords don't match"))
				}
				_, err = cl.immuClient.CreateUser(ctx, username, pass)
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("User %s created\n", username)
				return nil
			case "change-password":
				oldPass, err := cl.passwordReader.Read("Old password:")
				if err != nil {
					c.QuitWithUserError(err)
				}
				pass, err := cl.passwordReader.Read("New password:")
				if err != nil {
					c.QuitWithUserError(err)
				}
				pass2, err := cl.passwordReader.Read("Confirm new password:")
				if err != nil {
					c.QuitWithUserError(err)
				}
				if !bytes.Equal(pass, pass2) {
					c.QuitWithUserError(errors.New("Passwords don't match"))
				}
				if bytes.Equal(pass, oldPass) {
					c.QuitWithUserError(errors.New("New password is identical to the old one"))
				}
				if err = cl.immuClient.ChangePassword(ctx, username, oldPass, pass); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("Password changed for user %s\n", string(username))
				return nil
			case "delete":
				if err := cl.immuClient.DeleteUser(ctx, username); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("User %s has been deleted\n", string(username))
				return nil
			}
			c.QuitWithUserError(errors.New("Please specify one of the following actions: create, change-password or delete"))
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
