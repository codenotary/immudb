/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"context"
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cl *commandline) login(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "login username (you will be prompted for password)",
		Short:             fmt.Sprintf("Login using the specified username and password (admin username is %s)", auth.SysAdminUsername),
		Aliases:           []string{"l"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cl.context
			flagLogin := false
			var userStr string
			if len(args) == 0 && !viper.IsSet("username") {
				return fmt.Errorf("username is required")
			}
			if len(args) > 0 {
				userStr = args[0]
			} else {
				userStr = viper.GetString("username")
			}
			if userStr != auth.SysAdminUsername {
				err := fmt.Errorf("Permission denied: user %s has no admin rights", userStr)
				cl.quit(err)
				return err
			}
			user := []byte(userStr)

			var pass []byte
			if viper.IsSet("password") {
				flagLogin = true
				pass = []byte(viper.GetString("password"))
			} else {
				var err error
				if pass, err = cl.passwordReader.Read("Password:"); err != nil {
					cl.quit(err)
					return err
				}
			}

			responseWarning, err := cl.loginClient(ctx, user, pass)
			if err != nil {
				cl.quit(err)
				return err
			}

			c.PrintfColorW(cmd.OutOrStdout(), c.Green, "logged in\n")
			if flagLogin {
				return nil
			}

			if string(responseWarning) == auth.WarnDefaultAdminPassword {
				c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "SECURITY WARNING: %s\n", responseWarning)

				changedPassMsg, newPass, err := cl.changeUserPassword(userStr, pass)
				if err != nil {
					cl.quit(err)
					return err
				}

				if _, err := cl.loginClient(ctx, user, newPass); err != nil {
					cl.quit(err)
					return err
				}

				fmt.Fprint(cmd.OutOrStdout(), changedPassMsg)
			}

			return nil
		},
		Args: cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) loginClient(
	ctx context.Context,
	user []byte,
	pass []byte,
) (string, error) {
	response, err := cl.immuClient.Login(ctx, user, pass)
	if err != nil {
		return "", err
	}
	return string(response.GetWarning()), nil
}

func (cl *commandline) logout(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "logout",
		Aliases:           []string{"x"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cl.immuClient.Logout(cl.context); err != nil {
				cl.quit(err)
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "logged out\n")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
