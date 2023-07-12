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
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
)

func (cl *commandline) login(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "login username [database] (you will be prompted for password)",
		Short:             fmt.Sprintf("Login using the specified username and password (admin username is %s)", auth.SysAdminUsername),
		Aliases:           []string{"l"},
		PersistentPreRunE: cl.ConfigChain(nil),
		//PersistentPostRun: nil,
		RunE: func(cmd *cobra.Command, args []string) error {
			userStr := args[0]
			if userStr != auth.SysAdminUsername {
				err := fmt.Errorf("Permission denied: user %s has no admin rights", userStr)
				cl.quit(err)
				return err
			}

			user := []byte(userStr)
			pass, err := cl.passwordReader.Read("Password:")
			if err != nil {
				cl.quit(err)
				return err
			}

			// If no database name is given, the default database name is used.
			database := "defaultdb"
			if len(args) > 1 {
				database = args[1]
			}

			err = cl.openSession(user, pass, database)
			if err != nil {
				cl.quit(err)
				return err
			}
			c.PrintfColorW(cmd.OutOrStdout(), c.Green, "logged in\n")

			// The auth.WarnDefaultAdminPassword does not seem to be available
			// anymore using the new OpenSession api.
			//
			//  ctx := cl.context
			//  responseWarning, err := cl.loginClient(ctx, user, pass)
			//  if err != nil {
			//  	cl.quit(err)
			//  	return err
			//  }

			//  if string(responseWarning) == auth.WarnDefaultAdminPassword {
			//  	c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "SECURITY WARNING: %s\n", responseWarning)

			//  	newpass, err := cl.getNewPassword(cmd, "new-password-file", "Chooser a new password:")
			//  	if err != nil {
			//  		cl.quit(err)
			//  	}

			//  	changedPassMsg, newPass, err := cl.changeUserPassword(userStr, pass, newpass)
			//  	if err != nil {
			//  		cl.quit(err)
			//  		return err
			//  	}

			//  	if _, err := cl.loginClient(ctx, user, newPass); err != nil {
			//  		cl.quit(err)
			//  		return err
			//  	}

			//  	fmt.Fprint(cmd.OutOrStdout(), changedPassMsg)
			//  }

			return nil
		},
		Args: cobra.RangeArgs(1, 2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) logout(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "logout",
		Aliases:           []string{"x"},
		PersistentPreRunE: cl.ConfigChain(nil),
		//PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl.disconnect(cmd, args)
			fmt.Fprintf(cmd.OutOrStdout(), "logged out\n")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
