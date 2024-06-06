/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

			responseWarning, err := cl.loginClient(ctx, user, pass)
			if err != nil {
				cl.quit(err)
				return err
			}

			c.PrintfColorW(cmd.OutOrStdout(), c.Green, "logged in\n")

			if string(responseWarning) == auth.WarnDefaultAdminPassword {
				c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "SECURITY WARNING: %s\n", responseWarning)

				changedPassMsg, newPass, err := cl.changeUserPassword(cmd, userStr, pass)
				if err != nil {
					cl.quit(err)
					return err
				}

				if _, err := cl.loginClient(ctx, user, newPass); err != nil {
					cl.quit(err)
					return err
				}

				fmt.Fprintln(cmd.OutOrStdout(), changedPassMsg)
			}

			return nil
		},
		Args: cobra.ExactArgs(1),
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
