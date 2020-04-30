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
	"context"
	"fmt"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

func (cl *commandline) login(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "login username \"password\"",
		Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
		Aliases: []string{"l"},
		RunE: func(cmd *cobra.Command, args []string) error {
			user := []byte(args[0])
			pass := []byte(args[1])
			ctx := context.Background()
			response, err := cl.GetImmuClient().Login(ctx, user, pass)
			if err != nil {
				c.QuitWithUserError(err)
			}
			if err := ioutil.WriteFile(client.TokenFileName, response.Token, 0644); err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Printf("logged in\n")
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) logout(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "logout",
		Aliases: []string{"x"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := os.Remove(client.TokenFileName); err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Println("logged out")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
