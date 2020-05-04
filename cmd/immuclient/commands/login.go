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
	"github.com/spf13/cobra"
)

func (cl *CommandlineClient) login(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "login username (you will be prompted for password)",
		Short:   "Login using the specified username and password",
		Aliases: []string{"l"},
		RunE: func(cmd *cobra.Command, args []string) error {
			user := []byte(args[0])
			pass, err := cl.passwordReader.Read("Password:")
			if err != nil {
				c.QuitWithUserError(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.Login(ctx, user, pass)
			if err != nil {
				c.QuitWithUserError(err)
			}
			tokenFileName := cl.ImmuClient.GetOptions().TokenFileName
			if err := ioutil.WriteFile(tokenFileName, response.Token, 0644); err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Printf("logged in\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *CommandlineClient) logout(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "logout",
		Aliases: []string{"x"},
		RunE: func(cmd *cobra.Command, args []string) error {
			os.Remove(cl.ImmuClient.GetOptions().TokenFileName)
			fmt.Println("logged out")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}
