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
	"context"
	"fmt"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

type Commandline interface {
	user(cmd *cobra.Command)
	login(cmd *cobra.Command)
	logout(cmd *cobra.Command)
	status(cmd *cobra.Command)
	stats(cmd *cobra.Command)
	serverConfig(cmd *cobra.Command)
	printTree(rootCmd *cobra.Command)
}

type CommandlineCli interface {
	disconnect(cmd *cobra.Command, args []string)
	connect(cmd *cobra.Command, args []string) (err error)
	checkLoggedIn(cmd *cobra.Command, args []string) (err error)
	checkLoggedInAndConnect(cmd *cobra.Command, args []string) (err error)
}

type commandline struct {
	options        *client.Options
	immuClient     client.ImmuClient
	passwordReader c.PasswordReader
	context        context.Context
	hds            client.HomedirService
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immuClient.Disconnect(); err != nil {
		c.QuitToStdErr(err)
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {

	if cl.immuClient, err = client.NewImmuClient(cl.options); err != nil {
		c.QuitToStdErr(err)
	}
	return
}

func (cl *commandline) checkLoggedIn(cmd *cobra.Command, args []string) (err error) {
	possiblyLoggedIn, err2 := cl.hds.FileExistsInUserHomeDir(cl.options.TokenFileName)
	if err2 != nil {
		fmt.Println("error checking if token file exists:", err2)
	} else if !possiblyLoggedIn {
		err = fmt.Errorf("please login first")
		c.QuitToStdErr(err)
	}
	return
}

func (cl *commandline) checkLoggedInAndConnect(cmd *cobra.Command, args []string) (err error) {
	if err = cl.checkLoggedIn(cmd, args); err != nil {
		return err
	}
	if err = cl.connect(cmd, args); err != nil {
		return err
	}
	return
}
