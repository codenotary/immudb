/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/spf13/cobra"
)

// Commandline ...
type Commandline interface {
	user(cmd *cobra.Command)
	login(cmd *cobra.Command)
	logout(cmd *cobra.Command)
	status(cmd *cobra.Command)
	stats(cmd *cobra.Command)
	serverConfig(cmd *cobra.Command)
	database(cmd *cobra.Command)
	ConfigChain(post func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error)
}

// CommandlineCli ...
type CommandlineCli interface {
	disconnect(cmd *cobra.Command, args []string)
	connect(cmd *cobra.Command, args []string) (err error)
	checkLoggedIn(cmd *cobra.Command, args []string) (err error)
	checkLoggedInAndConnect(cmd *cobra.Command, args []string) (err error)
}

type commandline struct {
	options        *client.Options
	config         c.Config
	immuClient     client.ImmuClient
	passwordReader c.PasswordReader
	context        context.Context
	ts             tokenservice.TokenService
	onError        func(msg interface{})
	os             immuos.OS
}

func NewCommandLine() *commandline {
	cl := &commandline{}
	cl.config.Name = "immuadmin"
	cl.passwordReader = c.DefaultPasswordReader
	cl.context = context.Background()
	//
	return cl
}

func (cl *commandline) ConfigChain(post func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error) {
	return func(cmd *cobra.Command, args []string) (err error) {
		if err = cl.config.LoadConfig(cmd); err != nil {
			return err
		}
		// here all command line options and services need to be configured by options retrieved from viper
		cl.options = Options()
		cl.ts = tokenservice.NewFileTokenService().WithHds(homedir.NewHomedirService()).WithTokenFileName(cl.options.TokenFileName)
		if post != nil {
			return post(cmd, args)
		}
		return nil
	}
}

func (cl *commandline) Register(rootCmd *cobra.Command) *cobra.Command {
	cl.user(rootCmd)
	cl.login(rootCmd)
	cl.logout(rootCmd)
	cl.status(rootCmd)
	cl.stats(rootCmd)
	cl.serverConfig(rootCmd)
	cl.database(rootCmd)
	return rootCmd
}

func (cl *commandline) quit(msg interface{}) {
	if cl.onError == nil {
		c.QuitToStdErr(msg)
	}
	cl.onError(msg)
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immuClient.Disconnect(); err != nil {
		cl.quit(err)
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	if cl.immuClient, err = client.NewImmuClient(cl.options); err != nil {
		cl.quit(err)
	}
	cl.immuClient.WithTokenService(tokenservice.NewFileTokenService().WithTokenFileName("token_admin"))
	return

}

func (cl *commandline) checkLoggedIn(cmd *cobra.Command, args []string) (err error) {
	possiblyLoggedIn, err2 := cl.ts.IsTokenPresent()
	if err2 != nil {
		fmt.Println("error checking if token file exists:", err2)
	} else if !possiblyLoggedIn {
		err = fmt.Errorf("please login first. If elevated privileges are required to execute requested action remember to execute login as super user. Eg. sudo login immudb")
		cl.quit(err)
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
