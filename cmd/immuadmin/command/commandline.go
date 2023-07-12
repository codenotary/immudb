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
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/codenotary/immudb/pkg/auth"

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
	nonInteractive bool
	context        context.Context
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
		// Determine if the command is run interactive.
		cl.nonInteractive, err = cmd.Flags().GetBool("non-interactive")
		if err != nil {
			return err
		}
		// here all command line options and services need to be configured by options retrieved from viper
		cl.options = Options()
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
	// Check if client connection exists.
	if cl.immuClient == nil || !cl.immuClient.IsConnected() {
		return
	}
	if err := cl.immuClient.CloseSession(cl.context); err != nil {
		cl.quit(err)
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	// Check if a username was specified.
	username, err := cmd.Flags().GetString("username")
	if err != nil {
		return err
	}
	if username == "" {
		err = fmt.Errorf("please specify a username using the --username flag")
		return err
	}
	// Get the corresponding password.
	pass, err := cl.getPassword(cmd, "password-file", "Password:")
	if err != nil {
		return err
	}
	// Get the selected database for the session.
	database, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}
	// Open a session with the server.
	cl.immuClient = client.NewClient().WithOptions(cl.options)
	if err := cl.immuClient.OpenSession(cl.context, []byte(username), pass, database); err != nil {
		return err
	}
	return

}

func (cl *commandline) checkLoggedIn(cmd *cobra.Command, args []string) (err error) {
	possiblyLoggedIn := cl.immuClient.IsConnected()
	if !possiblyLoggedIn {
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

// getPasswordFromFlag reads a password from the file specified by the given
// commandline flag.
func (cl *commandline) getPasswordFromFlag(cmd *cobra.Command, flag string) (pass []byte, err error) {
	// Check if a file containing the password has been specified.
	var passwordFile string
	passwordFile, err = cmd.Flags().GetString(flag)
	if err != nil {
		return
	}
	// If no file is specified, return nothing.
	if passwordFile == "" {
		return
	}
	// Read the content of the specified file.
	var info fs.FileInfo
	info, err = os.Stat(passwordFile)
	if err != nil {
		return
	}
	if info.Size() > 34 {
		err = fmt.Errorf("password file %s is larger than 34 bytes, which exceeds the the maximal password length plus a trailing new line", passwordFile)
		return
	}
	pass, err = os.ReadFile(passwordFile)
	if err != nil {
		err = fmt.Errorf("reading password file %s failed: %v", passwordFile, err)
		return
	}
	// Remove trailing new lines
	pass = []byte(strings.TrimRight(string(pass), "\r\n"))
	return
}

// getPassword either retrieves a password from the commandline flag, if it is
// set, or otherwise prompts the user to enter a password. The user is only
// prompted for a password if the command was run interactively.
func (cl *commandline) getPassword(cmd *cobra.Command, flag string, prompt string) (pass []byte, err error) {
	// Try to get the password from a command line flag.
	pass, err = cl.getPasswordFromFlag(cmd, flag)
	if err != nil {
		return
	}
	// If a password could be retrieved from the command line flag, it is used.
	if pass != nil {
		return
	}
	// Prompt the user to provide the password if the command may be interactive.
	if cl.nonInteractive {
		err = fmt.Errorf("please specify a password using the --%s flag", flag)
		return
	}
	pass, err = cl.passwordReader.Read(prompt)
	return
}

// getNewPassword retrieves a password using a set command line flag or by
// prompting the user to enter one. If the password was retrieved interactively,
// the user has to confirm the chosen password by entering it twice,
// to prevent typos. In both cases the password is checked whether it meets
// the minimum requirements.
func (cl *commandline) getNewPassword(cmd *cobra.Command, flag string, prompt string) (pass []byte, err error) {
	// Try to get the password from a command line flag.
	pass, err = cl.getPasswordFromFlag(cmd, flag)
	if err != nil {
		return
	}
	// If no password was found for the flag, prompt the user to enter one.
	passRead := false
	if pass == nil {
		// Query the user for the password if the command is run interactive.
		if cl.nonInteractive {
			err = fmt.Errorf("please specify a password using the %s flag", flag)
			return
		}
		pass, err = cl.passwordReader.Read(prompt)
		if err != nil {
			return
		}
		// Remember that the password was entered interactively.
		passRead = true
	}
	// Verify that the password meets requirements.
	if err = auth.IsStrongPassword(string(pass)); err != nil {
		err = fmt.Errorf("Password does not meet the requirements. It must contain upper and lower case letters, digits, punctuation mark or symbol")
		return
	}
	// Prompt the user for the password again to confirm it was entered correctly.
	if passRead {
		var pass2 []byte
		pass2, err = cl.passwordReader.Read("Confirm password:")
		if err != nil {
			return
		}
		if !bytes.Equal(pass, pass2) {
			err = fmt.Errorf("Passwords don't match")
			return
		}
	}
	return
}
