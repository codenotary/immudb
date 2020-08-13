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

package main

import (
	"os"

	c "github.com/codenotary/immudb/cmd/helper"
	immutest "github.com/codenotary/immudb/cmd/immutest/command"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/client"
)

func main() {
	err := execute(
		func(opts *client.Options) (client.ImmuClient, error) { return client.NewImmuClient(opts) },
		c.DefaultPasswordReader,
		c.NewTerminalReader(os.Stdin),
		client.NewTokenService(),
		c.QuitWithUserError,
		nil)
	if err != nil {
		c.QuitWithUserError(err)
	}
	os.Exit(0)
}

func execute(
	newImmuClient func(*client.Options) (client.ImmuClient, error),
	pwr c.PasswordReader,
	tr c.TerminalReader,
	ts client.TokenService,
	onError func(err error),
	args []string,
) error {
	version.App = "immutest"
	cmd := immutest.NewCmd(newImmuClient, pwr, tr, ts, onError)
	if args != nil {
		cmd.SetArgs(args)
	}
	return cmd.Execute()
}
