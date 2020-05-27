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
	"fmt"
	"os"
	"strings"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/cli"
	immuclient "github.com/codenotary/immudb/cmd/immuclient/command"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/spf13/cobra"
)

func main() {
	version.App = "immuclient"
	cmd := immuclient.NewCmd()
	if isCommand(commandNames(cmd.Commands())) {
		if err := cmd.Execute(); err != nil {
			fmt.Println(cmd.Aliases)
			if strings.HasPrefix(err.Error(), "unknown command") {
				os.Exit(0)
			}
			c.QuitWithUserError(err)
		}
		return
	}

	cliClient, err := cli.Init()
	if err != nil {
		c.QuitWithUserError(err)
	}
	cliClient.Run()
}

func isCommand(args []string) bool {
	hs, hl := "-h", "--help"
	for i := range os.Args {
		if os.Args[i] == hs || os.Args[i] == hl {
			return true
		}
	}
	if len(os.Args) < 2 {
		return false
	}
	if !strings.HasPrefix(os.Args[1], "-") {
		return true
	}
	for i := range args {
		for j := range os.Args {
			if args[i] == os.Args[j] {
				fmt.Printf("Please sort your commands in \"immudb [command] [flags]\" order. \n")
				return true
			}
		}
	}
	return false
}

func commandNames(cms []*cobra.Command) []string {
	args := make([]string, 0)
	for i := range cms {
		arg := strings.Split(cms[i].Use, " ")[0]
		args = append(args, arg)
	}
	return args
}
