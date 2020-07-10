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

package cli

import (
	"fmt"

	"github.com/codenotary/immudb/cmd/version"
)

func (cli *cli) history(args []string) (string, error) {
	return cli.immucl.History(args)
}

func (cli *cli) healthCheck(args []string) (string, error) {
	return cli.immucl.HealthCheck(args)
}

func (cli *cli) version(args []string) (string, error) {
	return version.VersionStr(), nil
}

func (cli *cli) UserOperations(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
	}
	switch args[0] {
	case "list":
		fmt.Printf("Admin\n")
		return cli.immucl.UserList(args[1:])
	case "changepassword":
		if len(args) < 2 {
			return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
		}
		return cli.immucl.ChangeUserPassword(args[1:])
	case "activate", "deactivate":
		if len(args) < 2 {
			return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
		}
		var active bool
		switch args[0] {
		case "activate":
			active = true
		case "deactivate":
			active = false
		}
		return cli.immucl.SetActiveUser(args[1:], active)
	case "permission":
		if len(args) < 5 {
			return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
		}
		return cli.immucl.SetUserPermission(args[1:])
	default:
		return "", fmt.Errorf("uknown command %s", args[0])
	}

}
