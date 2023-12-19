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

package main

import (
	"os"

	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/spf13/viper"

	c "github.com/codenotary/immudb/cmd/helper"
	immutest "github.com/codenotary/immudb/cmd/immutest/command"
	"github.com/codenotary/immudb/cmd/version"
)

func main() {
	err := execute(
		c.DefaultPasswordReader,
		c.NewTerminalReader(os.Stdin),
		tokenservice.NewFileTokenService().WithHds(homedir.NewHomedirService()).WithTokenFileName(viper.GetString("tokenfile")),
		c.QuitWithUserError,
		nil)
	if err != nil {
		c.QuitWithUserError(err)
	}
	os.Exit(0)
}

func execute(
	pwr c.PasswordReader,
	tr c.TerminalReader,
	ts tokenservice.TokenService,
	onError func(err error),
	args []string,
) error {
	version.App = "immutest"
	cmd := immutest.NewCmd(pwr, tr, ts, onError)
	if args != nil {
		cmd.SetArgs(args)
	}
	return cmd.Execute()
}
