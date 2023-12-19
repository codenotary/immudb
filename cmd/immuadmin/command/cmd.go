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
	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/spf13/cobra"
)

func Execute() {
	if err := newCommand().Execute(); err != nil {
		c.QuitWithUserError(err)
	}
}

func newCommand() *cobra.Command {
	version.App = "immuadmin"
	// register admin commands
	cml := NewCommandLine()
	cmd, err := cml.NewCmd()
	if err != nil {
		c.QuitToStdErr(err)
	}

	cmd = cml.Register(cmd)
	// register backup related commands
	os := immuos.NewStandardOS()
	clb, err := newCommandlineBck(os)
	if err != nil {
		c.QuitToStdErr(err)
	}
	cmd = clb.Register(cmd)

	// register hot backup related commands
	clhb, err := newCommandlineHotBck(os)
	if err != nil {
		c.QuitToStdErr(err)
	}
	cmd = clhb.Register(cmd)

	cmd.AddCommand(man.Generate(cmd, "immuadmin", "./cmd/docs/man/"+version.App))
	cmd.AddCommand(version.VersionCmd())

	return cmd
}
