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

package immudb

import (
	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immudb/command/service"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
)

func Execute() {
	version.App = "immudb"
	cmd, err := newCommand(server.DefaultServer())
	if err != nil {
		c.QuitWithUserError(err)
	}
	if err := cmd.Execute(); err != nil {
		c.QuitWithUserError(err)
	}
}

// NewCmd ...
func newCommand(immudbServer server.ImmuServerIf) (*cobra.Command, error) {
	cl := Commandline{P: c.NewPlauncher(), config: c.Config{Name: "immudb"}}
	cmd, err := cl.NewRootCmd(immudbServer)
	if err != nil {
		c.QuitToStdErr(err)
	}

	cmd.AddCommand(man.Generate(cmd, "immudb", "./cmd/docs/man/immudb"))
	cmd.AddCommand(version.VersionCmd())

	scl := service.NewCommandLine()
	scl.Register(cmd)

	return cmd, nil
}
