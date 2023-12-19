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

package immudb

import (
	"fmt"
	"strconv"
	"time"

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immudb/command/service"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
)

func Execute() {
	version.App = "immudb"

	// set the version fields so that they are available to the monitoring HTTP server
	server.Version = server.VersionResponse{
		Component: "immudb",
		Version:   fmt.Sprintf("%s-%s", version.Version, version.Commit),
		BuildTime: version.BuiltAt,
		BuiltBy:   version.BuiltBy,
		Static:    version.Static == "static",
		FIPS:      version.FIPSBuild(),
	}
	if version.BuiltAt != "" {
		i, err := strconv.ParseInt(version.BuiltAt, 10, 64)
		if err == nil {
			server.Version.BuildTime = time.Unix(i, 0).Format(time.RFC1123)
		}
	}

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
