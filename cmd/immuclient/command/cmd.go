/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package immuclient

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/spf13/cobra"
)

func Execute(cmd *cobra.Command) error {
	if isCommand(commandNames(cmd.Commands())) {
		if err := cmd.Execute(); err != nil {
			return err
		}
	}
	return nil
}

func NewCommand() *cobra.Command {
	version.App = "immuclient"

	// set the version fields so that they are available to the auditor monitoring HTTP server
	auditor.Version = auditor.VersionResponse{
		Component: "immuclient-auditor",
		Version:   fmt.Sprintf("%s-%s", version.Version, version.Commit),
		BuildTime: version.BuiltAt,
		BuiltBy:   version.BuiltBy,
		Static:    version.Static == "static",
		FIPS:      version.FIPSBuild(),
	}
	if version.BuiltAt != "" {
		i, err := strconv.ParseInt(version.BuiltAt, 10, 64)
		if err == nil {
			auditor.Version.BuildTime = time.Unix(i, 0).Format(time.RFC1123)
		}
	}

	cl := NewCommandLine()
	cmd, err := cl.NewCmd()
	if err != nil {
		c.QuitToStdErr(err)
	}

	// login and logout
	cl.Register(cmd)
	// man file generator
	cmd.AddCommand(man.Generate(cmd, "immuclient", "./cmd/docs/man/immuclient"))
	cmd.AddCommand(version.VersionCmd())
	return cmd
}

func isCommand(args []string) bool {
	if len(os.Args) > 1 {
		if strings.HasPrefix(os.Args[1], "-") {
			for i := range args {
				for j := range os.Args {
					if args[i] == os.Args[j] {
						fmt.Printf("Please sort your commands in \"immudb [command] [flags]\" order. \n")
						return true
					}
				}
			}
		}
	}
	return true
}

func commandNames(cms []*cobra.Command) []string {
	args := make([]string, 0)
	for i := range cms {
		arg := strings.Split(cms[i].Use, " ")[0]
		args = append(args, arg)
	}
	return args
}

// fprintln is equivalent to fmt.Fprintln but appends newline only if one doesn't exist.
func fprintln(w io.Writer, msg string) {
	if strings.HasSuffix(msg, "\n") {
		_, _ = fmt.Fprint(w, msg)
	} else {
		_, _ = fmt.Fprintln(w, msg)
	}
}
