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
	"fmt"
	"time"

	"github.com/spf13/cobra"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuadmin/command/stats"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func (cl *commandline) status(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "status",
		Short:             "Show heartbeat status",
		Aliases:           []string{"p"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cl.context

			info, err := cl.immuClient.ServerInfo(ctx, &schema.ServerInfoRequest{})
			if err != nil {
				c.QuitWithUserError(err)
			}

			startedAt := time.Unix(info.StartedAt, 0)
			uptime := time.Now().Truncate(time.Second).Sub(startedAt)

			fmt.Fprintf(
				cmd.OutOrStdout(),
				"Status:\t\tOK - server is reachable and responding to queries\nVersion:\t%s\nUp time:\t%s (up %s)\nDatabases:\t%d (%s)\nTransactions:\t%d\n",
				info.Version,
				startedAt.Format(time.RFC822),
				uptime,
				info.NumDatabases,
				c.FormatByteSize(uint64(info.DatabasesDiskSize)),
				info.NumTransactions,
			)
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) stats(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "stats",
		Short:             "Show statistics as text or visually with the '-v' option. Run 'immuadmin stats -h' for details.",
		Aliases:           []string{"s"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, err := cmd.Flags().GetBool("raw")
			if err != nil {
				c.QuitToStdErr(err)
			}
			options := cl.immuClient.GetOptions()
			if raw {
				if err := stats.ShowMetricsRaw(cmd.OutOrStderr(), options.Address); err != nil {
					c.QuitToStdErr(err)
				}
				return nil
			}
			text, err := cmd.Flags().GetBool("text")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if text {
				if err := stats.ShowMetricsAsText(cmd.OutOrStderr(), options.Address); err != nil {
					c.QuitToStdErr(err)
				}
				return nil
			}
			if err := stats.ShowMetricsVisually(options.Address); err != nil {
				c.QuitToStdErr(err)
			}
			return nil
		},
		Args: cobra.NoArgs,
	}
	ccmd.Flags().BoolP("text", "t", false, "show statistics as text instead of the default graphical view")
	ccmd.Flags().BoolP("raw", "r", false, "show raw statistics")
	cmd.AddCommand(ccmd)
}
