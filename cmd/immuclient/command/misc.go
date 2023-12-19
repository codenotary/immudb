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

package immuclient

import (
	"github.com/codenotary/immudb/cmd/immuclient/audit"
	"github.com/codenotary/immudb/cmd/immuclient/cli"
	service "github.com/codenotary/immudb/cmd/immuclient/service/constants"
	"github.com/spf13/cobra"
)

func (cl *commandline) history(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "history key",
		Short:             "Fetch history for the item having the specified key",
		Aliases:           []string{"h"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.History(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) status(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "status",
		Short:             "Ping to check if server connection is alive",
		Aliases:           []string{"p"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.CurrentState(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) auditmode(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:       "audit-mode command",
		Short:     "Starts immuclient as daemon in auditor mode. Run 'immuclient help audit-mode' or use -h flag for details",
		Aliases:   []string{"audit-mode"},
		Example:   service.UsageExamples,
		ValidArgs: []string{"start", "install", "uninstall", "restart", "stop", "status"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := audit.Init(args, cmd.Parent()); err != nil {
				cl.quit(err)
			}
			return nil
		},
	}
	cmd.AddCommand(ccmd)
}

// #TODO will be new root.
func (cl *commandline) interactiveCli(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "it",
		Short:   "Starts immuclient in CLI mode. Use 'help' in the shell or the -h flag for details",
		Aliases: []string{"cli-mode"},
		Example: cli.Init(cl.immucl).HelpMessage(),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cl.immucl.Connect(args); err != nil {
				cl.quit(err)
			}
			cli.Init(cl.immucl).Run()
			return nil
		},
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) use(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "use",
		Short:             "Select database",
		Example:           "use {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.UseDatabase(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}
