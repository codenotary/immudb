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
	"github.com/spf13/cobra"
)

func (cl *commandline) sqlExec(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "exec",
		Short:             "Executes sql statement",
		Aliases:           []string{"x"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SQLExec(args)
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

func (cl *commandline) sqlQuery(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "query",
		Short:             "Query sql statement",
		Aliases:           []string{"q"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SQLQuery(args)
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

func (cl *commandline) listTables(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "tables",
		Short:             "List tables",
		Aliases:           []string{"tables"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.ListTables()
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) describeTable(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe table",
		Aliases:           []string{"table"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.DescribeTable(args)
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
