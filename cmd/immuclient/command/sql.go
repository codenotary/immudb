/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
package immuclient

import (
	"fmt"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func (cl *commandline) sqlExec(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "exec",
		Short:             "Executes sql statement",
		Aliases:           []string{"e"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SQLExec(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
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

			consoleTable := tablewriter.NewWriter(cmd.OutOrStdout())

			cols := make([]string, len(resp.Columns))
			for i, c := range resp.Columns {
				cols[i] = c.Name
			}
			consoleTable.SetHeader(cols)

			for _, r := range resp.Rows {
				row := make([]string, len(r.Values))

				for i, v := range r.Values {
					row[i] = v.String()
				}

				consoleTable.Append(row)
			}

			consoleTable.Render()

			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}
