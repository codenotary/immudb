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

package immuadmin

import (
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
)

func (cl *commandline) database(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "database",
		Short:   "Issue all database commands",
		Aliases: []string{"d"},
		//PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"list", "create", "use"},
	}
	ccd := &cobra.Command{
		Use:               "list",
		Short:             "List all databases",
		Aliases:           []string{"l"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.DatabaseList(cl.context)
			if err != nil {
				return err
			}
			c.PrintTable(
				cmd.OutOrStdout(),
				[]string{"Database Name"},
				len(resp.Databases),
				func(i int) []string {
					row := make([]string, 1)
					if cl.options.CurrentDatabase == resp.Databases[i].Databasename {
						row[0] += fmt.Sprintf("*")
					}
					row[0] += fmt.Sprintf("%s", resp.Databases[i].Databasename)
					return row
				},
				fmt.Sprintf("%d database(s)", len(resp.Databases)),
			)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	cc := &cobra.Command{
		Use:               "create",
		Short:             "Create a new database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "create {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cl.immuClient.CreateDatabase(cl.context, &schema.Database{
				Databasename: args[0],
			}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "database successfully created\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}

	ccu := &cobra.Command{
		Use:               "use command",
		Short:             "Select database",
		Example:           "use {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{
				Databasename: args[0],
			})
			if err != nil {
				cl.quit(err)
			}
			if err != nil {
				return err
			}
			cl.immuClient.GetOptions().CurrentDatabase = args[0]
			if err = cl.ts.SetToken(args[0], resp.Token); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Now using %s\n", args[0])
			return nil
		},
		Args: cobra.MaximumNArgs(2),
	}

	ccmd.AddCommand(ccu)
	ccmd.AddCommand(ccd)
	ccmd.AddCommand(cc)
	cmd.AddCommand(ccmd)
}
