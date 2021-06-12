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

package immuadmin

import (
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (cl *commandline) database(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "database",
		Short:   "Issue all database commands",
		Aliases: []string{"d"},
		//PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"list", "create", "use", "clean"},
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
					if cl.options.CurrentDatabase == resp.Databases[i].DatabaseName {
						row[0] += fmt.Sprintf("*")
					}
					row[0] += fmt.Sprintf("%s", resp.Databases[i].DatabaseName)
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
			isReplica, err := cmd.Flags().GetBool("replica")
			if err != nil {
				return err
			}

			if err := cl.immuClient.CreateDatabase(cl.context, &schema.DatabaseSettings{
				DatabaseName: args[0],
				Replica:      isReplica,
			}); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' (replica = %v) successfully created\n", args[0], isReplica)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cc.Flags().BoolP("replica", "r", false, "set database as a replica")

	cu := &cobra.Command{
		Use:               "update",
		Short:             "Update database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "update {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			isReplica, err := cmd.Flags().GetBool("replica")
			if err != nil {
				return err
			}

			if err := cl.immuClient.UpdateDatabase(cl.context, &schema.DatabaseSettings{
				DatabaseName: args[0],
				Replica:      isReplica,
			}); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' (replica = %v) successfully updated\n", args[0], isReplica)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cu.Flags().BoolP("replica", "r", false, "set database as a replica")

	ccu := &cobra.Command{
		Use:               "use command",
		Short:             "Select database",
		Example:           "use {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.UseDatabase(cl.context, &schema.Database{
				DatabaseName: args[0],
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

	ccc := &cobra.Command{
		Use:               "clean command",
		Short:             "Clean database index",
		Example:           "clean",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immuClient.CleanIndex(cl.context, &emptypb.Empty{})
			if err != nil {
				cl.quit(err)
			}
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Database index successfully cleaned\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	ccmd.AddCommand(ccc)
	ccmd.AddCommand(ccu)
	ccmd.AddCommand(ccd)
	ccmd.AddCommand(cc)
	ccmd.AddCommand(cu)
	cmd.AddCommand(ccmd)
}
