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
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (cl *commandline) database(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "database",
		Short:   "Issue all database commands",
		Aliases: []string{"d"},
		//PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"list", "create", "update", "use", "clean"},
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
			settings, err := prepareDatabaseSettings(args[0], cmd.Flags())
			if err != nil {
				return err
			}

			if err := cl.immuClient.CreateDatabase(cl.context, settings); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"database '%s' {replica: %v, exclude-commit-time: %v} successfully created\n", args[0], settings.Replica, settings.ExcludeCommitTime)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cc.Flags().Bool("exclude-commit-time", false,
		"do not include server-side timestamps in commit checksums, useful when reproducibility is a desired feature")
	cc.Flags().Bool("replication-enabled", false, "set database as a replica")
	cc.Flags().String("replication-master-database", "", "set master database to be replicated")
	cc.Flags().String("replication-master-address", "127.0.0.1", "set master address")
	cc.Flags().Uint32("replication-master-port", 3322, "set master port")
	cc.Flags().String("replication-follower-username", "", "set username used for replication")
	cc.Flags().String("replication-follower-password", "", "set password used for replication")

	cu := &cobra.Command{
		Use:               "update",
		Short:             "Update database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "update {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings, err := prepareDatabaseSettings(args[0], cmd.Flags())
			if err != nil {
				return err
			}

			if err := cl.immuClient.UpdateDatabase(cl.context, settings); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"database '%s' {replica: %v, exclude-commit-time: %v} successfully updated\n", args[0], settings.Replica, settings.ExcludeCommitTime)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cu.Flags().Bool("exclude-commit-time", false,
		"do not include server-side timestamps in commit checksums, useful when reproducibility is a desired feature")
	cu.Flags().Bool("replication-enabled", false, "set database as a replica")
	cu.Flags().String("replication-master-database", "", "set master database to be replicated")
	cu.Flags().String("replication-master-address", "127.0.0.1", "set master address")
	cu.Flags().Uint32("replication-master-port", 3322, "set master port")
	cu.Flags().String("replication-follower-username", "", "set username used for replication")
	cu.Flags().String("replication-follower-password", "", "set password used for replication")

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

	fcc := &cobra.Command{
		Use:               "flush command",
		Short:             "Flush database index",
		Example:           "flush",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			cleanupPercentage, err := cmd.Flags().GetInt("cleanup-percentage")
			if err != nil {
				return err
			}

			err = cl.immuClient.FlushIndex(cl.context, cleanupPercentage)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Database index successfully flushed\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	fcc.Flags().Int("cleanup-percentage", 0, "set cleanup percentage")

	ccc := &cobra.Command{
		Use:               "compact command",
		Short:             "Compact database index",
		Example:           "compact",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immuClient.CompactIndex(cl.context, &emptypb.Empty{})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Database index successfully compacted\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	ccmd.AddCommand(fcc)
	ccmd.AddCommand(ccc)
	ccmd.AddCommand(ccu)
	ccmd.AddCommand(ccd)
	ccmd.AddCommand(cc)
	ccmd.AddCommand(cu)
	cmd.AddCommand(ccmd)
}

func prepareDatabaseSettings(db string, flags *pflag.FlagSet) (*schema.DatabaseSettings, error) {
	excludeCommitTime, err := flags.GetBool("exclude-commit-time")
	if err != nil {
		return nil, err
	}

	replicationEnabled, err := flags.GetBool("replication-enabled")
	if err != nil {
		return nil, err
	}

	if !replicationEnabled {
		return &schema.DatabaseSettings{
			DatabaseName:      db,
			ExcludeCommitTime: excludeCommitTime,
		}, nil
	}

	masterDatabase, err := flags.GetString("replication-master-database")
	if err != nil {
		return nil, err
	}

	masterAddress, err := flags.GetString("replication-master-address")
	if err != nil {
		return nil, err
	}

	masterPort, err := flags.GetUint32("replication-master-port")
	if err != nil {
		return nil, err
	}

	followerUsername, err := flags.GetString("replication-follower-username")
	if err != nil {
		return nil, err
	}

	followerPassword, err := flags.GetString("replication-follower-password")
	if err != nil {
		return nil, err
	}

	return &schema.DatabaseSettings{
		DatabaseName:      db,
		ExcludeCommitTime: excludeCommitTime,
		Replica:           replicationEnabled,
		MasterDatabase:    masterDatabase,
		MasterAddress:     masterAddress,
		MasterPort:        masterPort,
		FollowerUsername:  followerUsername,
		FollowerPassword:  followerPassword,
	}, nil
}
