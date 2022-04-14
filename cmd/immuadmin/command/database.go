/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"strings"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/types/known/emptypb"
)

func addDbUpdateFlags(c *cobra.Command) {
	c.Flags().Bool("exclude-commit-time", false,
		"do not include server-side timestamps in commit checksums, useful when reproducibility is a desired feature")
	c.Flags().Bool("replication-enabled", false, "set database as a replica")
	c.Flags().String("replication-master-database", "", "set master database to be replicated")
	c.Flags().String("replication-master-address", "", "set master address")
	c.Flags().Uint32("replication-master-port", 0, "set master port")
	c.Flags().String("replication-follower-username", "", "set username used for replication")
	c.Flags().String("replication-follower-password", "", "set password used for replication")
	c.Flags().Uint32("write-tx-header-version", 1, "set write tx header version (use 0 for compatibility with immudb 1.1, 1 for immudb 1.2+)")
	c.Flags().Bool("autoload", true, "enable database autoloading")
}

func (cl *commandline) database(cmd *cobra.Command) {
	dbCmd := &cobra.Command{
		Use:               "database",
		Short:             "Issue all database commands",
		Aliases:           []string{"d"},
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"list", "create", "load", "unload", "delete", "update", "use", "flush", "compact"},
	}

	listCmd := &cobra.Command{
		Use:               "list",
		Short:             "List all databases",
		Aliases:           []string{"l"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immuClient.DatabaseListV2(cl.context)
			if err != nil {
				return err
			}
			c.PrintTable(
				cmd.OutOrStdout(),
				[]string{"Database Name", "Status"},
				len(resp.Databases),
				func(i int) []string {
					row := make([]string, 2)

					db := resp.Databases[i]

					if cl.options.CurrentDatabase == db.Name {
						row[0] += "*"
					}
					row[0] += db.Name

					if db.GetLoaded() {
						row[1] += "LOADED"
					} else {
						row[1] += "UNLOADED"
					}

					return row
				},
				fmt.Sprintf("%d database(s)", len(resp.Databases)),
			)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	createCmd := &cobra.Command{
		Use:               "create",
		Short:             "Create a new database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "create {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings, err := prepareDatabaseNullableSettings(cmd.Flags())
			if err != nil {
				return err
			}

			_, err = cl.immuClient.CreateDatabaseV2(cl.context, args[0], settings)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' {%s} successfully created\n",
				args[0], databaseNullableSettingsStr(settings))
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	addDbUpdateFlags(createCmd)

	loadCmd := &cobra.Command{
		Use:               "load",
		Short:             "Load database",
		Example:           "load {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := cl.immuClient.LoadDatabase(cl.context, &schema.LoadDatabaseRequest{
				Database: args[0],
			})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' successfully loaded\n", args[0])
			return nil
		},
		Args: cobra.ExactArgs(1),
	}

	unloadCmd := &cobra.Command{
		Use:               "unload",
		Short:             "Unload database",
		Example:           "unload {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := cl.immuClient.UnloadDatabase(cl.context, &schema.UnloadDatabaseRequest{
				Database: args[0],
			})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' successfully unloaded\n", args[0])
			return nil
		},
		Args: cobra.ExactArgs(1),
	}

	deleteCmd := &cobra.Command{
		Use:               "delete",
		Short:             "Delete database (unrecoverable operation)",
		Example:           "delete --yes-i-know-what-i-am-doing {database_name}",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			safetyFlag, err := cmd.Flags().GetBool("yes-i-know-what-i-am-doing")
			if err != nil {
				return err
			}

			if !safetyFlag {
				fmt.Fprintf(cmd.OutOrStdout(), "database '%s' was not deleted. Safety flag not set\n", args[0])
				return nil
			}

			_, err = cl.immuClient.DeleteDatabase(cl.context, &schema.DeleteDatabaseRequest{
				Database: args[0],
			})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database '%s' successfully deleted\n", args[0])
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	deleteCmd.Flags().Bool("yes-i-know-what-i-am-doing", false, "safety flag to confirm database deletion")
	deleteCmd.MarkFlagRequired("yes-i-know-what-i-am-doing")

	updateCmd := &cobra.Command{
		Use:               "update",
		Short:             "Update database",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		Example:           "update {database_name}",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings, err := prepareDatabaseNullableSettings(cmd.Flags())
			if err != nil {
				return err
			}

			if _, err := cl.immuClient.UpdateDatabaseV2(cl.context, args[0], settings); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"database '%s' {%s} successfully updated\n",
				args[0], databaseNullableSettingsStr(settings))
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	addDbUpdateFlags(updateCmd)

	useCmd := &cobra.Command{
		Use:               "use",
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

			fmt.Fprintf(cmd.OutOrStdout(), "now using database '%s'\n", args[0])
			return nil
		},
		Args: cobra.MaximumNArgs(2),
	}

	flushCmd := &cobra.Command{
		Use:               "flush",
		Short:             "Flush database index",
		Example:           "flush",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			cleanupPercentage, err := cmd.Flags().GetFloat32("cleanup-percentage")
			if err != nil {
				return err
			}

			synced, err := cmd.Flags().GetBool("synced")
			if err != nil {
				return err
			}

			_, err = cl.immuClient.FlushIndex(cl.context, cleanupPercentage, synced)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database index successfully flushed\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	flushCmd.Flags().Float32("cleanup-percentage", 0.00, "set cleanup percentage")
	flushCmd.Flags().Bool("synced", true, "synced mode enables physical data deletion")

	compactCmd := &cobra.Command{
		Use:               "compact",
		Short:             "Compact database index",
		Example:           "compact",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immuClient.CompactIndex(cl.context, &emptypb.Empty{})
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "database index successfully compacted\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}

	dbCmd.AddCommand(listCmd)
	dbCmd.AddCommand(createCmd)
	dbCmd.AddCommand(loadCmd)
	dbCmd.AddCommand(unloadCmd)
	dbCmd.AddCommand(deleteCmd)
	dbCmd.AddCommand(useCmd)
	dbCmd.AddCommand(updateCmd)
	dbCmd.AddCommand(flushCmd)
	dbCmd.AddCommand(compactCmd)

	cmd.AddCommand(dbCmd)
}

func prepareDatabaseNullableSettings(flags *pflag.FlagSet) (*schema.DatabaseNullableSettings, error) {
	var err error

	condBool := func(name string) (*schema.NullableBool, error) {
		if flags.Changed(name) {
			val, err := flags.GetBool(name)
			if err != nil {
				return nil, err
			}
			return &schema.NullableBool{Value: val}, nil
		}
		return nil, nil
	}

	condString := func(name string) (*schema.NullableString, error) {
		if flags.Changed(name) {
			val, err := flags.GetString(name)
			if err != nil {
				return nil, err
			}
			return &schema.NullableString{Value: val}, nil
		}
		return nil, nil
	}

	condUInt32 := func(name string) (*schema.NullableUint32, error) {
		if flags.Changed(name) {
			val, err := flags.GetUint32(name)
			if err != nil {
				return nil, err
			}
			return &schema.NullableUint32{Value: val}, nil
		}
		return nil, nil
	}

	ret := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{},
	}

	ret.ExcludeCommitTime, err = condBool("exclude-commit-time")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.Replica, err = condBool("replication-enabled")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.MasterDatabase, err = condString("replication-master-database")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.MasterAddress, err = condString("replication-master-address")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.MasterPort, err = condUInt32("replication-master-port")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.FollowerUsername, err = condString("replication-follower-username")
	if err != nil {
		return nil, err
	}

	ret.ReplicationSettings.FollowerPassword, err = condString("replication-follower-password")
	if err != nil {
		return nil, err
	}

	ret.WriteTxHeaderVersion, err = condUInt32("write-tx-header-version")
	if err != nil {
		return nil, err
	}

	ret.Autoload, err = condBool("autoload")
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func databaseNullableSettingsStr(settings *schema.DatabaseNullableSettings) string {
	propertiesStr := []string{}

	if settings.ReplicationSettings != nil {
		propertiesStr = append(propertiesStr, fmt.Sprintf("replica: %v", settings.ReplicationSettings.Replica.GetValue()))
	}

	if settings.ExcludeCommitTime != nil {
		propertiesStr = append(propertiesStr, fmt.Sprintf("exclude-commit-time: %v", settings.ExcludeCommitTime.GetValue()))
	}

	if settings.WriteTxHeaderVersion != nil {
		propertiesStr = append(propertiesStr, fmt.Sprintf("write-tx-header-version: %d", settings.WriteTxHeaderVersion.GetValue()))
	}

	if settings.Autoload != nil {
		propertiesStr = append(propertiesStr, fmt.Sprintf("autoload: %v", settings.Autoload.GetValue()))
	}

	return strings.Join(propertiesStr, ", ")
}
