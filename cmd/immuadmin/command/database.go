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
}

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
	addDbUpdateFlags(cc)

	cu := &cobra.Command{
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
	addDbUpdateFlags(cu)

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
			cleanupPercentage, err := cmd.Flags().GetFloat32("cleanup-percentage")
			if err != nil {
				return err
			}

			synced, err := cmd.Flags().GetBool("synced")
			if err != nil {
				return err
			}

			err = cl.immuClient.FlushIndex(cl.context, cleanupPercentage, synced)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Database index successfully flushed\n")
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	fcc.Flags().Float32("cleanup-percentage", 0.00, "set cleanup percentage")
	fcc.Flags().Bool("synced", true, "synced mode enables physical data deletion")

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

	return strings.Join(propertiesStr, ", ")
}
