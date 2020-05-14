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
	"os"
	"time"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) dumpToFile(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "dump [file]",
		Short:             "Dump database content to a file",
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := fmt.Sprint("immudb_" + time.Now().Format("2006-01-02_15-04-05") + ".bkp")
			if len(args) > 0 {
				filename = args[0]
			}
			file, err := os.Create(filename)
			defer file.Close()
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := cl.context
			response, err := cl.immuClient.Dump(ctx, file)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response, filename)
			return nil
		},
		Args: cobra.MaximumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) backup(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "backup [--uncompressed]",
		Short: "Make a copy of the database files and folders",
		Long: "Pause the immudb server, create and save on the server machine a snapshot " +
			"of the database files and folders (zip on Windows, tar.gz on Linux or uncompressed).",
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			uncompressed, err := cmd.Flags().GetBool("uncompressed")
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := cl.context
			response, err := cl.immuClient.Backup(ctx, uncompressed)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("Snapshot created on the server: %s\n", response.GetMessage())
			return nil
		},
		Args: cobra.NoArgs,
	}
	ccmd.Flags().BoolP("uncompressed", "u", false, "create an uncompressed snapshot")
	cmd.AddCommand(ccmd)
}

func (cl *commandline) restore(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   "restore snapshot-path",
		Short: "Restore the database from a snapshot archive or folder",
		Long: "Pause the immudb server and restore the database files and folders from a snapshot " +
			"file (zip or tar.gz) or folder (uncompressed) residing on the server machine.",
		PersistentPreRunE: cl.checkLoggedInAndConnect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			snapshotPath := args[0]
			ctx := cl.context
			err := cl.immuClient.Restore(ctx, []byte(snapshotPath))
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("Database restored from snapshot at %s\n", snapshotPath)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}
