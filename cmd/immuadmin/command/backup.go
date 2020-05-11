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
