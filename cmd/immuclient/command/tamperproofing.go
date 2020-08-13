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

package immuclient

import (
	"fmt"

	"github.com/spf13/cobra"
)

func (cl *commandline) consistency(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "check-consistency index hash",
		Short:             "Check consistency for the specified index and hash",
		Aliases:           []string{"c"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.Consistency(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.MinimumNArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) inclusion(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "inclusion index",
		Short:             "Check if specified index is included in the current tree",
		Aliases:           []string{"i"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.Inclusion(args)
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
