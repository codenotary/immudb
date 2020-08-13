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
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) printTree(rootCmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "print",
		Short:             "Print merkle tree",
		Aliases:           []string{"prt"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cl.context
			tree, err := cl.immuClient.PrintTree(ctx)
			if err != nil {
				c.QuitWithUserError(err)
			}
			c.PrintfColorW(cmd.OutOrStdout(), c.White, "\t\t\t\t\t\t\t\tDisk elements\n")
			c.PrintfColorW(cmd.OutOrStdout(), c.Green, "\tImmudb Merkle Tree\t\t\t\t\tCache elements\n")
			c.PrintfColorW(cmd.OutOrStdout(), c.White, "\t\t\t\t\t\t\t\t* refKey presents\n")
			c.PrintfColorW(cmd.OutOrStdout(), c.Red, "\t\t\t\t\t\t\t\tRoot\n")
			c.PrintfColorW(cmd.OutOrStdout(), c.Purple, "\t\t\t\t\t\t\t\tCached Root\n\n")

			for k, l := range tree.T {
				c.PrintfColorW(cmd.OutOrStdout(), c.Yellow, "Layer %d\n", k)
				for _, h := range l.L {
					color := c.White
					if h.Cache {
						color = c.Green
					}
					if h.Root {
						color = c.Red
					}
					if h.Root && h.Cache {
						color = c.Purple
					}
					strp := "%x %d"
					if h.Ref {
						strp += "*"
					}
					strp += "\n"
					c.PrintfColorW(cmd.OutOrStdout(), color, strp, h.H, h.I)
				}
			}
			return nil
		},
		Args: cobra.NoArgs,
	}
	rootCmd.AddCommand(ccmd)
}
