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
	"github.com/spf13/cobra"
)

func (cl *commandline) reference(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "reference refkey key",
		Short:             "Add new reference to an existing key",
		Aliases:           []string{"r"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SetReference(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safereference(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safereference refkey key",
		Short:             "Add and verify new reference to an existing key",
		Aliases:           []string{"sr"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.VerifiedSetReference(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}
