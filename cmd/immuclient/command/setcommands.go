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

func (cl *commandline) rawSafeSet(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "rawsafeset key value",
		Short:             "Set a value for the item having the specified key, without setup structured values",
		Aliases:           []string{"rs"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.RawSafeSet(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) set(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "set key value",
		Short:             "Add new item having the specified key and value",
		Aliases:           []string{"s"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.Set(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(2),
	}

	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeset(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safeset key value",
		Short:             "Add and verify new item having the specified key and value",
		Aliases:           []string{"ss"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SafeSet(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) zAdd(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "zadd setname score key",
		Short:             "Add new key with score to a new or existing sorted set",
		Aliases:           []string{"za"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.ZAdd(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeZAdd(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safezadd setname score key",
		Short:             "Add and verify new key with score to a new or existing sorted set",
		Aliases:           []string{"sza"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SafeZAdd(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}
