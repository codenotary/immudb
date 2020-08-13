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

func (cl *commandline) getByIndex(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "getByIndex",
		Short:             "Return an element by index",
		Aliases:           []string{"bi"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.GetByIndex(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) getRawBySafeIndex(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "getRawBySafeIndex",
		Short:             "Return an element by index",
		Aliases:           []string{"brsi"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.GetRawBySafeIndex(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) getKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "get key",
		Short:             "Get item having the specified key",
		Aliases:           []string{"g"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.GetKey(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) rawSafeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "rawsafeget key",
		Short:             "Get item having the specified key, without parsing structured values",
		Aliases:           []string{"rg"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.RawSafeGetKey(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safeget key",
		Short:             "Get and verify item having the specified key",
		Aliases:           []string{"sg"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.SafeGetKey(args)
			if err != nil {
				cl.quit(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), resp+"\n")
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}
