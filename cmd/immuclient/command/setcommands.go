/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
			fprintln(cmd.OutOrStdout(), resp)
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
			resp, err := cl.immucl.VerifiedSet(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) restore(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "restore key@revision",
		Short:             "Restore value for the key to given revision number",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.Restore(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}

	cmd.AddCommand(ccmd)
}

func (cl *commandline) deleteKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "delete key value",
		Short:             "Delete existent entry having the specified key (logical deletion)",
		Aliases:           []string{"del"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.DeleteKey(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(1),
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
			fprintln(cmd.OutOrStdout(), resp)
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
			resp, err := cl.immucl.VerifiedZAdd(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}
