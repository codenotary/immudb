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

func (cl *commandline) getTxByID(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "tx",
		Short:             "Return a tx by id",
		Aliases:           []string{"tx"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.GetTxByID(args)
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

func (cl *commandline) safegetTxByID(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safetx",
		Short:             "Return a tx by ID",
		Aliases:           []string{"stx"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.VerifiedGetTxByID(args) //TODO: use verified
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

func (cl *commandline) getKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "get key[@revision]",
		Short:             "Get item having the specified key",
		Aliases:           []string{"g"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.Get(args)
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

func (cl *commandline) safeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safeget key",
		Short:             "Get and verify item having the specified key",
		Aliases:           []string{"sg"},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.VerifiedGet(args)
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
