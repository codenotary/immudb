/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"errors"

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
			return errors.New("not supported")
			/*resp, err := cl.immucl.Consistency(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil*/
		},
		Args: cobra.MinimumNArgs(2),
	}
	cmd.AddCommand(ccmd)
}
