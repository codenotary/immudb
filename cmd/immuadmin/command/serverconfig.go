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

package immuadmin

import (
	"fmt"
	"strconv"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/spf13/cobra"
)

func (cl commandline) serverConfig(cmd *cobra.Command) {
	authKinds := map[string]auth.Kind{
		"none":     auth.KindNone,
		"password": auth.KindPassword,
		// "cryptosig": auth.KindCryptoSig,
	}
	ccmd := &cobra.Command{
		Use:               "set auth|mtls value",
		Short:             "Update server config items: auth (none|password|cryptosig), mtls (true|false)",
		PersistentPreRunE: cl.ConfigChain(cl.checkLoggedInAndConnect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cl.context
			configItem := args[0]
			v := args[1]
			switch configItem {
			case "auth":
				authKind, ok := authKinds[v]
				if !ok {
					return fmt.Errorf(
						"unsupported %s auth mode, only none and password are currently supported", v)
				}
				if err := cl.immuClient.UpdateAuthConfig(ctx, authKind); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Server auth config updated\n")

			case "mtls":
				enabled, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("unsupported value %s, server MTLS can be set to true or false", v)
				}
				if err := cl.immuClient.UpdateMTLSConfig(ctx, enabled); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Server MTLS config updated\n")
			default:
				return fmt.Errorf(
					"unsupported %s config item, supported config items: auth or mtls", configItem)
			}
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
