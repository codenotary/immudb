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

import "github.com/spf13/cobra"

func (cl *commandline) NewCmd() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "immuadmin",
		Short: "CLI admin client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI admin client for immudb - the lightweight, high-speed immutable database for systems and applications.

immudb documentation:
  https://docs.immudb.io/

Environment variables:
  IMMUADMIN_IMMUDB_ADDRESS=127.0.0.1
  IMMUADMIN_IMMUDB_PORT=3322
  IMMUADMIN_MTLS=true
  IMMUADMIN_SERVERNAME=localhost
  IMMUADMIN_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUADMIN_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUADMIN_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		SilenceUsage:      false,
		SilenceErrors:     false,
		DisableAutoGenTag: true,
		PersistentPreRunE: cl.ConfigChain(nil),
	}

	if err := cl.configureFlags(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}
