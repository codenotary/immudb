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
	"github.com/codenotary/immudb/cmd/immuclient/cli"
	"github.com/spf13/cobra"
)

// NewCmd ...
func (cl *commandline) NewCmd() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "immuclient",
		Short: "CLI client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI client for immudb - the lightweight, high-speed immutable database for systems and applications.

immudb documentation:
  https://docs.immudb.io/

Environment variables:
  IMMUCLIENT_IMMUDB_ADDRESS=127.0.0.1
  IMMUCLIENT_IMMUDB_PORT=3322
  IMMUCLIENT_AUTH=true
  IMMUCLIENT_USERNAME=username
  IMMUCLIENT_PASSWORD=password
  IMMUCLIENT_DATABASE=database
  IMMUCLIENT_MTLS=false
  IMMUCLIENT_MAX_RECV_MSG_SIZE=4194304
  IMMUCLIENT_SERVERNAME=localhost
  IMMUCLIENT_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUCLIENT_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUCLIENT_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem
  IMMUCLIENT_SERVER_SIGNING_PUB_KEY=
  IMMUCLIENT_REVISION_SEPARATOR=@

IMPORTANT: All get and safeget functions return base64-encoded keys and values, while all set and safeset functions expect base64-encoded inputs.`,
		DisableAutoGenTag: true,
		PersistentPreRunE: cl.ConfigChain(nil),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immucl.Connect(args)
			if err != nil {
				cl.quit(err)
			}
			cli.Init(cl.immucl).Run()
			return nil
		},
	}
	if err := cl.configureFlags(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}
