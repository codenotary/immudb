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
	"fmt"
	"os"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cl *commandline) configureFlags(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", client.DefaultOptions().Port, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", client.DefaultOptions().Address, "immudb host address")
	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.toml)")
	cmd.PersistentFlags().String("username", "", "immudb username used to login")
	cmd.PersistentFlags().String("password", "", "immudb password used to login; can be plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.PersistentFlags().String("database", "", "immudb database to be used")
	cmd.PersistentFlags().String(
		"tokenfile",
		client.DefaultOptions().TokenFileName,
		fmt.Sprintf(
			"authentication token file (default path is $HOME or binary location; default filename is %s)",
			client.DefaultOptions().TokenFileName))
	cmd.PersistentFlags().BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	cmd.PersistentFlags().Int("max-recv-msg-size", client.DefaultOptions().MaxRecvMsgSize, "max message size in bytes the client can receive")
	cmd.PersistentFlags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	cmd.PersistentFlags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.PersistentFlags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.PersistentFlags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	cmd.PersistentFlags().Bool("value-only", false, "returning only values for get operations")
	cmd.PersistentFlags().String("revision-separator", "@", "Separator between the key name and a revision number when doing a get operation, use empty string to disable")
	cmd.PersistentFlags().String("roots-filepath", "/tmp/", "Filepath for storing root hashes after every successful audit loop. Default is tempdir of every OS.")
	cmd.PersistentFlags().String("dir", os.TempDir(), "Main directory for audit process tool to initialize")
	cmd.PersistentFlags().String("audit-username", "", "immudb username used to login during audit")
	cmd.PersistentFlags().String("audit-password", "", "immudb password used to login during audit; can be plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.PersistentFlags().String("audit-databases", "", "Optional comma-separated list of databases (names) to be audited. Can be full name(s) or just name prefix(es).")
	cmd.PersistentFlags().String("audit-notification-url", "", "If set, auditor will send a POST request at this URL with audit result details.")
	cmd.PersistentFlags().String("audit-notification-username", "", "Username used to authenticate when publishing audit result to 'audit-notification-url'.")
	cmd.PersistentFlags().String("audit-notification-password", "", "Password used to authenticate when publishing audit result to 'audit-notification-url'.")
	cmd.PersistentFlags().String("audit-monitoring-host", "0.0.0.0", "Host for the monitoring HTTP server when running in audit mode (serves endpoints like metrics, health and version).")
	cmd.PersistentFlags().Int("audit-monitoring-port", 9477, "Port for the monitoring HTTP server when running in audit mode (serves endpoints like metrics, health and version).")
	cmd.PersistentFlags().String("server-signing-pub-key", "", "Path to the public key to verify signatures when presents")

	viper.BindPFlag("immudb-port", cmd.PersistentFlags().Lookup("immudb-port"))
	viper.BindPFlag("immudb-address", cmd.PersistentFlags().Lookup("immudb-address"))
	viper.BindPFlag("username", cmd.PersistentFlags().Lookup("username"))
	viper.BindPFlag("password", cmd.PersistentFlags().Lookup("password"))
	viper.BindPFlag("database", cmd.PersistentFlags().Lookup("database"))
	viper.BindPFlag("tokenfile", cmd.PersistentFlags().Lookup("tokenfile"))
	viper.BindPFlag("mtls", cmd.PersistentFlags().Lookup("mtls"))
	viper.BindPFlag("max-recv-msg-size", cmd.PersistentFlags().Lookup("max-recv-msg-size"))
	viper.BindPFlag("servername", cmd.PersistentFlags().Lookup("servername"))
	viper.BindPFlag("certificate", cmd.PersistentFlags().Lookup("certificate"))
	viper.BindPFlag("pkey", cmd.PersistentFlags().Lookup("pkey"))
	viper.BindPFlag("clientcas", cmd.PersistentFlags().Lookup("clientcas"))
	viper.BindPFlag("value-only", cmd.PersistentFlags().Lookup("value-only"))
	viper.BindPFlag("revision-separator", cmd.PersistentFlags().Lookup("revision-separator"))
	viper.BindPFlag("roots-filepath", cmd.PersistentFlags().Lookup("roots-filepath"))
	viper.BindPFlag("dir", cmd.PersistentFlags().Lookup("dir"))
	viper.BindPFlag("audit-username", cmd.PersistentFlags().Lookup("audit-username"))
	viper.BindPFlag("audit-password", cmd.PersistentFlags().Lookup("audit-password"))
	viper.BindPFlag("audit-databases", cmd.PersistentFlags().Lookup("audit-databases"))
	viper.BindPFlag("audit-notification-url", cmd.PersistentFlags().Lookup("audit-notification-url"))
	viper.BindPFlag("audit-notification-username", cmd.PersistentFlags().Lookup("audit-notification-username"))
	viper.BindPFlag("audit-notification-password", cmd.PersistentFlags().Lookup("audit-notification-password"))
	viper.BindPFlag("audit-monitoring-host", cmd.PersistentFlags().Lookup("audit-monitoring-host"))
	viper.BindPFlag("audit-monitoring-port", cmd.PersistentFlags().Lookup("audit-monitoring-port"))
	viper.BindPFlag("server-signing-pub-key", cmd.PersistentFlags().Lookup("server-signing-pub-key"))

	viper.SetDefault("immudb-port", client.DefaultOptions().Port)
	viper.SetDefault("immudb-address", client.DefaultOptions().Address)
	viper.SetDefault("password", "")
	viper.SetDefault("username", "")
	viper.SetDefault("database", "")
	viper.SetDefault("tokenfile", client.DefaultOptions().TokenFileName)
	viper.SetDefault("mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("max-recv-msg-size", client.DefaultOptions().MaxRecvMsgSize)
	viper.SetDefault("servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("clientcas", client.DefaultMTLsOptions().ClientCAs)
	viper.SetDefault("value-only", false)
	viper.SetDefault("revision-separator", "@")
	viper.SetDefault("roots-filepath", os.TempDir())
	viper.SetDefault("audit-password", "")
	viper.SetDefault("audit-username", "")
	viper.SetDefault("audit-databases", "")
	viper.SetDefault("audit-notification-url", "")
	viper.SetDefault("audit-notification-username", "")
	viper.SetDefault("audit-notification-password", "")
	viper.SetDefault("audit-monitoring-host", "0.0.0.0")
	viper.SetDefault("audit-monitoring-port", 9477)
	viper.SetDefault("server-signing-pub-key", "")
	viper.SetDefault("dir", os.TempDir())
	return nil
}
