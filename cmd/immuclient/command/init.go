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
	"os"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func (cl *commandline) configureFlags(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", client.DefaultOptions().Port, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", client.DefaultOptions().Address, "immudb host address")
	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.toml)")
	cmd.PersistentFlags().String(
		"tokenfile",
		client.DefaultOptions().TokenFileName,
		fmt.Sprintf(
			"authentication token file (default path is $HOME or binary location; default filename is %s)",
			client.DefaultOptions().TokenFileName))
	cmd.PersistentFlags().BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	cmd.PersistentFlags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	cmd.PersistentFlags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.PersistentFlags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.PersistentFlags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	cmd.PersistentFlags().Bool("value-only", false, "returning only values for get operations")
	cmd.PersistentFlags().String("roots-filepath", "/tmp/", "Filepath for storing root hashes after every successful audit loop. Default is tempdir of every OS.")
	cmd.PersistentFlags().String("prometheus-port", "9477", "Launch port of the Prometheus exporter.")
	cmd.PersistentFlags().String("prometheus-host", "0.0.0.0", "Launch host of the Prometheus exporter.")
	cmd.PersistentFlags().String("dir", os.TempDir(), "Main directory for audit process tool to initialize")
	cmd.PersistentFlags().String("audit-username", "", "immudb username used to login during audit")
	cmd.PersistentFlags().String("audit-password", "", "immudb password used to login during audit; can be plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.PersistentFlags().String("audit-signature", "", "audit signature mode. ignore|validate. If 'ignore' is set auditor doesn't check for the root server signature. If 'validate' is set auditor verify that the root is signed properly by immudb server. Default value is 'ignore'")

	if err := viper.BindPFlag("immudb-port", cmd.PersistentFlags().Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", cmd.PersistentFlags().Lookup("immudb-address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("tokenfile", cmd.PersistentFlags().Lookup("tokenfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("mtls", cmd.PersistentFlags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("servername", cmd.PersistentFlags().Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("certificate", cmd.PersistentFlags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pkey", cmd.PersistentFlags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("clientcas", cmd.PersistentFlags().Lookup("clientcas")); err != nil {
		return err
	}
	if err := viper.BindPFlag("value-only", cmd.PersistentFlags().Lookup("value-only")); err != nil {
		return err
	}
	if err := viper.BindPFlag("roots-filepath", cmd.PersistentFlags().Lookup("roots-filepath")); err != nil {
		return err
	}
	if err := viper.BindPFlag("prometheus-port", cmd.PersistentFlags().Lookup("prometheus-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("prometheus-host", cmd.PersistentFlags().Lookup("prometheus-host")); err != nil {
		return err
	}

	if err := viper.BindPFlag("dir", cmd.PersistentFlags().Lookup("dir")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-username", cmd.PersistentFlags().Lookup("audit-username")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-password", cmd.PersistentFlags().Lookup("audit-password")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-signature", cmd.PersistentFlags().Lookup("audit-signature")); err != nil {
		return err
	}

	viper.SetDefault("immudb-port", client.DefaultOptions().Port)
	viper.SetDefault("immudb-address", client.DefaultOptions().Address)
	viper.SetDefault("tokenfile", client.DefaultOptions().TokenFileName)
	viper.SetDefault("mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("clientcas", client.DefaultMTLsOptions().ClientCAs)
	viper.SetDefault("value-only", false)
	viper.SetDefault("prometheus-port", "9477")
	viper.SetDefault("prometheus-host", "0.0.0.0")
	viper.SetDefault("roots-filepath", os.TempDir())
	viper.SetDefault("audit-password", "")
	viper.SetDefault("audit-username", "")
	viper.SetDefault("audit-signature", "ignore")
	viper.SetDefault("dir", os.TempDir())
	return nil
}
