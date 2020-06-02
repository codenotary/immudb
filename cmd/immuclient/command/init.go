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

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/cli"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	immucl immuc.Client
}

func Init(o *c.Options) *cobra.Command {

	cl := new(commandline)
	cmd := &cobra.Command{
		Use:   "immuclient",
		Short: "CLI client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI client for immudb - the lightweight, high-speed immutable database for systems and applications.
Environment variables:
  IMMUCLIENT_IMMUDB_ADDRESS=127.0.0.1
  IMMUCLIENT_IMMUDB_PORT=3322
  IMMUCLIENT_AUTH=false
  IMMUCLIENT_MTLS=false
  IMMUCLIENT_SERVERNAME=localhost
  IMMUCLIENT_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUCLIENT_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUCLIENT_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cl.immucl.Connect(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			cli.Init(cl.immucl).Run()
			return nil
		},
	}
	if err := configureOptions(cmd, o); err != nil {
		c.QuitToStdErr(err)
	}
	var err error
	cl.immucl, err = immuc.Init()
	if err != nil {
		c.QuitToStdErr(err)
	}
	// login and logout
	cl.login(cmd)
	cl.logout(cmd)
	// current status
	cl.currentRoot(cmd)
	// get operations
	cl.getByIndex(cmd)
	cl.getRawBySafeIndex(cmd)
	cl.getKey(cmd)
	cl.rawSafeGetKey(cmd)
	cl.safeGetKey(cmd)
	// set operations
	cl.rawSafeSet(cmd)
	cl.set(cmd)
	cl.safeset(cmd)
	cl.zAdd(cmd)
	cl.safeZAdd(cmd)
	// scanners
	cl.zScan(cmd)
	cl.iScan(cmd)
	cl.scan(cmd)
	cl.count(cmd)
	// references
	cl.reference(cmd)
	cl.safereference(cmd)
	// misc
	cl.inclusion(cmd)
	cl.consistency(cmd)
	cl.history(cmd)
	cl.status(cmd)
	cl.auditmode(cmd)
	cl.interactiveCli(cmd)
	// man file generator
	cmd.AddCommand(man.Generate(cmd, "immuclient", "./cmd/docs/man/immuclient"))
	return cmd
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	err = cl.immucl.Connect(args)
	if err != nil {
		c.QuitToStdErr(err)
	}
	return
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immucl.Disconnect(args); err != nil {
		c.QuitToStdErr(err)
	}
	os.Exit(0)
}

func configureOptions(cmd *cobra.Command, o *c.Options) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", client.DefaultOptions().Port, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", client.DefaultOptions().Address, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.toml)")
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
	cmd.PersistentFlags().String("prometheus-host", "127.0.0.1", "Launch host of the Prometheus exporter.")
	cmd.PersistentFlags().String("dir", os.TempDir(), "Main directory for audit process tool to initialize")
	cmd.PersistentFlags().String("audit-username", "", "immudb username used to login during audit")
	cmd.PersistentFlags().String("audit-password", "", "immudb password used to login during audit")

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
	viper.SetDefault("prometheus-host", "127.0.0.1")
	viper.SetDefault("roots-filepath", os.TempDir())
	viper.SetDefault("audit-password", "")
	viper.SetDefault("audit-username", "")
	viper.SetDefault("dir", os.TempDir())
	o.InitConfig("")
	return nil
}
