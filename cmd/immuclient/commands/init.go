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

package commands

import (
	"github.com/codenotary/immudb/cmd"
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	ImmuClient     client.ImmuClient
	passwordReader cmd.PasswordReader
}

func Init(cmd *cobra.Command, o *c.Options) {
	if err := configureOptions(cmd, o); err != nil {
		c.QuitToStdErr(err)
	}
	cl := new(commandline)
	cl.passwordReader = c.DefaultPasswordReader

	// login and logout
	cl.login(cmd)
	cl.logout(cmd)
	// current status
	cl.currentRoot(cmd)
	// get operations
	cl.getByIndex(cmd)
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
	cl.healthCheck(cmd)
	cl.dumpToFile(cmd)

	// man file generator
	cmd.AddCommand(man.Generate(cmd, "immuclient", "./cmd/docs/man/immuclient"))
}

func options() *client.Options {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	authEnabled := viper.GetBool("default.auth")
	mtls := viper.GetBool("default.mtls")
	certificate := viper.GetString("default.certificate")
	servername := viper.GetString("default.servername")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(authEnabled).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	if cl.ImmuClient, err = client.NewImmuClient(options()); err != nil || cl.ImmuClient == nil {
		c.QuitToStdErr(err)
	}
	return
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.ImmuClient.Disconnect(); err != nil {
		c.QuitToStdErr(err)
	}
}

func configureOptions(cmd *cobra.Command, o *c.Options) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.ini)")
	cmd.PersistentFlags().BoolP("auth", "s", client.DefaultOptions().Auth, "use authentication")
	cmd.PersistentFlags().BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	cmd.PersistentFlags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	cmd.PersistentFlags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.PersistentFlags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.PersistentFlags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	if err := viper.BindPFlag("default.port", cmd.PersistentFlags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.PersistentFlags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.auth", cmd.PersistentFlags().Lookup("auth")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.mtls", cmd.PersistentFlags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.servername", cmd.PersistentFlags().Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.certificate", cmd.PersistentFlags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pkey", cmd.PersistentFlags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.clientcas", cmd.PersistentFlags().Lookup("clientcas")); err != nil {
		return err
	}
	viper.SetDefault("default.port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("default.address", gw.DefaultOptions().ImmudbAddress)
	viper.SetDefault("default.auth", client.DefaultOptions().Auth)
	viper.SetDefault("default.mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("default.servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("default.certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("default.pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("default.clientcas", client.DefaultMTLsOptions().ClientCAs)

	return nil
}
