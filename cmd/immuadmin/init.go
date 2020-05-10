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

package immuadmin

import (
	"context"
	"fmt"
	"os"

	c "github.com/codenotary/immudb/cmd/command"
	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/common"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
)

type commandline struct {
	immuClient     client.ImmuClient
	passwordReader c.PasswordReader
	context        context.Context
}

type commandlineDisc struct{}

func Init(cmd *cobra.Command, o *c.Options) {
	if err := configureOptions(cmd, o); err != nil {
		c.QuitToStdErr(err)
	}
	cl := new(commandline)
	cl.passwordReader = c.DefaultPasswordReader
	cl.context = metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs(common.ClientIDMetadataKey, common.ClientIDMetadataValueAdmin))

	cl.user(cmd)
	cl.login(cmd)
	cl.logout(cmd)
	cl.status(cmd)
	cl.stats(cmd)
	cl.dumpToFile(cmd)

	cld := new(commandlineDisc)
	cld.service(cmd)

	cmd.AddCommand(man.Generate(cmd, "immuadmin", "./cmd/docs/man/immuadmin"))
}

func options() *client.Options {
	port := viper.GetInt("immudb-port")
	address := viper.GetString("immudb-address")
	mtls := viper.GetBool("mtls")
	certificate := viper.GetString("certificate")
	servername := viper.GetString("servername")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(true).
		WithTokenFileName("token_admin").
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

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immuClient.Disconnect(); err != nil {
		c.QuitToStdErr(err)
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	if cl.immuClient, err = client.NewImmuClient(options()); err != nil {
		c.QuitToStdErr(err)
	}
	return
}
func (cl *commandline) checkLoggedInAndConnect(cmd *cobra.Command, args []string) (err error) {
	opts := options()
	if _, err = os.Stat(opts.TokenFileName); err != nil && os.IsNotExist(err) {
		c.QuitToStdErr(fmt.Errorf("please login first"))
	}
	if cl.immuClient, err = client.NewImmuClient(opts); err != nil {
		c.QuitToStdErr(err)
	}
	return
}

func configureOptions(cmd *cobra.Command, o *c.Options) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuadmin.toml)")
	cmd.PersistentFlags().BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	cmd.PersistentFlags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	cmd.PersistentFlags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.PersistentFlags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.PersistentFlags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	if err := viper.BindPFlag("immudb-port", cmd.PersistentFlags().Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", cmd.PersistentFlags().Lookup("immudb-address")); err != nil {
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
	viper.SetDefault("immudb-port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("immudb-address", gw.DefaultOptions().ImmudbAddress)
	viper.SetDefault("mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("clientcas", client.DefaultMTLsOptions().ClientCAs)

	return nil
}
