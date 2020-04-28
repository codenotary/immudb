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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/cmd/immuadmin/statisticscmd"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/client"
)

var o = c.Options{}
var immuClient client.ImmuClient

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuadmin") })
}

func main() {

	cmd := &cobra.Command{
		Use:   "immuadmin",
		Short: "CLI admin client for the ImmuDB tamperproof database",
		Long: `CLI admin client for the ImmuDB tamperproof database.

Environment variables:
  IMMUADMIN_ADDRESS=127.0.0.1
  IMMUADMIN_PORT=3322
  IMMUADMIN_MTLS=true`,
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if immuClient, err = client.NewImmuClient(options()); err != nil {
				c.QuitToStdErr(err)
			}
			return
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if err := immuClient.Disconnect(); err != nil {
				c.QuitToStdErr(err)
			}
		},
	}
	commands := []*cobra.Command{
		{
			Use:     "login username \"password\"",
			Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
			Aliases: []string{"l"},
			RunE: func(cmd *cobra.Command, args []string) error {
				user := []byte(args[0])
				pass := []byte(args[1])
				ctx := context.Background()
				response, err := immuClient.Login(ctx, user, pass)
				if err != nil {
					c.QuitWithUserError(err)
				}
				if err := ioutil.WriteFile(client.TokenFileName, response.Token, 0644); err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("logged in\n")
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
			Use:     "logout",
			Aliases: []string{"x"},
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := os.Remove(client.TokenFileName); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("logged out")
				return nil
			},
			Args: cobra.NoArgs,
		},
		{
			Use:     "status",
			Short:   "Show heartbeat status",
			Aliases: []string{"p"},
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				if err := immuClient.HealthCheck(ctx); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("OK - server is reachable and responding to queries")
				return nil
			},
			Args: cobra.NoArgs,
		},
		statisticscmd.NewCommand(optionsWithAuth, &immuClient),
	}

	if err := configureOptions(cmd); err != nil {
		c.QuitToStdErr(err)
	}

	for _, command := range commands {
		cmd.AddCommand(command)
	}

	cmd.AddCommand(man.Generate(cmd, "immuadmin", "../docs/man/immuadmin"))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}

func configureOptions(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.ini)")
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
	viper.SetDefault("default.mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("default.servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("default.certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("default.pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("default.clientcas", client.DefaultMTLsOptions().ClientCAs)

	return nil
}

func options() *client.Options {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	mtls := viper.GetBool("default.mtls")
	certificate := viper.GetString("default.certificate")
	servername := viper.GetString("default.servername")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(true).
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

func optionsWithAuth() *client.Options {
	var token string
	tokenBytes, err := ioutil.ReadFile(client.TokenFileName)
	if err == nil {
		token = string(tokenBytes)
	}
	opts := options()
	dialOpts := opts.DialOptions
	if dialOpts == nil {
		dialOpts = &[]grpc.DialOption{}
	}
	extraDialOpts := []grpc.DialOption{
		// TODO OGG: check if this is really needed
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
		grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
	}
	newDialOpts := append(*dialOpts, extraDialOpts...)
	opts = opts.WithDialOptions(&newDialOpts)
	return opts
}
