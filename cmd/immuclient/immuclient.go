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
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/immuclient/commands"
	"github.com/codenotary/immudb/pkg/client"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var App = "immuclient"
var Version = "v0.0.1"
var Commit string
var BuiltBy string
var BuiltAt string

var o = &c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuclient") })
}

var GeneralClient = &commands.CommandlineClient{}

func main() {
	cmd := &cobra.Command{
		Use:   "immuclient",
		Short: "CLI client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI client for immudb - the lightweight, high-speed immutable database for systems and applications.
Environment variables:
  IMMUCLIENT_ADDRESS=127.0.0.1
  IMMUCLIENT_PORT=3322
  IMMUCLIENT_AUTH=false`,
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if GeneralClient.ImmuClient, err = client.NewImmuClient(options()); err != nil || GeneralClient.ImmuClient == nil {
				c.QuitToStdErr(err)
			}
			return
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if err := GeneralClient.ImmuClient.Disconnect(); err != nil {
				c.QuitToStdErr(err)
			}
		},
	}

	GeneralClient.Init(cmd, o)
	cmd.AddCommand(c.VersionCmd(App, Version, Commit, BuiltBy, BuiltAt))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
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
