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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	immuClient     client.ImmuClient
	passwordReader cmd.PasswordReader
}

func Init(cmd *cobra.Command) {
	cl := new(commandline)
	cl.passwordReader = c.DefaultPasswordReader

	cl.user(cmd)
	cl.login(cmd)
	cl.logout(cmd)
	cl.status(cmd)
	cl.stats(cmd)
	cmd.AddCommand(man.Generate(cmd, "immuadmin", "../docs/man/immuadmin"))
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
