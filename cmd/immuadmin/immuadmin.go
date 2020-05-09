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
	"github.com/codenotary/immudb/cmd/immuadmin/commands"
	"github.com/spf13/cobra"
)

var App = "immuadmin"
var Version string
var Commit string
var BuiltBy string
var BuiltAt string

var o = &c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuadmin") })
}

func main() {
	cmd := &cobra.Command{
		Use:   "immuadmin",
		Short: "CLI admin client for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `CLI admin client for immudb - the lightweight, high-speed immutable database for systems and applications.

Environment variables:
  IMMUADMIN_DEFAULT.IMMUDB-ADDRESS=127.0.0.1
  IMMUADMIN_DEFAULT.IMMUDB-PORT=3322
  IMMUADMIN_DEFAULT.MTLS=true
  IMMUADMIN_DEFAULT.SERVERNAME=localhost
  IMMUADMIN_DEFAULT.PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUADMIN_DEFAULT.CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUADMIN_DEFAULT.CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		SilenceUsage:      true,
		SilenceErrors:     true,
		DisableAutoGenTag: true,
	}
	commands.Init(cmd, o)
	cmd.AddCommand(c.VersionCmd(App, Version, Commit, BuiltBy, BuiltAt))
	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}
