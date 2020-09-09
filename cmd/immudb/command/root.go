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

package immudb

import (
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	daem "github.com/takama/daemon"
)

func (cl *Commandline) NewRootCmd(immudbServer server.ImmuServerIf) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "immudb",
		Short: "immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `immudb - the lightweight, high-speed immutable database for systems and applications.

Environment variables:
  IMMUDB_DIR=.
  IMMUDB_NETWORK=tcp
  IMMUDB_ADDRESS=0.0.0.0
  IMMUDB_PORT=3322
  IMMUDB_DBNAME=immudb
  IMMUDB_PIDFILE=
  IMMUDB_LOGFILE=
  IMMUDB_MTLS=false
  IMMUDB_AUTH=true
  IMMUDB_DETACHED=false
  IMMUDB_CONSISTENCY_CHECK=true
  IMMUDB_PKEY=./tools/mtls/3_application/private/localhost.key.pem
  IMMUDB_CERTIFICATE=./tools/mtls/3_application/certs/localhost.cert.pem
  IMMUDB_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem
  IMMUDB_DEVMODE=true
  IMMUDB_MAINTENANCE=false
  IMMUDB_ADMIN_PASSWORD=immudb,
  IMMUDB_SIGNING_KEY=`,
		DisableAutoGenTag: true,
		RunE:              cl.Immudb(immudbServer),
		PersistentPreRunE: cl.ConfigChain(nil),
	}

	cl.setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return nil, err
	}

	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	return cmd, nil
}

// Immudb ...
func (cl *Commandline) Immudb(immudbServer server.ImmuServerIf) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (err error) {
		var options server.Options
		if options, err = parseOptions(); err != nil {
			return err
		}
		immudbServer := immudbServer.WithOptions(options)
		if options.Logfile != "" {
			if flogger, file, err := logger.NewFileLogger("immudb ", options.Logfile); err == nil {
				defer func() {
					if err = file.Close(); err != nil {
						c.QuitToStdErr(err)
					}
				}()
				immudbServer.WithLogger(flogger)
			} else {
				c.QuitToStdErr(err)
			}
		}
		if options.Detached {
			if err := cl.P.Detached(); err == nil {
				return nil
			}
		}

		var d daem.Daemon
		if d, err = daem.New("immudb", "immudb", "immudb"); err != nil {
			c.QuitToStdErr(err)
		}

		service := server.Service{
			ImmuServerIf: immudbServer,
		}

		d.Run(service)

		return nil
	}
}
