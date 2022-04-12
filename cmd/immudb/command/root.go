/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

immudb documentation:
  https://docs.immudb.io/

Environment variables:
  IMMUDB_DIR=.
  IMMUDB_NETWORK=tcp
  IMMUDB_ADDRESS=0.0.0.0
  IMMUDB_PORT=3322
  IMMUDB_REPLICATION_ENABLED=true
  IMMUDB_REPLICATION_MASTER_ADDRESS=127.0.0.1
  IMMUDB_REPLICATION_PORT=3322
  IMMUDB_REPLICATION_USERNAME=immudb
  IMMUDB_REPLICATION_PASSWORD=immudb
  IMMUDB_PIDFILE=
  IMMUDB_LOGFILE=
  IMMUDB_MTLS=false
  IMMUDB_AUTH=true
  IMMUDB_MAX_RECV_MSG_SIZE=4194304
  IMMUDB_DETACHED=false
  IMMUDB_CONSISTENCY_CHECK=true
  IMMUDB_PKEY=
  IMMUDB_CERTIFICATE=
  IMMUDB_CLIENTCAS=
  IMMUDB_DEVMODE=true
  IMMUDB_MAINTENANCE=false
  IMMUDB_ADMIN_PASSWORD=immudb
  IMMUDB_SIGNINGKEY=
  IMMUDB_SYNCED=true
  IMMUDB_TOKEN_EXPIRY_TIME=1440
  IMMUDB_PGSQL_SERVER=true
  IMMUDB_PGSQL_SERVER_PORT=5432
  IMMUDB_MAX_SESSION_AGE_TIME=0 (infinity)
  IMMUDB_MAX_SESSION_INACTIVITY_TIME=3m
  IMMUDB_SESSION_TIMEOUT=2m
  IMMUDB_SESSIONS_GUARD_CHECK_INTERVAL=1m
  LOG_LEVEL={debug|info|warning|error}

`,
		DisableAutoGenTag: true,
		RunE:              cl.Immudb(immudbServer),
		PersistentPreRunE: cl.ConfigChain(nil),
	}

	cl.setupFlags(cmd, server.DefaultOptions())

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return nil, err
	}

	setupDefaults(server.DefaultOptions())

	return cmd, nil
}

// Immudb ...
func (cl *Commandline) Immudb(immudbServer server.ImmuServerIf) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (err error) {
		var options *server.Options
		if options, err = parseOptions(); err != nil {
			return err
		}
		immudbServer := immudbServer.WithOptions(options)
		if options.Logfile != "" {
			if flogger, file, err := logger.NewFileLogger("immudb ", options.Logfile); err == nil {
				defer file.Close()
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

		if err = immudbServer.Initialize(); err != nil {
			return err
		}

		service := server.Service{
			ImmuServerIf: immudbServer,
		}

		d.Run(service)

		return nil
	}
}
