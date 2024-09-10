/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immudb

import (
	"path/filepath"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/embedded/logger"
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

immudb documentation: https://docs.immudb.io/

Setting the logging level and other options through environment variables:
- Logging level: LOG_LEVEL={debug|info|warning|error}
- The environment variable names for other settings are derived by prefixing flag names with "IMMUDB_"
  e.g IMMUDB_PORT=3323 ./immudb.
  Note: flags take precedence over environment variables.
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

		// initialize logger for immudb
		ilogger, err := logger.NewLogger(&logger.Options{
			Name:              "immudb",
			LogFormat:         options.LogFormat,
			LogDir:            filepath.Join(options.Dir, options.LogDir),
			LogFile:           options.Logfile,
			LogRotationSize:   options.LogRotationSize,
			LogRotationAge:    options.LogRotationAge,
			LogFileTimeFormat: logger.LogFileFormat,
			Level:             logger.LogLevelFromEnvironment(),
		})
		if err != nil {
			c.QuitToStdErr(err)
		}
		defer ilogger.Close()
		immudbServer.WithLogger(ilogger)

		// check if immudb needs to be run in detached mode
		if options.Detached {
			if err := cl.P.Detached(); err == nil {
				return nil
			}
		}

		// check if immudb needs to run in daemon mode
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
