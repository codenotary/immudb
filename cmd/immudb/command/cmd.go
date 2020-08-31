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
	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	daem "github.com/takama/daemon"
)

func Execute() {
	version.App = "immudb"
	cl := Commandline{P: c.NewPlauncher(), config: c.Config{Name: "immudb"}}
	cmd, err := cl.NewCmd(server.DefaultServer())
	if err != nil {
		c.QuitWithUserError(err)
	}
	if err := cmd.Execute(); err != nil {
		c.QuitWithUserError(err)
	}
}

// NewCmd ...
func (cl *Commandline) NewCmd(immudbServer server.ImmuServerIf) (*cobra.Command, error) {
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
  IMMUDB_SIGNATUREPRIVATEKEY=./test/signer/ec3.key`,
		DisableAutoGenTag: true,
		RunE:              cl.Immudb(immudbServer),
		PersistentPreRunE: cl.ConfigChain(nil),
	}

	cl.setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return nil, err
	}

	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	cmd.AddCommand(man.Generate(cmd, "immudb", "./cmd/docs/man/immudb"))
	cmd.AddCommand(version.VersionCmd())

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

func parseOptions() (options server.Options, err error) {
	dir, err := c.ResolvePath(viper.GetString("dir"), true)
	if err != nil {
		return options, err
	}
	port := viper.GetInt("port")
	address := viper.GetString("address")
	if err != nil {
		return options, err
	}
	pidfile, err := c.ResolvePath(viper.GetString("pidfile"), true)
	if err != nil {
		return options, err
	}
	logfile, err := c.ResolvePath(viper.GetString("logfile"), true)
	if err != nil {
		return options, err
	}
	mtls := viper.GetBool("mtls")
	auth := viper.GetBool("auth")
	noHistograms := viper.GetBool("no-histograms")
	detached := viper.GetBool("detached")
	consistencyCheck := viper.GetBool("consistency-check")
	certificate, err := c.ResolvePath(viper.GetString("certificate"), true)
	if err != nil {
		return options, err
	}
	pkey, err := c.ResolvePath(viper.GetString("pkey"), true)
	if err != nil {
		return options, err
	}
	clientcas, err := c.ResolvePath(viper.GetString("clientcas"), true)
	if err != nil {
		return options, err
	}
	devMode := viper.GetBool("devmode")
	adminPassword := viper.GetString("admin-password")
	maintenance := viper.GetBool("maintenance")
	signaturePrivateKey := viper.GetString("signaturePrivateKey")
	options = server.
		DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls).
		WithAuth(auth).
		WithNoHistograms(noHistograms).
		WithDetached(detached).
		WithCorruptionCheck(consistencyCheck).
		WithDevMode(devMode).
		WithAdminPassword(adminPassword).
		WithMaintenance(maintenance).
		WithSignaturePrivateKey(signaturePrivateKey)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = server.DefaultMTLsOptions().
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options, nil
}

func (cl *Commandline) setupFlags(cmd *cobra.Command, options server.Options, mtlsOptions server.MTLsOptions) {
	cmd.Flags().String("dir", options.Dir, "data folder")
	cmd.Flags().IntP("port", "p", options.Port, "port number")
	cmd.Flags().StringP("address", "a", options.Address, "bind address")
	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immudb.toml)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immudb.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immudb/immudb.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().BoolP("auth", "s", options.MTLs, "enable auth")
	cmd.Flags().Bool("no-histograms", options.MTLs, "disable collection of histogram metrics like query durations")
	cmd.Flags().Bool("consistency-check", options.CorruptionCheck, "enable consistency check monitor routine. To disable: --consistency-check=false")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
	cmd.Flags().Bool("devmode", options.DevMode, "enable dev mode: accept remote connections without auth")
	cmd.Flags().String("admin-password", options.AdminPassword, "admin password (default is 'immudb') as plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.Flags().Bool("maintenance", options.GetMaintenance(), "override the authentication flag")
	cmd.Flags().String("signaturePrivateKey", options.SignaturePrivateKey, "signature private key. If not empty, it enables the cryptographic signature of the root")
}

func setupDefaults(options server.Options, mtlsOptions server.MTLsOptions) {
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("mtls", options.MTLs)
	viper.SetDefault("auth", options.GetAuth())
	viper.SetDefault("no-histograms", options.NoHistograms)
	viper.SetDefault("consistency-check", options.CorruptionCheck)
	viper.SetDefault("detached", options.Detached)
	viper.SetDefault("certificate", mtlsOptions.Certificate)
	viper.SetDefault("pkey", mtlsOptions.Pkey)
	viper.SetDefault("clientcas", mtlsOptions.ClientCAs)
	viper.SetDefault("devmode", options.DevMode)
	viper.SetDefault("admin-password", options.AdminPassword)
	viper.SetDefault("maintenance", options.GetMaintenance())
}
