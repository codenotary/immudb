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

package immugw

import (
	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	daem "github.com/takama/daemon"
	"google.golang.org/grpc"
	"os"
)

var o = c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immugw") })
}

// NewCmd creates a new immugw command
func NewCmd(immugwServer gw.ImmuGw) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "immugw",
		Short: "immu gateway: a smart REST proxy for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `immu gateway: a smart REST proxy for immudb - the lightweight, high-speed immutable database for systems and applications.
It exposes all gRPC methods with a REST interface while wrapping all SAFE endpoints with a verification service.

Environment variables:
  IMMUGW_ADDRESS=0.0.0.0
  IMMUGW_PORT=3323
  IMMUGW_IMMUDB_ADDRESS=127.0.0.1
  IMMUGW_IMMUDB_PORT=3322
  IMMUGW_DIR=.
  IMMUGW_PIDFILE=
  IMMUGW_LOGFILE=
  IMMUGW_DETACHED=false
  IMMUGW_MTLS=false
  IMMUGW_SERVERNAME=localhost
  IMMUGW_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUGW_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUGW_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		RunE: Immugw(immugwServer),
	}

	setupFlags(cmd, gw.DefaultOptions(), client.DefaultMTLsOptions())

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		c.QuitToStdErr(err)
	}

	setupDefaults(gw.DefaultOptions(), client.DefaultMTLsOptions())
	cmd.AddCommand(man.Generate(cmd, "immugw", "./cmd/docs/man/immugw"))

	cmd.AddCommand(version.VersionCmd())

	return cmd
}

// Immugw returns a new immudb gateway service
func Immugw(immugwServer gw.ImmuGw) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (err error) {
		var options gw.Options
		if options, err = parseOptions(cmd); err != nil {
			return err
		}
		cliOpts := client.Options{
			Dir:                options.Dir,
			Address:            options.ImmudbAddress,
			Port:               options.ImmudbPort,
			HealthCheckRetries: 1,
			MTLs:               options.MTLs,
			MTLsOptions:        options.MTLsOptions,
			Auth:               true,
			Config:             "",
			DialOptions:        &[]grpc.DialOption{},
			HDS:                client.NewHomedirService(),
		}

		immuGwServer := immugwServer.
			WithOptions(options).WithCliOptions(cliOpts)
		if options.Logfile != "" {
			if flogger, file, err := logger.NewFileLogger("immugw ", options.Logfile); err == nil {
				defer func() {
					if err = file.Close(); err != nil {
						c.QuitToStdErr(err)
					}
				}()
				immuGwServer.WithLogger(flogger)
			} else {
				return err
			}
		}
		plauncher := c.NewPlauncher()
		if options.Detached {
			if err := plauncher.Detached(); err == nil {
				os.Exit(0)
			}
		}

		var d daem.Daemon
		if d, err = daem.New("immugw", "immugw", "immugw"); err != nil {
			c.QuitToStdErr(err)
		}

		service := gw.Service{
			ImmuGwServer: immuGwServer,
		}

		d.Run(service)

		return
	}
}

func parseOptions(cmd *cobra.Command) (options gw.Options, err error) {
	dir, err := c.ResolvePath(viper.GetString("dir"), true)
	if err != nil {
		return options, err
	}
	port := viper.GetInt("port")
	address := viper.GetString("address")
	immudbport := viper.GetInt("immudb-port")
	immudbAddress := viper.GetString("immudb-address")
	// config file came only from arguments or default folder
	if o.CfgFn, err = cmd.Flags().GetString("config"); err != nil {
		return gw.Options{}, err
	}
	audit := viper.GetBool("audit")
	auditInterval := viper.GetDuration("audit-interval")
	auditUsername := viper.GetString("audit-username")
	auditPassword := viper.GetString("audit-password")
	pidfile, err := c.ResolvePath(viper.GetString("pidfile"), true)
	if err != nil {
		return options, err
	}
	logfile, err := c.ResolvePath(viper.GetString("logfile"), true)
	if err != nil {
		return options, err
	}
	mtls := viper.GetBool("mtls")
	detached := viper.GetBool("detached")
	servername := viper.GetString("servername")
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

	options = gw.DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithImmudbAddress(immudbAddress).
		WithImmudbPort(immudbport).
		WithAudit(audit).
		WithAuditInterval(auditInterval).
		WithAuditUsername(auditUsername).
		WithAuditPassword(auditPassword).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls).
		WithDetached(detached)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options, nil
}

func setupFlags(cmd *cobra.Command, options gw.Options, mtlsOptions client.MTLsOptions) {
	cmd.Flags().String("dir", options.Dir, "program files folder")
	cmd.Flags().IntP("port", "p", options.Port, "immugw port number")
	cmd.Flags().StringP("address", "a", options.Address, "immugw host address")
	cmd.Flags().IntP("immudb-port", "j", options.ImmudbPort, "immudb port number")
	cmd.Flags().StringP("immudb-address", "k", options.ImmudbAddress, "immudb host address")
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immugw.toml)")
	cmd.Flags().Bool("audit", options.Audit, "enable audit mode (continuously fetches latest root from server, checks consistency against a local root and saves the latest root locally)")
	cmd.Flags().Duration("audit-interval", options.AuditInterval, "interval at which audit should run")
	cmd.Flags().String("audit-username", options.AuditUsername, "immudb username used to login during audit")
	cmd.Flags().String("audit-password", options.AuditPassword, "immudb password used to login during audit; can be plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immugw.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immugw/immugw.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().String("servername", mtlsOptions.Servername, "used to verify the hostname on the returned certificates")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func setupDefaults(options gw.Options, mtlsOptions client.MTLsOptions) {
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("immudb-port", options.ImmudbPort)
	viper.SetDefault("immudb-address", options.ImmudbAddress)
	viper.SetDefault("audit", options.Audit)
	viper.SetDefault("audit-interval", options.AuditInterval)
	viper.SetDefault("audit-username", options.AuditUsername)
	viper.SetDefault("audit-password", options.AuditPassword)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("mtls", options.MTLs)
	viper.SetDefault("detached", options.Detached)
	viper.SetDefault("certificate", mtlsOptions.Certificate)
	viper.SetDefault("pkey", mtlsOptions.Pkey)
	viper.SetDefault("clientcas", mtlsOptions.ClientCAs)
}
