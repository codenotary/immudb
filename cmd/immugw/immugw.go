package main

import (
	"fmt"
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var o = c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immugw") })
}

func main() {

	immugwCmd := &cobra.Command{
		Use:   "immugw",
		Short: "Immu gateway",
		Long:  `Immu gateway is an smart proxy for immudb. It exposes all gRPC methods with a rest interface and wrap all SAFE endpoints with a verification service.`,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var options gw.Options
			if options, err = parseOptions(cmd); err != nil {
				return err
			}
			immuGwServer := gw.
				DefaultServer().
				WithOptions(options)
			if options.Logfile != "" {
				if flogger, file, err := logger.NewFileLogger("immugw ", options.Logfile); err == nil {
					defer func() {
						if err := file.Close(); err != nil {
							c.QuitToStdErr(err)
						}
					}()
					immuGwServer.WithLogger(flogger)
				} else {
					return err
				}
			}
			return immuGwServer.Start()
		},
	}

	setupFlags(immugwCmd, gw.DefaultOptions(), client.DefaultMTLsOptions())

	if err := bindFlags(immugwCmd); err != nil {
		c.QuitToStdErr(err)
	}
	setupDefaults(gw.DefaultOptions(), client.DefaultMTLsOptions())
	immugwCmd.AddCommand(man.Generate(immugwCmd, "immugw", "../docs/man/immugw"))

	if err := immugwCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(cmd *cobra.Command) (options gw.Options, err error) {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	immudport := viper.GetInt("default.immudport")
	immudAddress := viper.GetString("default.immudAddress")
	// config file came only from arguments or default folder
	if o.CfgFn, err = cmd.Flags().GetString("config"); err != nil {
		return gw.Options{}, err
	}
	pidfile := viper.GetString("default.pidfile")
	logfile := viper.GetString("default.logfile")
	mtls := viper.GetBool("default.mtls")
	servername := viper.GetString("default.servername")
	certificate := viper.GetString("default.certificate")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")

	options = gw.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithImmudAddress(immudAddress).
		WithImmudPort(immudport).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls)
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
	cmd.Flags().IntP("port", "p", options.Port, "immugw port number")
	cmd.Flags().StringP("address", "a", options.Address, "immugw host address")
	cmd.Flags().IntP("immudport", "j", options.ImmudPort, "immudb port number")
	cmd.Flags().StringP("immudaddress", "k", options.ImmudAddress, "immudb host address")
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file (default path are config or $HOME. Default filename is immugw.ini)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immugw.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immugw/immugw.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().String("servername", mtlsOptions.Servername, "used to verify the hostname on the returned certificates")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag("default.port", cmd.Flags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.Flags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.immudport", cmd.Flags().Lookup("immudport")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.immudaddress", cmd.Flags().Lookup("immudaddress")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pidfile", cmd.Flags().Lookup("pidfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.logfile", cmd.Flags().Lookup("logfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.mtls", cmd.Flags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.servername", cmd.Flags().Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.certificate", cmd.Flags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pkey", cmd.Flags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.clientcas", cmd.Flags().Lookup("clientcas")); err != nil {
		return err
	}
	return nil
}

func setupDefaults(options gw.Options, mtlsOptions client.MTLsOptions) {
	viper.SetDefault("default.port", options.Port)
	viper.SetDefault("default.address", options.Address)
	viper.SetDefault("default.immudport", options.ImmudPort)
	viper.SetDefault("default.immudaddress", options.ImmudAddress)
	viper.SetDefault("default.pidfile", options.Pidfile)
	viper.SetDefault("default.logfile", options.Logfile)
	viper.SetDefault("default.mtls", options.MTLs)
	viper.SetDefault("default.certificate", mtlsOptions.Certificate)
	viper.SetDefault("default.pkey", mtlsOptions.Pkey)
	viper.SetDefault("default.clientcas", mtlsOptions.ClientCAs)
}
